"""Paired transport experiments; uses existing Compose, k6, audit and helpers.

No volumes or ledgers are deleted. Run serially on a disposable development
environment: pauses MySQL, stops Broker and kills background processes.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import threading
import time
import urllib.request

import support as h

SNAPSHOT = """
local function size(k) return redis.call('MEMORY','USAGE',k) or 0 end
local info=redis.call('INFO','memory')
local total=tonumber(string.match(info,'used_memory:(%d+)'))
local p=ARGV[1]; local ledger=0
for _,s in ipairs({'meta','stock','users','reservations','states','revision'}) do ledger=ledger+size(p..s) end
local pending=0; local lag=0
for _,g in ipairs(redis.call('XINFO','GROUPS',KEYS[1])) do
 for i=1,#g,2 do
  if g[i]=='pending' then pending=pending+g[i+1] end
  if g[i]=='lag' and type(g[i+1])=='number' then lag=lag+g[i+1] end
 end
end
return cjson.encode({redis_used_bytes=total,stream_bytes=size(KEYS[1]),stream_entries=redis.call('XLEN',KEYS[1]),pel=pending,unread=lag,fixture_ledger_bytes=ledger,reservations=redis.call('HLEN',p..'reservations')})
"""


def snapshot(t):
    prefix = f"fc:t:{t['merchant_id']}:{t['id']}:"
    return json.loads(h.redis('EVAL', SNAPSHOT, 1, 'fc:outbox', prefix))


def write(name, value):
    h.save(name, value)


def manifest():
    write('manifest.json', {
        p.relative_to(h.EVIDENCE).as_posix(): hashlib.sha256(p.read_bytes().replace(b'\r\n', b'\n')).hexdigest()
        for p in h.EVIDENCE.rglob('*') if p.is_file() and p != h.EVIDENCE/'manifest.json'
    })


class Experiment:
    def __init__(self):
        self.override = h.LOCAL / 'queue-experiment.compose.json'
        h.DC.extend(['-f', str(self.override)])
        self.mode = None

    def configure(self, mode, disable_cleanup=False):
        self.override.write_text(json.dumps({'services': {
            'worker': {'environment': {'TEST_STREAM_WORKER': str(int(mode == 'stream')),
                                      'TEST_DISABLE_OUTBOX_CLEANUP': str(int(disable_cleanup))}},
            'relay': {'environment': {'TEST_DISABLE_OUTBOX_CLEANUP': str(int(disable_cleanup))}}
        }}), encoding='utf-8')

    def start(self, mode, disable_cleanup=False):
        if not self.override.exists(): self.configure('mq')
        h.dc('stop', 'relay', 'worker')
        self.configure(mode, disable_cleanup)
        h.dc('up', '-d', '--no-deps', 'worker', *(['relay'] if mode == 'mq' else []))
        for port in ((8080, 8081, 8082) if mode == 'mq' else (8080, 8082)):
            h.until(lambda: h.request('/health/ready', base=f'http://127.0.0.1:{port}')[0] == 200, 90)
        h.until(lambda: h.redis('XLEN', 'fc:outbox') == 0, 60)
        self.mode = mode

    def lock(self, t):
        query = f"BEGIN; SELECT id FROM templates WHERE merchant_id={t['merchant_id']} AND id='{t['id']}' FOR UPDATE; SELECT GET_LOCK('fc_queue_experiment',0); SELECT SLEEP(90); ROLLBACK;"
        process = subprocess.Popen(h.DC + ['exec', '-T', 'mysql', 'mysql', '-uroot',
            '-pflashcoupon', 'control', '-e', query], cwd=h.ROOT,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        def connection():
            value = h.sql("SELECT IS_USED_LOCK('fc_queue_experiment');").splitlines()[-1]
            return int(value) if value != 'NULL' else None
        return process, h.until(connection, 10)

    def case(self, scenario, mode, round_id, count=1000):
        name = f'{scenario}-{mode}-{round_id}'
        existing = h.EVIDENCE / (name + '.json')
        if existing.exists():
            saved = json.loads(existing.read_text())
            assert saved['metrics']['checks']['values']['rate'] == 1
            assert all(t['ok'] for metric in saved['metrics'].values()
                       for t in metric.get('thresholds', {}).values()), name
            assert saved['experiment']['final_audit']['consistent']
            assert saved['experiment']['final_audit']['issued'] == count
            return
        self.start(mode, scenario == 'cleanup')
        t = h.template(count)
        hold_seconds = 45 if scenario == 'long-blocked' else 12
        # Cache template before pausing DB. This measures the existing hot path,
        # not a cold read that legitimately requires MySQL.
        assert h.request(f"/v1/templates/{t['merchant_id']}/{t['id']}", user=900001)[0] == 200
        before = snapshot(t)
        extra_relay = 'flashcoupon-queue-extra-relay' if scenario == 'multi-relay' else None
        if extra_relay:
            h.dc('run', '-d', '--no-deps', '--name', extra_relay, 'relay')
        blocker = connection = None
        if scenario in ('blocked', 'long-blocked', 'crash'): blocker, connection = self.lock(t)
        if scenario == 'paused': h.dc('pause', 'mysql')
        if scenario == 'broker': h.dc('stop', 'broker')
        samples, events, errors = [], [], []
        stop = threading.Event()
        start = time.monotonic()

        def sample():
            while not stop.is_set():
                try: samples.append(dict(seconds=round(time.monotonic()-start, 3), **snapshot(t)))
                except Exception as e: errors.append(type(e).__name__)
                stop.wait(.5)

        release_time = []
        def recover():
            try:
                if scenario == 'crash':
                    time.sleep(2)
                    h.dc('kill', 'worker', *(['relay'] if mode == 'mq' else []))
                    events.append(dict(action='kill', seconds=round(time.monotonic()-start, 3)))
                time.sleep(max(0, hold_seconds-(time.monotonic()-start)))
                if connection: h.sql(f'KILL {connection};')
                if scenario == 'paused': h.dc('unpause', 'mysql')
                if scenario == 'broker': h.dc('start', 'broker')
                release_time.append(time.monotonic())
                if scenario == 'crash':
                    h.dc('up', '-d', '--no-deps', 'worker', *(['relay'] if mode == 'mq' else []))
                events.append(dict(action='recovery_requested', seconds=round(release_time[0]-start, 3)))
            except Exception as e: errors.append(repr(e))

        monitor = threading.Thread(target=sample); monitor.start()
        recovery = None
        if scenario in ('blocked', 'long-blocked', 'paused', 'crash', 'broker'):
            recovery = threading.Thread(target=recover); recovery.start()
        args = h.DC + ['run', '--rm', '--no-deps', 'k6', 'run', '-e', 'MODE=async',
            '-e', f'ITERATIONS={count}', '-e', 'VUS=100', '-e', f'STOCK={count}',
            '-e', 'LATENCY_BUDGET_MS=60000', '-e', 'TEMPLATE_JSON='+json.dumps(t,separators=(',',':')),
            '-e', 'REPORT_NAME='+name, '/scripts/claims.js']
        try:
            with (h.LOCAL/(name+'.log')).open('wb') as log:
                result = subprocess.run(args, cwd=h.ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=360)
            if recovery: recovery.join(timeout=120)
            assert not recovery or not recovery.is_alive(), 'recovery thread stuck'
            data = json.loads((h.LOCAL/(name+'.json')).read_text())
            final = h.drain(t, count)
            drained = time.monotonic()
            unique = h.sql(f"SELECT COUNT(*),COUNT(DISTINCT user_id) FROM grants WHERE merchant_id={t['merchant_id']} AND template_id='{t['id']}' AND state='issued';").splitlines()[-1].split()
            assert list(map(int,unique)) == [count,count], unique
            cleanup_before = snapshot(t)
            if scenario == 'cleanup':
                assert cleanup_before['stream_entries'] >= count, cleanup_before
                self.configure(mode, False)
                h.dc('up','-d','--no-deps','relay')
            h.until(lambda: h.redis('XLEN','fc:outbox') == 0, 60)
            after = snapshot(t)
            assert after['reservations'] == count and final['consistent'], (after,final)
            data['experiment'] = dict(scenario=scenario, transport=mode, round=round_id, stall_seconds=hold_seconds,
                k6_exit_code=result.returncode,
                before=before, samples=samples, after=after, cleanup_before=cleanup_before,
                events=events, sampling_errors=errors, final_audit=final,
                unique_users=int(unique[1]),
                recovery_to_drain_seconds=round(drained-release_time[0],3) if release_time else None)
            write(name+'.json',data)
            assert result.returncode == 0 and data['metrics']['checks']['values']['rate'] == 1, name
            assert not errors, errors
            print(name, data['measurements'], 'stream_peak', max(s['stream_bytes'] for s in samples), flush=True)
        finally:
            if recovery: recovery.join(timeout=120)
            stop.set();monitor.join(timeout=10)
            if scenario == 'paused':
                # May already be unpaused; do not mask an earlier failure.
                subprocess.run(h.DC+['unpause','mysql'],cwd=h.ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            if blocker:
                try: h.sql(f'KILL {connection};')
                except RuntimeError: pass
                blocker.wait(timeout=10)
            if scenario == 'broker': h.dc('start','broker')
            if extra_relay:
                h.run(['docker','rm','-f',extra_relay])


def summarize():
    rows=[]
    for path in sorted(h.EVIDENCE.glob('*.json')):
        data=json.loads(path.read_text())
        if 'experiment' not in data: continue
        e=data['experiment']; samples=e['samples']
        # Last sample before requested recovery isolates the stalled phase from drain.
        recovery_events=[x['seconds'] for x in e['events'] if x['action']=='recovery_requested']
        cutoff=recovery_events[0]-.5 if recovery_events else float('inf')
        stalled=[s for s in samples if s['seconds'] < cutoff]
        last=stalled[-1] if stalled else samples[-1]
        rows.append(dict(name=path.stem,scenario=e['scenario'],transport=e['transport'],round=e['round'],
            accepted_qps=data['measurements']['accepted_http_qps'], final_tps=data['measurements']['final_issue_tps'],
            p95_ms=data['metrics']['claim_latency']['values']['p(95)'],p99_ms=data['metrics']['claim_latency']['values']['p(99)'],
            unexpected_errors=data['metrics'].get('unexpected_claim_errors',{}).get('values',{}).get('count',0),
            peak_stream_bytes=max(s['stream_bytes'] for s in samples),
            peak_redis_delta_bytes=max(s['redis_used_bytes'] for s in samples)-e['before']['redis_used_bytes'],
            stalled_stream_entries=last['stream_entries'],stalled_stream_bytes=last['stream_bytes'],
            stalled_fixture_ledger_bytes=last['fixture_ledger_bytes'],stalled_pel=last['pel'],stalled_unread=last['unread'],
            recovery_to_drain_seconds=e['recovery_to_drain_seconds'],
            cleanup_stream_before=e['cleanup_before']['stream_bytes'],cleanup_stream_after=e['after']['stream_bytes'],
            cleanup_redis_before=e['cleanup_before']['redis_used_bytes'],cleanup_redis_after=e['after']['redis_used_bytes'],
            final_issued=e['final_audit']['issued'],consistent=e['final_audit']['consistent']))
    groups=[]
    for scenario,mode in sorted({(r['scenario'],r['transport']) for r in rows}):
        rs=[r for r in rows if (r['scenario'],r['transport'])==(scenario,mode)]
        g=dict(scenario=scenario,transport=mode,rounds=len(rs))
        for key in ('accepted_qps','final_tps','p95_ms','p99_ms','peak_stream_bytes','peak_redis_delta_bytes',
                    'stalled_stream_entries','stalled_stream_bytes','stalled_fixture_ledger_bytes',
                    'recovery_to_drain_seconds','cleanup_stream_before','cleanup_stream_after',
                    'cleanup_redis_before','cleanup_redis_after'):
            values=[r[key] for r in rs if r[key] is not None]
            g[key]=round(statistics.median(values),3) if values else None
        groups.append(g)
    write('summary.json',dict(rounds=rows,medians=groups,all_consistent=all(r['consistent'] for r in rows)))
    lines=['# 双队列对照实验','',
        '| 场景 | 传输 | 轮数 | 受理 QPS | 最终 TPS | P95 ms | 阻塞期末 Stream 条数 / KiB | 恢复至排空 s |',
        '|---|---|---:|---:|---:|---:|---:|---:|']
    for g in groups:
        lines.append(f"| {g['scenario']} | {g['transport']} | {g['rounds']} | {g['accepted_qps']:.2f} | {g['final_tps']:.2f} | {g['p95_ms']:.2f} | {g['stalled_stream_entries']:.0f} / {g['stalled_stream_bytes']/1024:.2f} | {g['recovery_to_drain_seconds']} |")
    (h.EVIDENCE/'RESULTS.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',default='evidence/queue-isolation-2026-11-11')
    parser.add_argument('--only',choices=['normal','blocked','long-blocked','paused','crash','broker','cleanup','multi-relay'])
    parser.add_argument('--summarize',action='store_true')
    args=parser.parse_args()
    h.EVIDENCE=h.ROOT/args.output;h.EVIDENCE.mkdir(parents=True,exist_ok=True)
    if args.summarize: summarize();manifest();return
    experiment=Experiment()
    try:
        if not experiment.override.exists(): experiment.configure('mq')
        h.dc('up','-d','--no-deps','app')
        info=json.loads(h.run(['docker','info','--format','{{json .}}']))
        config=dict(logical_cpus=info['NCPU'],memory_bytes=info['MemTotal'],engine=info['ServerVersion'],
            rounds=3,requests=1000,vus=100,stock=1000,per_user=1,stall_seconds=12,long_blocked_seconds=45,
            stream_worker_goroutines=20,mq_worker_goroutines=20,cleanup_entries_per_round=128,cleanup_interval_ms=100,
            redis_aof='always',mq_flush='SYNC_FLUSH',mysql='single physical instance / Proxy / four coupon tables',
            stream_group='relay',mode_change='exclusive consumers; drained queue; not live migration',
            memory='used_memory total, Outbox MEMORY USAGE incl PEL, separate per-fixture ledger; no allocator RSS claim',
            warm_template=True,transport_only='Lua and Deliver unchanged between modes',
            base_commit=h.run(['git','rev-parse','HEAD']).strip(),
            runtime_image_id=h.run(['docker','image','inspect','flashcoupon-runtime:local','--format','{{.Id}}']).strip(),
            source_sha256={p.relative_to(h.ROOT).as_posix():hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest()
                           for p in (h.ROOT/'internal/app').rglob('*') if p.is_file() and not p.name.endswith('_test.go')})
        config_path=h.EVIDENCE/'configuration.json'
        if config_path.exists():
            previous=json.loads(config_path.read_text())
            assert previous==config, 'Configuration/image/source changed; use a new --output directory'
        else:
            write('configuration.json',config)
        for scenario in ([args.only] if args.only else ['normal','blocked','long-blocked','paused','crash','broker','cleanup','multi-relay']):
            for n in range(1,4):
                modes=['mq'] if scenario in ('cleanup','multi-relay') else (['mq','stream'] if n%2 else ['stream','mq'])
                for mode in modes: experiment.case(scenario,mode,n)
        summarize()
    finally:
        experiment.configure('mq')
        h.dc('up','-d','--no-deps','app','relay','worker')
    manifest()


if __name__=='__main__': main()
