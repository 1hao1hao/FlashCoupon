"""Summarize retained successful trials; preserve interrupted/failed raw samples."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import statistics as st

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',default='evidence/observability-2026-11-19');args=p.parse_args()
    root=Path(args.root);folder=root/'experiments';groups=collections.defaultdict(list);trials=[]
    for path in sorted(folder.glob('*-result.json')):
        r=json.loads(path.read_text());tid=r['test_id'];raw=json.loads((folder/(tid+'.json')).read_text());server=json.loads((folder/(tid+'-server.json')).read_text())
        assert r['audit']['consistent'] and r['audit']['pending']==0 and r['audit']['issued']==r['audit']['projected']==r['requests']
        assert raw['client']['k6_exit_code']==0
        locks=[];slow=[];errors=[];resource=[];stages=collections.defaultdict(list);counters=collections.defaultdict(list);digests=[];timers=[]
        for row in server['samples']:
            sql=row.get('sql_observation','');locks.extend(map(int,re.findall(r'^lock_waits\s+(\d+)',sql,re.M)));slow.extend(map(int,re.findall(r'^slow_count\s+(\d+)',sql,re.M)));errors+=row['errors']
            digests.extend(map(int,re.findall(r'^digest_count\s+(\d+)',sql,re.M)));timers.extend(map(int,re.findall(r'digest_timer_ps\s+(\d+)',sql)))
            resource+=row.get('resources',[])
            for role,lines in row['metrics'].items():
                for line in lines:
                    if line.startswith('flashcoupon_message_retries_total{'):
                        retry=re.search(r'stage="([^"]+)".*\}\s+([\d.eE+-]+)',line)
                        if retry:counters[role+'/'+retry[1]].append(float(retry[2]))
                    if line.startswith('flashcoupon_delivery_stage_seconds_'):
                        match=re.search(r'seconds_(sum|count)\{.*stage="([^"]+)".*\}\s+([\d.eE+-]+)',line)
                        if match:stages[(role,match[2],match[1])].append(float(match[3]))
        stage_means={}
        for role,name,kind in stages:
            counts=stages.get((role,name,'count'),[]);sums=stages.get((role,name,'sum'),[])
            if counts and len(counts)>1 and counts[-1]>counts[0]:stage_means[role+'/'+name]=(sums[-1]-sums[0])/(counts[-1]-counts[0])*1000
        resource_peaks={}
        for sample in resource:
            name=sample.get('Name','unknown');peak=resource_peaks.setdefault(name,{'cpu_percent':0,'memory_bytes':0})
            peak['cpu_percent']=max(peak['cpu_percent'],float(sample.get('CPUPerc','0%').rstrip('%')))
            mem=re.match(r'([\d.]+)([KMGT]?i?B)',sample.get('MemUsage','0B').split('/')[0].strip())
            if mem:
                unit=mem[2];scale={'B':1,'kB':1000,'KB':1000,'KiB':1024,'MB':1000**2,'MiB':1024**2,'GB':1000**3,'GiB':1024**3,'TB':1000**4,'TiB':1024**4}.get(unit,1)
                peak['memory_bytes']=max(peak['memory_bytes'],float(mem[1])*scale)
        trial=dict(test_id=tid,**r['measurements'],p95_ms=raw['metrics']['claim_latency']['values']['p(95)'],p99_ms=raw['metrics']['claim_latency']['values']['p(99)'],http_error_rate=raw['metrics']['http_req_failed']['values']['rate'],drain_seconds=raw['metrics']['drain_seconds']['values']['avg'],http_health_rtt_ms=r['client']['median_roundtrip_ms'],max_lock_wait_edges=max(locks,default=0),slow_statement_delta=max(slow)-min(slow) if slow else None,server_samples=len(server['samples']),collector_errors=errors,stage_mean_ms=stage_means,sampled_resource_peaks=resource_peaks)
        trial.update(control_statement_delta=max(digests)-min(digests) if digests else None,control_statement_time_seconds_delta=(max(timers)-min(timers))/1e12 if timers else None,sampled_retry_counter_delta={k:max(v)-min(v) for k,v in counters.items()})
        trials.append(trial);groups[(r['location'],r['scenario'],r['tracing'],r['workers'],r['mode'])].append(trial)
    aggregate=[]
    for key,rows in sorted(groups.items()):
        assert len(rows)==3,(key,len(rows))
        a=dict(zip(('location','scenario','tracing','workers','mode'),key));a['rounds']=len(rows)
        for field in ('accepted_http_qps','final_issue_tps','p95_ms','p99_ms','http_error_rate','drain_seconds','http_health_rtt_ms','max_lock_wait_edges','slow_statement_delta'):
            a[field]=st.median(x[field] for x in rows if x[field] is not None)
        aggregate.append(a)
    result=dict(accepted_trials=len(trials),issued=sum(json.loads(p.read_text())['requests'] for p in folder.glob('*-result.json')),aggregates=aggregate,trials=trials,limitations=['500 requests/30 VUs per trial; short-run warm-up/sampling noise','HTTP health RTT includes server/tunnel; different host hardware prevents network-only attribution','Stage timing is client duration; SQL digest and slow counts are shared test-instance aggregates','P95/P99 are k6 claim_latency; TPS includes final audit polling, not peak DB commit rate'])
    (root/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    lines=['| 环境 / 场景 | Trace / Worker | HTTP受理QPS | 最终TPS | P95 / P99 ms | 排空秒 |','|---|---|---:|---:|---:|---:|']
    for a in aggregate:lines.append(f"| {a['location']} / {a['scenario']} | {int(a['tracing'])} / {a['workers']} | {a['accepted_http_qps']:.2f} | {a['final_issue_tps']:.2f} | {a['p95_ms']:.2f} / {a['p99_ms']:.2f} | {a['drain_seconds']:.2f} |")
    (root/'results.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    # Git normalizes text to LF; hash the canonical text so Windows checkout
    # line endings do not invalidate the committed audit manifest.
    manifest={str(p.relative_to(root)).replace('\\','/'):hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest() for p in sorted(root.rglob('*')) if p.is_file() and p.name!='manifest.json'}
    (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    print('\n'.join(lines));print('trials',len(trials),'issued',result['issued'])

if __name__=='__main__':main()
