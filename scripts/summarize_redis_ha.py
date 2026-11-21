"""Audit selected complete trials, report medians, and hash canonical LF text."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import statistics

def memory_bytes(raw):
 m=re.match(r'([\d.]+)\s*([KMGT]?i?B)',raw.strip())
 if not m:return 0
 return float(m[1])*{'B':1,'kB':1000,'KB':1000,'KiB':1024,'MB':1e6,'MiB':1024**2,'GB':1e9,'GiB':1024**3,'TB':1e12,'TiB':1024**4}[m[2]]

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',default='evidence/redis-ha-2026-11-19');p.add_argument('--normal-tag',default='verified-20261121');p.add_argument('--fault-tag',default='resilience-final-20261121');args=p.parse_args()
 root=Path(args.root);groups=collections.defaultdict(list);trials=[]
 choices=[(root/'benchmark'/args.normal_tag,'normal'),(root/'benchmark'/args.fault_tag,'fault')]
 for folder,kind in choices:
  for path in sorted(folder.glob('*-result.json')):
   r=json.loads(path.read_text());scenario=r['scenario']
   if (kind=='normal' and scenario!='normal') or (kind=='fault' and scenario=='normal'):continue
   tid=r['test_id'];raw=json.loads((folder/(tid+'.json')).read_text());server=json.loads((folder/(tid+'-server.json')).read_text());a=r['audit']
   assert a['consistent'] and a['pending']==0 and a['issued']==a['projected']==500 and a['db_remaining']==a['redis_remaining']==0,tid
   assert raw['metrics']['checks']['values']['rate']==1,tid
   latency=raw['metrics']['claim_latency' if scenario=='normal' else 'ha_initial_latency']['values']
   if scenario!='normal':
    assert raw['configuration']['pacing_seconds']==.1
    assert server.get('started_unix'), 'new fault runs require timestamped collector provenance'
    if scenario=='fault':assert server['started_unix']<=r['fault']['injected_unix']<=server['started_unix']+server['samples'][-1]['seconds']+30
   initial_accepted=500 if scenario=='normal' else raw['measurements']['initial_accepted']
   unknown=0 if scenario=='normal' else raw['measurements']['initial_unknown'];rejected=0 if scenario=='normal' else raw['measurements']['initial_safe_rejected']
   assert initial_accepted+unknown+rejected==500,tid
   metrics=server['samples'][-1]['metrics'].get('api',[])
   def metric(name):
    vals=[float(x.rsplit(' ',1)[1]) for x in metrics if x.startswith(name+'{') or x.startswith(name+' ')]
    return sum(vals)
   count=metric('flashcoupon_redis_wait_seconds_count');wait_mean=metric('flashcoupon_redis_wait_seconds_sum')/count*1000 if count else 0
   node_names={'flashcoupon-ha-redis-single-1'} if r['mode']=='standalone' else {'flashcoupon-ha-'+s+'-1' for s in ('redis','redis-replica-1','redis-replica-2','sentinel-1','sentinel-2','sentinel-3')}
   rss=[];cpu=[]
   for row in server['samples']:
    active=[x for x in row.get('resources',[]) if x.get('Name') in node_names]
    rss.append(sum(memory_bytes(x.get('MemUsage','0B').split('/')[0]) for x in active));cpu.append(sum(float(x.get('CPUPerc','0%').rstrip('%')) for x in active))
   trial=dict(test_id=tid,scenario=scenario,mode=r['mode'],wait=r['wait'],accepted_http_qps=r['measurements']['accepted_http_qps'],final_issue_tps=r['measurements']['final_issue_tps'],p95_ms=latency['p(95)'],p99_ms=latency['p(99)'],initial_accepted=initial_accepted,initial_unknown=unknown,initial_safe_rejected=rejected,initial_failure_rate=(unknown+rejected)/500,http_error_rate_including_replays=raw['metrics']['http_req_failed']['values']['rate'],wait_mean_ms=wait_mean,wait_attempts=count,wait_timeouts=next((float(x.rsplit(' ',1)[1]) for x in metrics if x.startswith('flashcoupon_redis_wait_total{') and 'outcome="timeout"' in x),0),sampled_active_redis_memory_bytes_peak=max(rss,default=0),sampled_active_redis_cpu_percent_peak=max(cpu,default=0),server_samples=len(server['samples']),collector_errors=[e for row in server['samples'] for e in row['errors']],audit=a,election_seconds=r.get('fault',{}).get('election_seconds'),all_nodes_restored_seconds=r.get('fault',{}).get('all_nodes_restored_seconds'))
   if scenario=='fault':assert unknown+rejected>0 and trial['election_seconds'] is not None
   if scenario=='normal':assert trial['http_error_rate_including_replays']==0 and not trial['collector_errors'],tid
   groups[(scenario,r['mode'],r['wait'])].append(trial);trials.append(trial)
 aggregates=[]
 for key,rows in sorted(groups.items()):
  assert len(rows)==3,(key,len(rows))
  agg=dict(zip(('scenario','mode','wait'),key));agg['rounds']=3
  for field in ('accepted_http_qps','final_issue_tps','p95_ms','p99_ms','initial_failure_rate','initial_unknown','initial_safe_rejected','wait_mean_ms','wait_timeouts','sampled_active_redis_memory_bytes_peak','sampled_active_redis_cpu_percent_peak','election_seconds','all_nodes_restored_seconds'):
   values=[r[field] for r in rows if r.get(field) is not None];agg[field]=statistics.median(values) if values else None
  aggregates.append(agg)
 assert len(trials)==18 and len(aggregates)==6,(len(trials),len(aggregates))
 result=dict(accepted_trials=len(trials),unique_issued=500*len(trials),request_count_per_trial=500,vus=30,aggregates=aggregates,trials=trials,limitations=['Normal and paced fault/control use different offered pacing; compare within each group only.','One Linux host, Windows native k6 through SSH, shared hardware; not steady-state capacity or physical-host HA.','Single and HA Redis both remain deployed for fixture preservation; active-node samples are not whole-host resource costs.','WAIT timing covers API attempts including replay in fault trials; source collector sampling can miss subsecond peaks.','Final TPS includes client replay and audit polling; HTTP acceptance is not final issuance.'])
 (root/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
 lines=['| 场景 | Redis / WAIT | 受理 QPS | 最终 TPS | P95 / P99 ms | 初始失败率 | WAIT 均值 ms |','|---|---|---:|---:|---:|---:|---:|']
 for a in aggregates:lines.append(f"| {a['scenario']} | {a['mode']} / {a['wait']} | {a['accepted_http_qps']:.2f} | {a['final_issue_tps']:.2f} | {a['p95_ms']:.2f} / {a['p99_ms']:.2f} | {a['initial_failure_rate']:.1%} | {a['wait_mean_ms']:.2f} |")
 (root/'results.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
 files={str(p.relative_to(root)).replace('\\','/'):hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest() for p in sorted(root.rglob('*')) if p.is_file() and p.name!='manifest.json'}
 (root/'manifest.json').write_text(json.dumps(files,indent=2)+'\n',encoding='utf-8')
 print('\n'.join(lines));print('verified trials',len(trials),'unique issuance',result['unique_issued'])

if __name__=='__main__':main()
