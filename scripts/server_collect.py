"""Server-side independent telemetry sampler; stdlib + Docker Compose only.
No secret, raw SQL statement, process environment or user ID is exported.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
import urllib.request

def main():
    p=argparse.ArgumentParser();p.add_argument('--test-id',required=True);p.add_argument('--output',default='reports/server')
    p.add_argument('--project',default='flashcoupon');p.add_argument('--duration',type=float,default=300)
    p.add_argument('--api-port',type=int,default=8080);p.add_argument('--relay-port',type=int,default=8081);p.add_argument('--worker-port',type=int,default=8082)
    args=p.parse_args()
    if not args.test_id.replace('-','').replace('_','').isalnum():raise ValueError('invalid test id')
    out=Path(args.output);out.mkdir(parents=True,exist_ok=True);target=out/(args.test_id+'.json')
    if target.exists():raise FileExistsError(target)
    def run(cmd):
        r=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=8,check=True);return r.stdout.decode(errors='replace')
    def container(service):return run(['docker','ps','-q','--filter',f'label=com.docker.compose.project={args.project}','--filter',f'label=com.docker.compose.service={service}']).strip()
    mysql=container('mysql');redis=container('redis');samples=[];start=time.monotonic()
    query="SELECT 'lock_waits',COUNT(*) FROM performance_schema.data_lock_waits; SELECT 'transactions_waiting',COUNT(*) FROM information_schema.innodb_trx WHERE trx_state='LOCK WAIT'; SELECT 'digest_count',COALESCE(SUM(COUNT_STAR),0),'digest_timer_ps',COALESCE(SUM(SUM_TIMER_WAIT),0) FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME='control'; SELECT 'slow_count',COUNT(*) FROM mysql.slow_log;"
    while time.monotonic()-start<args.duration:
        final_sample=(out/(args.test_id+'.stop')).exists()
        row={'seconds':round(time.monotonic()-start,3),'metrics':{},'errors':[]}
        for role,port in [('api',args.api_port),('relay',args.relay_port),('worker',args.worker_port)]:
            try:
                text=urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics',timeout=2).read().decode()
                row['metrics'][role]=[x for x in text.splitlines() if x.startswith('flashcoupon_') and not '_bucket{' in x]
            except Exception as e:row['errors'].append(role+':'+type(e).__name__)
        try:
            ids=run(['docker','ps','-q','--filter',f'label=com.docker.compose.project={args.project}']).split()
            if not ids:raise RuntimeError('project has no running containers')
            row['resources']=[json.loads(x) for x in run(['docker','stats','--no-stream','--format','{{json .}}',*ids]).splitlines()]
        except Exception as e:row['errors'].append('resources:'+type(e).__name__)
        try:
            row['redis_info']=run(['docker','exec',redis,'redis-cli','INFO','memory'])
            row['sql_observation']=run(['docker','exec',mysql,'mysql','-N','-uroot','-pflashcoupon','-e',query])
        except Exception as e:row['errors'].append('data:'+type(e).__name__)
        samples.append(row);target.write_text(json.dumps({'test_id':args.test_id,'samples':samples},indent=2)+'\n',encoding='utf-8')
        if final_sample:break
        time.sleep(.5)
    print('server samples',len(samples),flush=True)

if __name__=='__main__':main()
