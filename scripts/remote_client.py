"""Native k6 client: HTTP only, no Compose/Docker/metrics-port dependency.
Supply ADMIN_KEY and USER_KEY in the process environment, never CLI arguments.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time
import urllib.request

def main():
    p=argparse.ArgumentParser();p.add_argument('--base-url',required=True);p.add_argument('--test-id',required=True)
    p.add_argument('--k6',default='k6');p.add_argument('--output',default='reports/client')
    p.add_argument('--mode',choices=['async','transaction','direct'],default='async')
    p.add_argument('--requests',type=int,default=500);p.add_argument('--vus',type=int,default=30)
    args=p.parse_args();out=Path(args.output).resolve();out.mkdir(parents=True,exist_ok=True)
    if not args.test_id.replace('-','').replace('_','').isalnum():raise ValueError('invalid test id')
    if (out/(args.test_id+'.json')).exists():raise FileExistsError('choose a new test id')
    rtt=[]
    for _ in range(5):
        start=time.perf_counter()
        with urllib.request.urlopen(args.base_url+'/health/live',timeout=10) as r:assert r.status==200
        rtt.append((time.perf_counter()-start)*1000)
    env=os.environ.copy();env.update(BASE_URL=args.base_url,MODE=args.mode,ITERATIONS=str(args.requests),VUS=str(args.vus),STOCK=str(args.requests),TEST_ID=args.test_id,REPORT_DIR=out.as_posix(),REPORT_NAME=args.test_id)
    script=Path(__file__).resolve().parents[1]/'tests/k6/claims.js'
    logs=Path(__file__).resolve().parents[1]/'reports/client-logs';logs.mkdir(parents=True,exist_ok=True)
    with (logs/(args.test_id+'.log')).open('wb') as log:
        result=subprocess.run([args.k6,'run',str(script)],env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600)
    data=json.loads((out/(args.test_id+'.json')).read_text())
    data['client']={'test_id':args.test_id,'http_health_roundtrip_ms':rtt,'median_roundtrip_ms':statistics.median(rtt),
                    'rtt_clock':'perf_counter','rtt_clock_resolution_seconds':time.get_clock_info('perf_counter').resolution,
                    'network_note':'HTTP health RTT includes server and tunnel overhead; not ICMP RTT',
                    'first_trace_id':hashlib.sha256((args.test_id+'/0').encode()).hexdigest()[:32],
                    'k6_exit_code':result.returncode}
    (out/(args.test_id+'.json')).write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8')
    if result.returncode or data['metrics']['checks']['values']['rate']!=1:raise RuntimeError('k6 failed; inspect local log')
    print(json.dumps(data['measurements']),flush=True)

if __name__=='__main__':main()
