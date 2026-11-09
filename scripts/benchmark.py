"""Three paired runs, 1000/5000 concurrent users, duplicates, batch and resource evidence."""
import argparse
import re
import threading
from support import *

def metric_stages():
    result={}
    for port,role in ((8080,'api'),(8081,'relay'),(8082,'worker')):
        text=urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics',timeout=10).read().decode()
        for line in text.splitlines():
            match=re.match(r'flashcoupon_delivery_stage_seconds_(sum|count)\{[^}]*stage="([^"]+)"[^}]*\} (\S+)',line)
            if match:result[f'{role}/{match[2]}/{match[1]}']=float(match[3])
    return result

def sample_resources(stop,samples):
    while not stop.is_set():
        try:
            rows=run(['docker','stats','--no-stream','--format','{{json .}}']).splitlines()
            current={}
            for raw in rows:
                r=json.loads(raw);name=r['Name']
                if not name.startswith('flashcoupon-'):continue
                service='k6' if '-k6-run-' in name else name[len('flashcoupon-'):].rsplit('-',1)[0]
                memory=r['MemUsage'].split('/')[0].strip();match=re.match(r'([0-9.]+)([KMGT]?i?B)',memory)
                scales={'B':1,'kB':1000,'KB':1000,'MB':10**6,'GB':10**9,'KiB':1024,'MiB':1024**2,'GiB':1024**3}
                current[service]={'cpu_percent':float(r['CPUPerc'].rstrip('%')),'memory_bytes':round(float(match[1])*scales[match[2]])}
            samples.append({'seconds':round(time.monotonic()-sample_resources.started,2),'services':current})
        except Exception as e:
            samples.append({'sampling_error':type(e).__name__})
        stop.wait(1)

def execute(name,settings,script='claims.js'):
    print('Running',name,flush=True);ready();before=metric_stages();args=DC+['run','--rm','k6','run']
    for key,value in settings.items():args+=['-e',f'{key}={value}']
    args+=['-e',f'REPORT_NAME={name}',f'/scripts/{script}']
    samples=[];stop=threading.Event();sample_resources.started=time.monotonic();thread=threading.Thread(target=sample_resources,args=(stop,samples));thread.start()
    try:
        with (LOCAL/(name+'.log')).open('wb') as log:
            subprocess.run(args,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=900)
    finally:stop.set();thread.join()
    path=LOCAL/(name+'.json' if script=='claims.js' else 'batch.json');data=json.loads(path.read_text(encoding='utf-8'))
    if script=='claims.js':
        t={'id':data['test_template']['id'],'merchant_id':data['test_template']['merchant_id']};data['audit']=audit(t)
        assert data['audit']['consistent'] and data['audit']['pending']==0
    assert data['metrics']['checks']['values']['rate']==1
    after=metric_stages();stage={}
    for key,value in after.items():
        if key.endswith('/count'):
            count=value-before.get(key,0);total=after.get(key[:-5]+'sum',0)-before.get(key[:-5]+'sum',0)
            if count>0:stage[key[:-6]]={'attempts':count,'mean_ms':round(total/count*1000,3)}
    # Canonical evidence keeps no credentials, signed tokens, full logs or personal data.
    data['resources']={'cpu_percent_definition':'100% = one logical CPU; Docker memory usage excludes reclaimable cache','samples':samples}
    data['stage_latency']=stage
    save(name+'.json',data)
    print(name,data.get('measurements',{}),flush=True)
    return data

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--only',choices=['comparison','contention','batch','duplicate']);args=parser.parse_args()
    if args.only in (None,'comparison'):
        for mode in ('direct','async'):
            execute('warmup-'+mode,dict(MODE=mode,ITERATIONS=50,VUS=5,STOCK=50))
            (EVIDENCE/('warmup-'+mode+'.json')).unlink()
        for round in range(1,4):
            for mode in (('direct','async') if round%2 else ('async','direct')):
                execute(f'{mode}-round-{round}',dict(MODE=mode,ITERATIONS=500,VUS=30,STOCK=500))
    if args.only in (None,'contention'):
        for users in (1000,5000):
            execute(f'contention-{users}',dict(MODE='async',ITERATIONS=users,VUS=users,STOCK=users//2,CONTENTION=1,LATENCY_BUDGET_MS=60000))
    if args.only in (None,'duplicate'):execute('duplicate',dict(MODE='async',ITERATIONS=500,VUS=30,STOCK=500,DUPLICATE=1))
    if args.only in (None,'batch'):
        dc('run','--rm','-e','INTEGRATION=0','toolbox','go','run','./cmd/whitelist')
        execute('batch',{},'batch.js')
    info=json.loads(run(['docker','info','--format','{{json .}}']))
    save('environment.json',dict(logical_cpus=info['NCPU'],memory_bytes=info['MemTotal'],engine_version=info['ServerVersion'],mysql_topology='one instance, two logical coupon databases, four user-coupon tables',comparison=dict(rounds=3,iterations=500,vus=30,stock=500,per_user=1,redis_appendfsync='always',broker_flush='SYNC_FLUSH',api_global_rate_limit=5000,worker_count=1,relay_count=1),evidence_schema=1))

if __name__=='__main__':main()
