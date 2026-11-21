"""Windows native k6 + independent Linux/server collection, joined by test ID.
Remote mode uses SSH for orchestration/tunnel, never local Compose for the server.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import socketserver
import socket
import select
import statistics
import subprocess
import sys
import threading
import time
import urllib.request

import support as h
from remote_deploy import connect,command

class Tunnel(socketserver.ThreadingTCPServer):
    allow_reuse_address=True
    daemon_threads=True
    request_queue_size=256

def forwarding(transport,port):
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
            try:channel=transport.open_channel('direct-tcpip',('127.0.0.1',port),self.request.getpeername())
            except Exception:return  # Target can legitimately disappear during fault tests.
            if channel is None:return
            try:
                while True:
                    readable,_,_=select.select([self.request,channel],[],[],1)
                    for source in readable:
                        data=source.recv(65536)
                        if not data:return
                        (channel if source is self.request else self.request).sendall(data)
            except (ConnectionError,OSError):
                pass  # Normal HTTP client teardown may reset a keep-alive socket.
            finally:channel.close()
    return Handler

class Target:
    def __init__(self,args):
        self.args=args;self.remote=bool(args.host);self.ssh=connect(args) if self.remote else None;self.tunnels=[]
        self.project='flashcoupon-observe' if self.remote else 'flashcoupon'
        self.directory=args.directory if self.remote else str(h.ROOT)
        self.override='reports/observability-experiment.compose.json'
        self.files=['compose.yaml','compose.sharding.yaml','compose.observability.yaml','compose.transaction.yaml']+(['compose.remote.yaml'] if self.remote else [])+[self.override]
        self.env={}
        if self.remote:
            sftp=self.ssh.open_sftp()
            with sftp.file(self.directory+'/.env') as f:
                for line in f.read().decode().splitlines():
                    if '=' in line:k,v=line.split('=',1);self.env[k]=v
            sftp.close()
            for local,remote in [(19080,18080),(19081,18081),(19082,18082),(19486,18486)]:
                server=Tunnel(('127.0.0.1',local),forwarding(self.ssh.get_transport(),remote));threading.Thread(target=server.serve_forever,daemon=True).start();self.tunnels.append(server)
        self.base='http://127.0.0.1:'+('19080' if self.remote else '8080')
        self.jaeger='http://127.0.0.1:'+('19486' if self.remote else '16686')

    def file(self,path,content):
        if self.remote:
            s=self.ssh.open_sftp();s.file(self.directory+'/'+path,'w').write(content);s.close()
        else:(h.ROOT/path).write_text(content,encoding='utf-8')

    def shell(self,cmd):
        if self.remote:return command(self.ssh,'cd '+shlex.quote(self.directory)+' && '+cmd)
        return h.run(['docker','compose','-p',self.project,*sum([['-f',x] for x in self.files],[]),*cmd])

    def dc(self,*args):
        if self.remote:return self.shell('docker compose -p '+self.project+' '+ ' '.join('-f '+shlex.quote(x) for x in self.files)+' '+' '.join(shlex.quote(x) for x in args))
        return self.shell(list(args))

    def bootstrap(self):
        cfg={'services':{s:{'environment':{'TX_EXPERIMENT_ENABLED':'0','TRACING_ENABLED':'0'}} for s in ('app','relay','worker')}}
        self.file(self.override,json.dumps(cfg))
        self.dc('up','-d','broker','tx-topic','mq-topic','jaeger')
        self.dc('run','--rm','--no-deps','mq-topic')
        self.dc('run','--rm','--no-deps','tx-topic')

    def ready(self):
        for port in ([19080,19081,19082] if self.remote else [8080,8081,8082]):
            h.until(lambda:urllib.request.urlopen(f'http://127.0.0.1:{port}/health/ready',timeout=4).status==200,120)

    def configure(self,tracing,workers):
        cfg={'services':{s:{'environment':{'TRACING_ENABLED':str(int(tracing)),'TRACE_SAMPLE_RATIO':'1','WORKER_CONCURRENCY':str(workers),'TX_EXPERIMENT_ENABLED':'1','TX_TEST_UNKNOWN':'0','TX_TEST_ROLLBACK':'0'}} for s in ('app','relay','worker')}}
        self.file(self.override,json.dumps(cfg));self.dc('up','-d','--no-deps','app','relay','worker');self.ready()

    def sql(self,query):
        if self.remote:
            return self.shell('printf %s '+shlex.quote(query)+' | docker compose -p '+self.project+' -f compose.yaml exec -T mysql mysql -N -uroot -pflashcoupon control')
        return h.sql(query)

    def collector(self,test_id):
        args=['python3' if self.remote else sys.executable,'scripts/server_collect.py','--test-id',test_id,'--project',self.project,'--output','reports/server','--duration','360']
        if self.remote:
            args+=['--api-port','18080','--relay-port','18081','--worker-port','18082']
            self.ssh.exec_command('cd '+shlex.quote(self.directory)+' && nohup '+' '.join(shlex.quote(a) for a in args)+' > reports/server-'+test_id+'.log 2>&1 < /dev/null &');return None
        return subprocess.Popen(args,cwd=h.ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

    def stop_collector(self,test_id,process,out):
        self.file('reports/server/'+test_id+'.stop','stop')
        if process:process.wait(timeout=20)
        else:time.sleep(4)
        if self.remote:
            s=self.ssh.open_sftp();s.get(self.directory+'/reports/server/'+test_id+'.json',str(out));s.close()
        else:out.write_bytes((h.LOCAL/'server'/(test_id+'.json')).read_bytes())

    def close(self):
        for server in self.tunnels:server.shutdown();server.server_close()
        if self.ssh:self.ssh.close()

def main():
    p=argparse.ArgumentParser();p.add_argument('--host');p.add_argument('--user');p.add_argument('--port',type=int,default=22);p.add_argument('--password-env',default='FLASHCOUPON_SSH_PASSWORD')
    p.add_argument('--directory',default='/home/public/bjh/Flashcoupon');p.add_argument('--k6',default='k6');p.add_argument('--output',default='evidence/observability-2026-11-19/experiments')
    p.add_argument('--only',choices=['normal','locked','concurrency','transaction']);p.add_argument('--requests',type=int,default=500)
    p.add_argument('--run-tag',default='',help='Unique suffix when retaining an interrupted run')
    args=p.parse_args();out=(h.ROOT/args.output).resolve();out.mkdir(parents=True,exist_ok=True);target=Target(args)
    location='cross-machine-ssh' if args.host else 'same-machine'
    env=os.environ.copy();env.update({k:v for k,v in target.env.items() if k in ('ADMIN_KEY','USER_KEY')})
    original_sql=None
    try:
        if not target.remote:(h.LOCAL/'server').mkdir(exist_ok=True)
        else:command(target.ssh,'mkdir -p '+shlex.quote(target.directory+'/reports/server'))
        target.bootstrap()
        original_sql=target.sql('SELECT @@GLOBAL.slow_query_log,@@GLOBAL.long_query_time,@@GLOBAL.log_output;').splitlines()[-1].split()
        target.sql("SET GLOBAL log_output='TABLE'; SET GLOBAL long_query_time=0.05; SET GLOBAL slow_query_log=ON;")
        cases=[('normal',False,20,'async'),('normal',True,20,'async'),('locked',True,20,'async')]
        if not target.remote:cases += [('concurrency',True,1,'async'),('concurrency',True,40,'async'),('transaction',True,20,'transaction')]
        for round_id in range(1,4):
            ordered=cases if round_id%2 else list(reversed(cases))
            for scenario,tracing,workers,mode in ordered:
                if args.only and scenario!=args.only:continue
                test_id=f'{location}-{scenario}-trace{int(tracing)}-w{workers}-r{round_id}'+('-'+args.run_tag if args.run_tag else '')
                if (out/(test_id+'-result.json')).exists():continue
                print('experiment',test_id,flush=True);target.configure(tracing,workers)
                process=target.collector(test_id);release=None;released=[]
                if scenario=='locked':
                    # Lock the small template table so the k6-created template must
                    # already exist: create via HTTP and pass it to the shared script.
                    secret=env.get('ADMIN_KEY',h.ADMIN);now=int(time.time())
                    req=urllib.request.Request(target.base+'/admin/templates',data=json.dumps(dict(merchant_id=time.time_ns()%1000000000000,name=test_id,stock=args.requests,per_user=1,threshold=1000,discount=100,starts_at=now-2,ends_at=now+600,valid_until=now+900,mode='async')).encode(),headers={'X-Admin-Key':secret,'Content-Type':'application/json'})
                    tpl=json.load(urllib.request.urlopen(req));lock_query=f"BEGIN; SELECT id FROM templates WHERE merchant_id={tpl['merchant_id']} AND id='{tpl['id']}' FOR UPDATE; SELECT GET_LOCK('fc_observe_lock',0); SELECT SLEEP(60); ROLLBACK;"
                    if target.remote:
                        _,lock_stdout,_=target.ssh.exec_command('cd '+shlex.quote(target.directory)+' && docker compose -p '+target.project+' -f compose.yaml exec -T mysql mysql -uroot -pflashcoupon control -e '+shlex.quote(lock_query))
                    else:
                        locker=subprocess.Popen(h.DC+['exec','-T','mysql','mysql','-uroot','-pflashcoupon','control','-e',lock_query],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                    def connection():
                        v=target.sql("SELECT IS_USED_LOCK('fc_observe_lock');").splitlines()[-1];return int(v) if v!='NULL' else None
                    cid=h.until(connection,15)
                    env['TEMPLATE_JSON']=json.dumps(tpl)
                    def unlock():time.sleep(12);target.sql(f'KILL {cid};');released.append(time.time())
                    release=threading.Thread(target=unlock);release.start()
                else:env.pop('TEMPLATE_JSON',None)
                try:
                    subprocess.run([sys.executable,str(h.ROOT/'scripts/remote_client.py'),'--base-url',target.base,'--test-id',test_id,'--k6',str(Path(args.k6).resolve()),'--output',str(out),'--mode',mode,'--requests',str(args.requests),'--vus','30'],cwd=h.ROOT,env=env,check=True,timeout=600)
                    if release:release.join(timeout=30)
                    raw=json.loads((out/(test_id+'.json')).read_text());tid=raw['test_template'];audit_req=urllib.request.Request(target.base+f"/admin/audit?merchant_id={tid['merchant_id']}&template_id={tid['id']}",headers={'X-Admin-Key':env.get('ADMIN_KEY',h.ADMIN)})
                    audit=json.load(urllib.request.urlopen(audit_req))[0];assert audit['consistent'] and audit['pending']==0 and audit['issued']==args.requests
                    target.stop_collector(test_id,process,out/(test_id+'-server.json'))
                    trace=None
                    if tracing:
                        trace_id=raw['client']['first_trace_id']
                        time.sleep(1)
                        trace=json.load(urllib.request.urlopen(target.jaeger+'/api/v3/traces/'+trace_id))
                        resources=trace.get('result',{}).get('resourceSpans',[])
                        services={a['value'].get('stringValue') for r in resources for a in r.get('resource',{}).get('attributes',[]) if a['key']=='service.name'}
                        spans={s['name'] for r in resources for scope in r.get('scopeSpans',[]) for s in scope.get('spans',[])}
                        expected={'flashcoupon-api','flashcoupon-worker'} | ({'flashcoupon-relay'} if mode=='async' else set())
                        assert services>=expected,(services,expected)
                        assert spans>={'sql.commit','worker.deliver','redis.finalize'},spans
                        (out/(test_id+'-trace.json')).write_text(json.dumps(trace,indent=2)+'\n',encoding='utf-8')
                    result=dict(test_id=test_id,location=location,scenario=scenario,tracing=tracing,workers=workers,mode=mode,requests=args.requests,measurements=raw['measurements'],audit=audit,client=raw['client'],fault_released_unix=released,finished_unix=time.time())
                    (out/(test_id+'-result.json')).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
                except Exception as error:
                    failure=dict(test_id=test_id,error_type=type(error).__name__,scenario=scenario,tracing=tracing,workers=workers,mode=mode,requests=args.requests)
                    try:target.stop_collector(test_id,process,out/(test_id+'-server.json'))
                    except Exception as capture_error:failure['collector_error_type']=type(capture_error).__name__
                    (out/(test_id+'-failure.json')).write_text(json.dumps(failure,indent=2)+'\n',encoding='utf-8')
                    raise
                finally:
                    if release:release.join(timeout=30)
        print('experiments completed',location,flush=True)
    finally:
        if original_sql:
            enabled,threshold,output=original_sql
            target.sql(f"SET GLOBAL slow_query_log={int(enabled)}; SET GLOBAL long_query_time={float(threshold)}; SET GLOBAL log_output='{output}';")
        target.close()

if __name__=='__main__':main()
