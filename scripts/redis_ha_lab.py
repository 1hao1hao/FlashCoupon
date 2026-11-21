"""Isolated real Redis HA lab; default remote lab reuses cached runtime images.
Never delete volumes. Credentials live only in ignored/remote mode-0600 env.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request

import support as h
from remote_deploy import connect,command
from distributed_experiment import Tunnel,forwarding
import threading

ROOT=h.ROOT
OUT=ROOT/'evidence/redis-ha-2026-11-19'

class Lab:
 def __init__(self,args):
  self.args=args;self.remote=bool(args.host);self.ssh=connect(args) if self.remote else None;self.project='flashcoupon-ha';self.directory=args.directory
  self.files=['compose.yaml','compose.sharding.yaml','compose.observability.yaml','compose.redis-ha.yaml','reports/ha-site.yaml']
  self.envfile='reports/redis-ha.env';self.servers=[]
  if self.remote:
   for local,remote in [(19880,18880),(19881,18881),(19882,18882),(19686,18686)]:
    server=Tunnel(('127.0.0.1',local),forwarding(self.ssh.get_transport(),remote));threading.Thread(target=server.serve_forever,daemon=True).start();self.servers.append(server)
  self.base='http://127.0.0.1:'+('19880' if self.remote else '18880')
  self.ports=[19880,19881,19882] if self.remote else [18880,18881,18882]
  self.env={}
 def shell(self,cmd):
  if self.remote:return command(self.ssh,'cd '+shlex.quote(self.directory)+' && '+cmd)
  return h.run(cmd)
 def dc(self,*args):
  if args and args[0]=='start':
   # Compose start may bring dependency services back too. Fault recovery must
   # restart only the named nodes (especially Sentinel without the dead master).
   cmd=['docker','start',*[self.project+'-'+s+'-1' for s in args[1:]]]
   return self.shell(' '.join(shlex.quote(a) for a in cmd)) if self.remote else self.shell(cmd)
  cmd=['docker','compose','-p',self.project,'--env-file',self.envfile]+sum([['-f',x] for x in self.files],[])+list(args)
  return self.shell(' '.join(shlex.quote(a) for a in cmd)) if self.remote else self.shell(cmd)
 def write(self,path,raw):
  if self.remote:
   s=self.ssh.open_sftp()
   with s.file(self.directory+'/'+path,'w') as f:f.write(raw)
   s.chmod(self.directory+'/'+path,0o600);s.close()
  else:(ROOT/path).write_text(raw,encoding='utf-8')
 def setup(self):
  if self.remote:self.shell('mkdir -p reports')
  try:
   if self.remote:
    s=self.ssh.open_sftp()
    with s.file(self.directory+'/'+self.envfile) as f:raw=f.read().decode()
    s.close()
   else:raw=(ROOT/self.envfile).read_text()
  except (FileNotFoundError,OSError):
   raw='\n'.join(k+'='+secrets.token_hex(24) for k in ('ADMIN_KEY','USER_KEY','REDIS_PASSWORD','REDIS_SENTINEL_PASSWORD'))+'\n';self.write(self.envfile,raw)
  self.env=dict(line.split('=',1) for line in raw.splitlines() if '=' in line)
  self.configure(1)
 def configure(self,wait,mode='sentinel'):
  yaml='services:\n'
  for service,ports in [('app',['127.0.0.1:18880:8080']),('relay',['127.0.0.1:18881:8080']),('worker',['127.0.0.1:18882:8080']),('mysql',[]),('proxy',[]),('jaeger',['127.0.0.1:18686:16686'])]:
   yaml+=f'  {service}:\n    ports: !override '+json.dumps(ports)+'\n'
   if service=='proxy':yaml+='    image: flashcoupon-proxy:latest\n'
   if service in ('app','relay','worker'):
    yaml+=f'    environment:\n      REDIS_WAIT_REPLICAS: "{wait}"\n      REDIS_MODE: {mode}\n      TRACING_ENABLED: "1"\n      TRACE_SAMPLE_RATIO: "1"\n'
    if mode=='standalone':yaml+='      REDIS_ADDR: redis-single:6379\n      REDIS_OBSERVE_ADDRS: redis-single:6379\n'
  # Independent single-node data for paired baseline, never switch an HA ledger.
  yaml+='  redis-single:\n    image: redis:7-alpine\n    restart: unless-stopped\n    networks: [default, redis-ha]\n    environment:\n      REDIS_PASSWORD: ${REDIS_PASSWORD}\n    command: ["redis-server", "--appendonly", "yes", "--appendfsync", "always", "--maxmemory-policy", "noeviction", "--requirepass", "${REDIS_PASSWORD}"]\n    volumes: ["ha-standalone-data:/data"]\nvolumes:\n  ha-standalone-data:\n'
  self.write('reports/ha-site.yaml',yaml)
 def start(self):
  self.dc('up','-d','--no-build','--pull','never','app','relay','worker','jaeger','redis-single');self.ready()
 def switch_mode(self,wait,mode='sentinel',previous='sentinel'):
  if mode!=previous:
   status,rows=self.request('/admin/audit');assert status==200 and all(a['consistent'] and a['pending']==0 for a in rows),'must settle all business before fixture copy'
  self.dc('stop','app','relay','worker')
  if mode!=previous:
   source=self.master() if previous=='sentinel' else 'redis-single'
   dest=self.master() if mode=='sentinel' else 'redis-single'
   cursor='0'
   while True:
    cursor,keys=self.redis(source,'SCAN',cursor,'MATCH','fc:*','COUNT',100)
    if keys:
     answer=self.redis(source,'MIGRATE',dest,6379,'',0,5000,'COPY','REPLACE','AUTH',self.env['REDIS_PASSWORD'],'KEYS',*keys)
     assert answer in ('OK','NOKEY'),answer
    if str(cursor)=='0':break
  self.configure(wait,mode);self.dc('up','-d','--no-deps','app','relay','worker');self.ready()
 def ready(self):
  for port in self.ports:h.until(lambda:urllib.request.urlopen(f'http://127.0.0.1:{port}/health/ready',timeout=3).status==200,180)
 def request(self,path,body=None,user=None,trace_id=None):
  import base64,hmac
  headers={'X-Admin-Key':self.env['ADMIN_KEY']}
  if user is not None:
   payload=base64.urlsafe_b64encode(f'{user}:{int(time.time())+3600}'.encode()).rstrip(b'=')
   headers={'Authorization':'Bearer '+payload.decode()+'.'+hmac.new(self.env['USER_KEY'].encode(),payload,hashlib.sha256).hexdigest()}
  headers['Content-Type']='application/json'
  if trace_id:headers['traceparent']=f'00-{trace_id}-0123456789abcdef-01'
  req=urllib.request.Request(self.base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
  try:
   with urllib.request.urlopen(req,timeout=15) as r:return r.status,json.load(r)
  except urllib.error.HTTPError as e:return e.code,json.load(e)
 def template(self,stock):
  now=int(time.time());status,t=self.request('/admin/templates',dict(merchant_id=time.time_ns()%1000000000000,name='redis-ha-lab',stock=stock,per_user=1,threshold=1000,discount=100,starts_at=now-2,ends_at=now+3600,valid_until=now+7200,mode='async'));assert status==201,(status,t);return t
 def claim(self,t,u,r='same'):
  try:return self.request('/v1/claims',dict(merchant_id=t['merchant_id'],template_id=t['id'],request_id=r),u)
  except (OSError,urllib.error.URLError):return 0,{'outcome':'unknown'}
 def audit(self,t):
  status,a=self.request(f"/admin/audit?merchant_id={t['merchant_id']}&template_id={t['id']}");assert status==200,(status,a);return a[0]
 def drain(self,t,count):
  def done():
   a=self.audit(t)
   return a if a['consistent'] and a['pending']==0 and a['issued']==a['projected']==count else None
  return h.until(done,180)
 def redis(self,node,*args,sentinel=False):
  password='REDIS_SENTINEL_PASSWORD' if sentinel else 'REDIS_PASSWORD'
  cmd=['exec','-T',node,'sh','-c',f'export REDISCLI_AUTH="${password}"; exec redis-cli --json '+('-p 26379 ' if sentinel else '')+'"$@"','sh',*map(str,args)]
  raw=self.dc(*cmd).strip()
  if raw.startswith('error:'):raise RuntimeError(raw)
  if args and str(args[0]).upper()=='INFO' and raw.startswith('#'):return raw
  return json.loads(raw)
 def barrier(self,node,count=2):
  # SET + WAIT through one redis-cli stdin connection, including idempotent setup.
  raw=self.dc('exec','-T',node,'sh','-c',f'printf "SET fc:ha:fixture-barrier {time.time_ns()}\\nWAIT {count} 2000\\n" | REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli --raw')
  assert int(raw.splitlines()[-1])>=count,raw
 def sql(self,query):
  return self.dc('exec','-T','mysql','mysql','-N','-uroot','-pflashcoupon','control','-e',query)
 def master(self):
  for sentinel in ('sentinel-1','sentinel-2','sentinel-3'):
   try:
    address=self.redis(sentinel,'SENTINEL','get-master-addr-by-name','flashcoupon',sentinel=True)[0]
    return {'.11':'redis','.12':'redis-replica-1','.13':'redis-replica-2'}.get('.'+address.split('.')[-1],address)
   except Exception:pass
  return None
 def info(self,node):
  raw=self.redis(node,'INFO','replication');return dict(line.split(':',1) for line in raw.splitlines() if ':' in line)
 def synced(self):
  def ok():
   try:
    m=self.master()
    if not m:return None
    f=self.info(m);return m if f.get('role')=='master' and int(f.get('connected_slaves','0'))==2 and all(f.get('slave'+str(i),'').find('state=online')>=0 for i in (0,1)) else None
   except (RuntimeError,ValueError,KeyError):return None
  return h.until(ok,120)
 def close(self):
  for s in self.servers:s.shutdown();s.server_close()
  if self.ssh:self.ssh.close()

def main():
 p=argparse.ArgumentParser();p.add_argument('--host');p.add_argument('--user');p.add_argument('--port',type=int,default=22);p.add_argument('--password-env',default='FLASHCOUPON_SSH_PASSWORD');p.add_argument('--directory',default='/home/public/bjh/Flashcoupon');p.add_argument('--setup',action='store_true');args=p.parse_args()
 lab=Lab(args)
 try:
  lab.setup()
  if args.setup:lab.start();print('HA deployment healthy; master:',lab.synced(),flush=True)
 finally:lab.close()

if __name__=='__main__':main()
