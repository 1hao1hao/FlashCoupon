"""Native k6 paired HA experiments. One lab/SQL/Proxy/MQ, verified fixture copy
between single-node and HA Redis with writers stopped. Never deletes data/volumes.
"""
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
from pathlib import Path
import shlex
import subprocess
import sys
import threading
import time
import urllib.request
from redis_ha_faults import parser
from redis_ha_lab import Lab,OUT,h

def main():
 p=parser();p.add_argument('--k6',default='k6');p.add_argument('--only',choices=['normal','fault']);args=p.parse_args()
 logging.getLogger('paramiko.transport').setLevel(logging.CRITICAL)
 out=OUT/'benchmark'/args.run_tag;out.mkdir(parents=True,exist_ok=True);lab=Lab(args);mode='sentinel';tid='setup'
 def collect(test_id,redis_mode):
  # Never reuse a prior collector after an interrupted SSH session.
  if lab.remote:
   lab.shell('test ! -e reports/server/'+test_id+'.json && test ! -e reports/server/'+test_id+'.done')
  elif (h.LOCAL/'server'/(test_id+'.json')).exists():raise FileExistsError('choose a new run tag')
  cmd=['python3' if lab.remote else sys.executable,'scripts/server_collect.py','--project',lab.project,'--test-id',test_id,'--duration','300','--api-port','18880','--relay-port','18881','--worker-port','18882']
  cmd+=['--sentinel-service','sentinel-1'] if redis_mode=='sentinel' else ['--redis-service','redis-single']
  if lab.remote:lab.shell('mkdir -p reports/server; nohup '+' '.join(shlex.quote(s) for s in cmd)+' > reports/'+test_id+'-collector.log 2>&1 < /dev/null &');return None
  return subprocess.Popen(cmd,cwd=h.ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 def finish_collect(test_id,process):
  lab.write('reports/server/'+test_id+'.stop','stop')
  if process:process.wait(timeout=20)
  else:h.until(lambda:lab.shell('if test -f reports/server/'+test_id+'.done; then printf done; fi').strip()=='done',30)
  if lab.remote:
   s=lab.ssh.open_sftp();s.get(lab.directory+'/reports/server/'+test_id+'.json',str(out/(test_id+'-server.json')));s.close()
  else:(out/(test_id+'-server.json')).write_bytes((h.LOCAL/'server'/(test_id+'.json')).read_bytes())
 try:
  lab.setup();lab.start();lab.synced()
  env=os.environ.copy();env.update({k:v for k,v in lab.env.items() if k in ('ADMIN_KEY','USER_KEY')})
  for round_id in range(1,4):
   cases=[('standalone',0),('sentinel',0),('sentinel',1),('sentinel',2)]
   if round_id%2==0:cases=list(reversed(cases))
   if args.only!='fault':
    for next_mode,wait in cases:
     tid=f'ha-normal-{next_mode}-wait{wait}-r{round_id}-{args.run_tag}'
     if (out/(tid+'-result.json')).exists():continue
     print('experiment',tid,flush=True);lab.switch_mode(wait,next_mode,mode);mode=next_mode
     if mode=='sentinel':lab.synced()
     process=collect(tid,mode)
     subprocess.run([sys.executable,'scripts/remote_client.py','--base-url',lab.base,'--test-id',tid,'--k6',str(Path(args.k6).resolve()),'--output',str(out),'--requests','500','--vus','30'],env=env,cwd=h.ROOT,check=True,timeout=600)
     raw=json.loads((out/(tid+'.json')).read_text());a=lab.drain(raw['test_template'],500)
     finish_collect(tid,process)
     (out/(tid+'-result.json')).write_text(json.dumps(dict(test_id=tid,scenario='normal',mode=mode,wait=wait,measurements=raw['measurements'],audit=a),indent=2)+'\n',encoding='utf-8')
   if args.only=='normal':continue
   # Pacing/replays identical in control and failure case; report separately
   # from unrestricted normal QPS. A failed response is not an accepted coupon.
   for fault in (False,True):
    tid=f'ha-failover-{int(fault)}-wait1-r{round_id}-{args.run_tag}'
    if (out/(tid+'-result.json')).exists():continue
    print('experiment',tid,flush=True);lab.switch_mode(1,'sentinel',mode);mode='sentinel';old=lab.synced();tpl=lab.template(500)
    process=collect(tid,mode);record={};errors=[]
    def inject():
     try:
      prefix=f"fc:t:{tpl['merchant_id']}:{tpl['id']}:"
      h.until(lambda:lab.redis(old,'HLEN',prefix+'reservations')>=5,30)
      record['injected_unix']=time.time();start=time.perf_counter()
      cmd=['docker','stop','-t','0',lab.project+'-'+old+'-1']
      lab.shell(' '.join(shlex.quote(s) for s in cmd) if lab.remote else cmd)
      new=h.until(lambda:lab.master() if lab.master()!=old else None,45)
      record.update(old_master=old,new_master=new,election_seconds=time.perf_counter()-start)
      lab.dc('start',old);lab.synced();record['all_nodes_restored_seconds']=time.perf_counter()-start
     except Exception as e:errors.append(type(e).__name__+': '+str(e))
    env2=env.copy();env2.update(BASE_URL=lab.base,MODE='async',ITERATIONS='500',STOCK='500',VUS='30',TEST_ID=tid,REPORT_DIR=out.as_posix(),REPORT_NAME=tid,TEMPLATE_JSON=json.dumps(tpl))
    thread=threading.Thread(target=inject) if fault else None
    if thread:thread.start()
    log=h.LOCAL/(tid+'.log')
    with log.open('wb') as f:r=subprocess.run([str(Path(args.k6).resolve()),'run','tests/k6/redis_ha_fault.js'],env=env2,cwd=h.ROOT,stdout=f,stderr=subprocess.STDOUT,timeout=600)
    if thread:thread.join(timeout=120)
    assert not errors and (not thread or not thread.is_alive()),errors
    assert r.returncode==0,'fault k6 validation failed, see ignored report log'
    raw=json.loads((out/(tid+'.json')).read_text());assert raw['metrics']['checks']['values']['rate']==1
    if fault:assert raw['measurements']['initial_unknown']+raw['measurements']['initial_safe_rejected']>0,'fault missed request window'
    a=lab.drain(tpl,500);finish_collect(tid,process)
    (out/(tid+'-result.json')).write_text(json.dumps(dict(test_id=tid,scenario='fault' if fault else 'paced_control',mode=mode,wait=1,measurements=raw['measurements'],audit=a,fault=record),indent=2)+'\n',encoding='utf-8')
 except Exception as e:
  (out/'failure.json').write_text(json.dumps(dict(passed=False,error=type(e).__name__+': '+str(e),test_id=tid),indent=2)+'\n',encoding='utf-8')
  raise
 finally:
  try:
   lab.dc('start','redis','redis-replica-1','redis-replica-2')
   if mode!='sentinel':
    lab.configure(0,mode);lab.dc('up','-d','--no-deps','app','relay','worker');lab.ready()
    lab.switch_mode(1,'sentinel',mode)
   else:lab.configure(1);lab.dc('up','-d','--no-deps','app','relay','worker')
  finally:lab.close()

if __name__=='__main__':main()
