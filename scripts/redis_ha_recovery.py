"""Additional live-process recovery, persisted Sentinel identity and trace proof."""
import hashlib
import json
import logging
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from redis_ha_faults import parser
from redis_ha_lab import Lab,OUT,h

def main():
 logging.getLogger('paramiko.transport').setLevel(logging.CRITICAL)
 args=parser().parse_args();lab=Lab(args);out=OUT/'recovery'/args.run_tag;out.mkdir(parents=True,exist_ok=True)
 if (out/'summary.json').exists():lab.close();raise FileExistsError('choose a new run tag')
 results=[]
 def save(name,data):
  data.update(passed=True,scenario=name);results.append(data);(out/(name+'.json')).write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8');print('PASS',name,flush=True)
 def runtime_ids():
  cmd=['docker','inspect','--format','{{.Id}} {{.RestartCount}}',*[lab.project+'-'+s+'-1' for s in ('app','relay','worker')]]
  return lab.shell(' '.join(cmd[:3])+" '{{.Id}} {{.RestartCount}}' "+' '.join(cmd[4:])) if lab.remote else lab.shell(cmd)
 try:
  lab.setup();lab.start();master=lab.synced()
  # Losing group metadata must not require a process restart or discard entries.
  relay=lab.project+'-relay-1'
  lab.shell('docker pause '+relay if lab.remote else ['docker','pause',relay])
  try:
   t=lab.template(2)
   for u in (7101,7102):assert lab.claim(t,u)[0]==202
   assert lab.redis(master,'XGROUP','DESTROY','fc:outbox','relay')==1
  finally:lab.shell('docker unpause '+relay if lab.remote else ['docker','unpause',relay])
  final=lab.drain(t,2)
  assert lab.redis(master,'XGROUP','DESTROY','fc:batch-outbox','batch')==1
  h.until(lambda:any((g.get('name') if isinstance(g,dict) else g[g.index('name')+1])=='batch' for g in lab.redis(master,'XINFO','GROUPS','fc:batch-outbox')),20)
  save('consumer_group_loss_without_process_restart',dict(final=final,batch_group_restored=True))
  # Persisted identities and all data-node AOFs survive a complete Redis-tier stop.
  ids=runtime_ids();sentinels=('sentinel-1','sentinel-2','sentinel-3');nodes=('redis','redis-replica-1','redis-replica-2')
  sentinel_ids={s:lab.redis(s,'SENTINEL','MYID',sentinel=True) for s in sentinels}
  t=lab.template(5)
  for u in (7201,7202,7203):assert lab.claim(t,u)[0]==202
  lab.drain(t,3);lab.barrier(master);start=time.perf_counter()
  lab.dc('stop',*sentinels);lab.dc('stop','-t','0',*nodes)
  status,response=lab.claim(t,7204);assert status!=202
  lab.dc('start',*nodes)
  h.until(lambda:lab.info(master).get('role')=='master',30)
  lab.dc('start',*sentinels);assert lab.synced()==master;lab.ready()
  for u in (7204,7205):h.until(lambda:lab.claim(t,u)[0]==202,45)
  final=lab.drain(t,5);assert runtime_ids()==ids,'Go roles were restarted rather than reconnected'
  assert {s:lab.redis(s,'SENTINEL','MYID',sentinel=True) for s in sentinels}==sentinel_ids
  save('all_redis_aof_and_sentinel_state_restart',dict(master=master,go_processes_restarted=False,sentinel_ids_retained=True,during_outage_status=status,recovery_seconds=time.perf_counter()-start,final=final))
  # The main application image never exposes Redis or Sentinel host ports.
  for service in (*nodes,*sentinels):
   cmd=['docker','inspect','--format','{{json .HostConfig.PortBindings}}',lab.project+'-'+service+'-1']
   raw=lab.shell(' '.join(cmd[:3])+" '{{json .HostConfig.PortBindings}}' "+cmd[-1]) if lab.remote else lab.shell(cmd)
   assert not any((json.loads(raw) or {}).values()),service
  for service,sentinel in ((master,False),('sentinel-1',True)):
   try:lab.redis(service,'AUTH','wrong-fixture-password',sentinel=sentinel);raise AssertionError('authentication bypass')
   except RuntimeError as e:assert 'WRONGPASS' in str(e),str(e)
  save('private_ports_and_authentication',dict(redis_and_sentinel_host_ports=False,invalid_credentials_rejected=True))
  # Lua has executed, but the API dies before WAIT/the HTTP response. Do not
  # infer rollback; replay the identical request against the retained ledger.
  t=lab.template(1);user=7251;request_id='response-lost'
  event_id=hashlib.sha256(f"{t['merchant_id']}/{user}/{t['id']}/{request_id}".encode()).hexdigest()
  lab.shell('mkdir -p reports/ha-response-gate; chmod 777 reports/ha-response-gate; rm -f reports/ha-response-gate/reached reports/ha-response-gate/release' if lab.remote else ['python','-c',"from pathlib import Path;p=Path('reports/ha-response-gate');p.mkdir(exist_ok=True);[(p/x).unlink(missing_ok=True) for x in ('reached','release')]"])
  gate='services:\n  app:\n    environment:\n      FAULT_INJECTION_ENABLED: "1"\n      FAULT_POINT: after_redis_eval\n      FAULT_EVENT_ID: '+event_id+'\n      FAULT_GATE_DIR: /fault-gate\n    volumes: ["./reports/ha-response-gate:/fault-gate"]\n'
  lab.write('reports/ha-response.compose.yaml',gate);lab.files.append('reports/ha-response.compose.yaml')
  try:
   lab.dc('up','-d','--no-deps','app');lab.ready()
   with ThreadPoolExecutor(max_workers=1) as pool:
    future=pool.submit(lab.claim,t,user,request_id)
    h.until(lambda:lab.shell('if test -f reports/ha-response-gate/reached; then printf reached; fi').strip()=='reached' if lab.remote else (h.LOCAL/'ha-response-gate'/'reached').exists(),30)
    assert lab.redis(lab.master(),'HGET',f"fc:t:{t['merchant_id']}:{t['id']}:reservations",event_id)
    lab.dc('stop','-t','0','app');status,response=future.result(timeout=15);assert status==0
  finally:
   lab.files.pop();lab.dc('up','-d','--no-deps','app');lab.ready()
  status,replay=lab.claim(t,user,request_id);assert status==202 and replay['event']['id']==event_id
  save('api_crash_after_lua_before_wait',dict(initial_http_response='connection_lost_or_unconfirmed',replay_status=status,identical_event_id=True,final=lab.drain(t,1)))
  tid=hashlib.sha256(f'ha-trace-{time.time_ns()}'.encode()).hexdigest()[:32];t=lab.template(1)
  status,r=lab.request('/v1/claims',dict(merchant_id=t['merchant_id'],template_id=t['id'],request_id='trace'),7301,trace_id=tid);assert status==202
  lab.drain(t,1);jaeger='http://127.0.0.1:'+('19686' if lab.remote else '18686')
  def trace():
   try:d=json.load(urllib.request.urlopen(jaeger+'/api/v3/traces/'+tid,timeout=5))
   except Exception:return None
   resources=d.get('result',{}).get('resourceSpans',[])
   services={a['value'].get('stringValue') for r in resources for a in r.get('resource',{}).get('attributes',[]) if a['key']=='service.name'}
   spans={s['name'] for r in resources for scope in r.get('scopeSpans',[]) for s in scope.get('spans',[])}
   return d if services>={'flashcoupon-api','flashcoupon-relay','flashcoupon-worker'} and spans>={'redis.wait','redis.ha_guard','sql.commit','redis.finalize'} else None
  proof=h.until(trace,30);(out/'trace.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf-8')
  audits=lab.request('/admin/audit')[1];assert all(a['consistent'] and a['pending']==0 for a in audits)
  metrics={role:[x for x in urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics').read().decode().splitlines() if x.startswith('flashcoupon_redis_')] for role,port in zip(('api','relay','worker'),lab.ports)}
  save('trace_and_final_global_audit',dict(trace_id=tid,three_services=True,templates=len(audits),pending=0,inconsistent=0,metrics=metrics))
  (out/'summary.json').write_text(json.dumps(dict(passed=True,scenarios=len(results),results=results),indent=2)+'\n',encoding='utf-8')
 finally:lab.close()

if __name__=='__main__':main()
