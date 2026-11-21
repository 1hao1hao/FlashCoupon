"""Real failover/partition/WAIT matrix in the isolated HA lab. No volume deletion."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import time
import urllib.request
from redis_ha_lab import Lab,OUT,h

def parser():
 p=argparse.ArgumentParser();p.add_argument('--host');p.add_argument('--user');p.add_argument('--port',type=int,default=22);p.add_argument('--password-env',default='FLASHCOUPON_SSH_PASSWORD');p.add_argument('--directory',default='/home/public/bjh/Flashcoupon');p.add_argument('--run-tag',default='run1');return p

def run(lab,out):
 results=[]
 def save(name,data):
  data['scenario']=name;data['passed']=True
  (out/(name+'.json')).write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8');results.append(data);print('PASS',name,flush=True)
 def replicas():return [n for n in ('redis','redis-replica-1','redis-replica-2') if n!=lab.master()]
 def all_roles():
  snap={}
  for role,port in zip(('api','relay','worker'),lab.ports):
   text=urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics',timeout=5).read().decode();snap[role]=[x for x in text.splitlines() if x.startswith('flashcoupon_redis_')]
  return snap
 def audit(t,n):
  a=lab.drain(t,n);m=t['merchant_id'];tid=t['id']
  row=lab.sql(f"SELECT COUNT(*),COUNT(DISTINCT user_id) FROM grants WHERE merchant_id={m} AND template_id='{tid}' AND state='issued'; SELECT COALESCE(MAX(issued),0) FROM user_limits WHERE merchant_id={m} AND template_id='{tid}';").splitlines()
  assert row[0].split()==[str(n),str(n)] and int(row[1])<=1,row
  return dict(audit=a,unique_users=n,max_per_user=int(row[1]))
 lab.setup();lab.start();lab.synced()
 t=lab.template(5)
 for u in range(101,106):assert lab.claim(t,u)[0]==202
 save('healthy_replication',dict(master=lab.master(),final=audit(t,5)))
 replica=replicas()[0];t=lab.template(2);lab.dc('stop',replica)
 h.until(lambda:int(lab.info(lab.master()).get('connected_slaves',0))==1,15)
 assert lab.claim(t,201)[0]==202;assert lab.claim(t,202)[0]==202
 save('one_replica_down_wait1',dict(final=audit(t,2)))
 lab.dc('start',replica);lab.synced()
 # WAIT timeout after a successful Lua must be UNKNOWN, never implicit rollback.
 lab.switch_mode(2);t=lab.template(1);lab.dc('stop','relay');replica=replicas()[0];lab.dc('stop',replica)
 h.until(lambda:int(lab.info(lab.master()).get('connected_slaves',0))==1,15)
 status,response=lab.claim(t,301);assert status==503 and response.get('outcome')=='unknown',(status,response)
 p=f"fc:t:{t['merchant_id']}:{t['id']}:";eid=response['event_id'];raw=lab.redis(lab.master(),'HGET',p+'reservations',eid)
 assert raw and lab.redis(lab.master(),'GET',p+'stock')=='0'
 lab.dc('start',replica);lab.synced();status,replay=lab.claim(t,301);assert status==202 and replay['event']['id']==eid
 lab.dc('start','relay');save('wait_timeout_and_idempotent_replay',dict(initial_status=503,initial_outcome='unknown',reservation_retained=True,replay_status=status,final=audit(t,1)))
 lab.switch_mode(1);t=lab.template(1);rs=replicas();lab.dc('stop',*rs);time.sleep(3)
 status,response=lab.claim(t,401);assert status==503 and response.get('outcome')=='safe_rejected',(status,response)
 p=f"fc:t:{t['merchant_id']}:{t['id']}:";assert lab.redis(lab.master(),'HLEN',p+'reservations')==0
 lab.dc('start',*rs);lab.synced();assert lab.claim(t,401)[0]==202
 save('insufficient_replicas_reject_before_lua',dict(status=status,outcome=response['outcome'],reservation_count_before_restore=0,final=audit(t,1)))
 lab.dc('stop','sentinel-1');t=lab.template(1);assert lab.claim(t,501)[0]==202
 save('one_sentinel_down',dict(final=audit(t,1)));lab.dc('start','sentinel-1')
 # Preserve a real pending entry across promotion; new Relay must reclaim it.
 t=lab.template(100);lab.dc('stop','relay','worker')
 for u in range(1001,1041):assert lab.claim(t,u)[0]==202
 old=lab.synced();pending=lab.redis(old,'XREADGROUP','GROUP','relay','dead-relay','COUNT',1,'STREAMS','fc:outbox','>')
 assert lab.redis(old,'XPENDING','fc:outbox','relay')[0]>=1
 lab.barrier(old)
 begin=time.perf_counter();lab.dc('stop','-t','0',old)
 with ThreadPoolExecutor(max_workers=20) as pool:initial=list(pool.map(lambda u:lab.claim(t,u),range(1041,1101)))
 new=h.until(lambda:lab.master() if lab.master()!=old else None,45)
 switch_seconds=time.perf_counter()-begin
 # Same three Go processes reconnect; they are not redeployed to learn the master.
 lab.dc('start',old);lab.synced()
 for u in range(1001,1101):h.until(lambda:lab.claim(t,u)[0]==202,60)
 lab.dc('start','relay','worker');final=audit(t,100)
 events=[lab.redis(new,'HGET',f"fc:t:{t['merchant_id']}:{t['id']}:reservations",lab.claim(t,u)[1]['event']['id']) for u in range(1001,1006)]
 for event in events:
  for _ in range(2):lab.redis(new,'XADD','fc:outbox','*','event',event)
 h.until(lambda:lab.redis(new,'XLEN','fc:outbox')==0,90)
 save('master_down_pending_reclaim_and_duplicates',dict(old_master=old,new_master=new,switch_seconds=switch_seconds,concurrent_responses={str(s):sum(x[0]==s for x in initial) for s in sorted(set(x[0] for x in initial))},extra_copies=10,final=audit(t,100),metrics=all_roles()))
 # Isolate the running old master from the entire Compose network.
 old=lab.synced();t=lab.template(1);lab.barrier(old);network=lab.project+'_redis-ha';container=lab.project+'-'+old+'-1'
 disconnected=False
 try:
  lab.shell(f'docker network disconnect {network} {container}' if lab.remote else ['docker','network','disconnect',network,container]);disconnected=True
  new=h.until(lambda:lab.master() if lab.master()!=old else None,45);time.sleep(3)
  try:lab.redis(old,'SET','fc:ha:isolated-probe','no');raise AssertionError('isolated old master accepted write')
  except RuntimeError as e:assert 'NOREPLICAS' in str(e),str(e)
  h.until(lambda:lab.claim(t,2001)[0]==202,60)
 finally:
  if disconnected:
   ip=lab.env.get('REDIS_HA_PREFIX','172.29.239')+'.'+{'redis':'11','redis-replica-1':'12','redis-replica-2':'13'}[old]
   lab.shell(f'docker network connect --ip {ip} --alias {old} {network} {container}' if lab.remote else ['docker','network','connect','--ip',ip,'--alias',old,network,container])
 lab.synced();assert lab.info(old)['role']=='slave'
 try:lab.redis(old,'SET','fc:ha:old-master-probe','no');raise AssertionError('old master remained writable')
 except RuntimeError as e:assert 'READONLY' in str(e),str(e)
 save('master_network_partition_and_old_master_rejoin',dict(old_master=old,new_master=new,old_master_write_rejected=True,old_master_rejoined_as='slave',final=audit(t,1)))
 # Without Sentinel majority there must be no automatic promotion.
 old=lab.synced();t=lab.template(1);lab.barrier(old);lab.dc('stop','sentinel-1','sentinel-2');lab.dc('stop','-t','0',old);time.sleep(6)
 assert lab.master()==old
 status,response=lab.claim(t,3001);assert status!=202
 lab.dc('start','sentinel-1','sentinel-2');new=h.until(lambda:lab.master() if lab.master()!=old else None,45)
 lab.dc('start',old);lab.synced();h.until(lambda:lab.claim(t,3001)[0]==202,60)
 save('sentinel_majority_required',dict(no_quorum_status=status,new_master=new,final=audit(t,1)))
 # Partial ledger loss is a closed gate, not a SQL-inventory reset.
 t=lab.template(2);lab.dc('stop','relay','worker');status,r=lab.claim(t,4001);assert status==202
 ev=r['event'];p=f"fc:t:{t['merchant_id']}:{t['id']}:";master=lab.master();raw=lab.redis(master,'HGET',p+'reservations',ev['id'])
 try:
  lab.redis(master,'HDEL',p+'reservations',ev['id'])
  status,r=lab.claim(t,4002);assert status==503 and r.get('outcome')=='safe_rejected',(status,r)
  assert lab.redis(master,'GET',p+'stock')=='1'
  lab.request('/admin/reconcile',{});assert lab.redis(master,'GET',p+'stock')=='1','unsafe automatic inventory reset'
 finally:lab.redis(master,'HSET',p+'reservations',ev['id'],raw)
 assert lab.claim(t,4002)[0]==202;lab.dc('start','relay','worker')
 save('partial_ledger_fail_closed',dict(status=status,sql_inventory_reset=False,restored_from='exact saved reservation payload',final=audit(t,2)))
 save('final_roles_and_audit',dict(master=lab.synced(),metrics=all_roles(),audits=lab.request('/admin/audit')[1]))
 assert all(a['consistent'] and a['pending']==0 for a in results[-1]['audits'])
 (out/'summary.json').write_text(json.dumps(dict(passed=True,scenarios=len(results),results=results),indent=2)+'\n',encoding='utf-8')

def main():
 logging.getLogger('paramiko.transport').setLevel(logging.CRITICAL)
 args=parser().parse_args();out=OUT/'faults'/args.run_tag;out.mkdir(parents=True,exist_ok=True)
 if (out/'summary.json').exists():raise FileExistsError('choose a new run tag')
 lab=Lab(args)
 try:run(lab,out)
 finally:
  try:
   lab.dc('start','redis','redis-replica-1','redis-replica-2','sentinel-1','sentinel-2','sentinel-3');lab.configure(1);lab.dc('up','-d','--no-deps','app','relay','worker')
  finally:lab.close()

if __name__=='__main__':main()
