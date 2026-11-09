"""Real independent-process failure tests. No mocked broker or destructive volume reset."""
from concurrent.futures import ThreadPoolExecutor
import json
import time
from support import *

def main():
    ready();results=[]
    t=template(100)
    dc('stop','relay','worker')
    graceful={s:json.loads(run(['docker','inspect',f'flashcoupon-{s}-1']))[0]['State'] for s in ('relay','worker')}
    assert all(v['ExitCode']==0 and not v['OOMKilled'] for v in graceful.values()),graceful
    with ThreadPoolExecutor(max_workers=25) as pool:
        responses=list(pool.map(lambda u:claim(t,u),range(200001,200101)))
    assert all(s==202 for s,_ in responses)
    events=[r['event'] for _,r in responses]
    # Assign an unacknowledged entry to a dead consumer, then recover with a different Relay process.
    redis('XREADGROUP','GROUP','relay','crashed-fixture','COUNT',1,'STREAMS','fc:outbox','>')
    observed_pending=redis('XPENDING','fc:outbox','relay')[0];assert observed_pending>=1
    dc('start','relay')
    until(lambda:redis('XPENDING','fc:outbox','relay')[0]==0,60)
    before=audit(t);assert before['issued']==0 and before['pending']==100,before
    enqueue(events,2)
    # Hold the fixture's template lock until Worker is demonstrably inside a blocked
    # SQL transaction. This proves in-flight crash recovery, not merely an idle restart.
    blocker=subprocess.Popen(DC+['exec','-T','mysql','mysql','-uroot','-pflashcoupon','control','-e',f"BEGIN; SELECT id FROM templates WHERE merchant_id={t['merchant_id']} AND id='{t['id']}' FOR UPDATE; SELECT GET_LOCK('fc_worker_fault',0); SELECT SLEEP(60); ROLLBACK;"],cwd=ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    def lock_connection():
        value=sql("SELECT IS_USED_LOCK('fc_worker_fault');").splitlines()[-1]
        return int(value) if value!='NULL' else None
    connection=until(lock_connection,15)
    dc('start','worker')
    waits=until(lambda:int(sql("SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state='LOCK WAIT';").splitlines()[-1]) or None,30)
    dc('kill','worker')
    interrupted=audit(t)
    assert interrupted['issued']==0,interrupted
    sql(f'KILL {connection};');blocker.wait(timeout=10)
    dc('start','worker');after=drain(t,100)
    results.append(dict(scenario='relay pending reclaim + duplicate MQ delivery + worker crash/restart',accepted=100,extra_stream_copies=200,observed_pending=observed_pending,blocked_transactions=waits,interrupted_issued=interrupted['issued'],final=after,graceful_exit_codes={s:v['ExitCode'] for s,v in graceful.items()}))
    # Same user, different requests: Redis and MySQL must both enforce a single coupon.
    one=template(20)
    with ThreadPoolExecutor(max_workers=20) as pool:
        rs=list(pool.map(lambda i:claim(one,310001,str(i)),range(20)))
    assert sum(s==202 for s,_ in rs)==1 and sum(s==409 for s,_ in rs)==19,rs
    results.append(dict(scenario='one user different request IDs',accepted=1,rejected=19,final=drain(one,1)))
    # Broker outage + Relay process crash + Redis AOF restart; API stays independent of MQ.
    outage=template(20);dc('stop','broker')
    ev=[]
    for u in range(400001,400021):
        status,r=claim(outage,u);assert status==202;(ev.append(r['event']))
    before=audit(outage);assert before['pending']==20 and before['issued']==0,before
    dc('kill','relay','worker');dc('restart','redis');dc('start','broker');dc('up','-d','relay','worker')
    started=time.monotonic();ready();after=drain(outage,20)
    recovery_seconds=round(time.monotonic()-started,3)
    for u,original in zip(range(400001,400021),ev):
        status,r=claim(outage,u);assert status==202 and r['event']==original
    results.append(dict(scenario='broker outage + process crash + Redis AOF recovery',before=before,final=after,recovery_seconds=recovery_seconds,replayed_requests=20))
    # Permanent business rejection, at-least-once compensation, and count repair.
    comp=template(1);m=comp['merchant_id'];tid=comp['id'];prefix=f'fc:t:{m}:{tid}:'
    sql(f"UPDATE templates SET remaining=0 WHERE merchant_id={m} AND id='{tid}';")
    status,r=claim(comp,500001);assert status==202
    until(lambda:redis('HGET',prefix+'states',r['event']['id'])=='failed')
    enqueue([r['event']],3);time.sleep(2)
    assert redis('GET',prefix+'stock')=='1' and redis('HGET',prefix+'users',500001)=='0'
    sql(f"UPDATE templates SET remaining=1 WHERE merchant_id={m} AND id='{tid}';")
    redis('SET',prefix+'stock',6);redis('HSET',prefix+'users',500001,5)
    status,_=request('/admin/reconcile',{});assert status==200
    after=drain(comp,0);assert after['failed']==1 and redis('HGET',prefix+'users',500001) is None
    results.append(dict(scenario='permanent rejection compensation replay and reconciliation',replayed=3,final=after))
    direct=template(2,'direct',2);user=600001
    status,r=claim(direct,user);assert status==200;cid=r['event']['id']
    body=dict(merchant_id=direct['merchant_id'],amount=1000,order_id='synthetic-'+direct['id']+'-1')
    with ThreadPoolExecutor(max_workers=20) as pool:
        rs=list(pool.map(lambda _:request(f'/v1/coupons/{cid}/redeem',body,user)[0],range(20)))
    assert rs==[200]*20,rs
    assert request(f'/v1/coupons/{cid}/redeem',dict(body,order_id='synthetic-'+direct['id']+'-2'),user)[0]==409
    status,r=claim(direct,user,'two');assert status==200
    assert request(f"/v1/coupons/{r['event']['id']}/redeem",body,user)[0]==409
    results.append(dict(scenario='duplicate redemption and order uniqueness',same_order_success_responses=20,conflicting_orders_rejected=2,final=drain(direct,2)))
    snapshots={}
    for port,role in ((8080,'api'),(8081,'relay'),(8082,'worker')):
        text=urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics').read().decode()
        assert f'role="{role}"' in text
        # Keep only bounded application metrics, never request IDs or user data.
        snapshots[role]=[line for line in text.splitlines() if line.startswith('flashcoupon_')]
    save('faults.json',dict(passed=True,scenarios=results,metrics=snapshots))
    print('Independent process faults passed:',len(results),flush=True)

if __name__=='__main__':
    try: main()
    finally: dc('up','-d','app','relay','worker')
