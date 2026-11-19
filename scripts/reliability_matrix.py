"""Deterministic process-local fault gates, real Redis/MQ/MySQL, no mocks.
Use on the local test Compose stack only. No volume or ledger is deleted.
"""
from concurrent.futures import ThreadPoolExecutor
import json
import hashlib
from pathlib import Path
import subprocess
import time
import support as h

EVIDENCE=h.ROOT/'evidence/observability-2026-11-19/reliability'
EVIDENCE.mkdir(parents=True,exist_ok=True)
OVERRIDE=h.LOCAL/'fault-matrix.compose.json'
BASE=h.DC+['-f','compose.observability.yaml','-f',str(OVERRIDE)]

def dc(*args):return h.run(BASE+list(args))
def configure(role=None,point=None,event_id=""):
    services={}
    for service in ('app','relay','worker'):
        env={'FAULT_INJECTION_ENABLED':'0','FAULT_POINT':'','FAULT_GATE_DIR':'/fault-gate'}
        cfg={'environment':env}
        if service==role:
            env.update(FAULT_INJECTION_ENABLED='1',FAULT_POINT=point,FAULT_EVENT_ID=event_id)
            cfg['volumes']=[str((h.LOCAL/'fault-gates'/point).resolve()).replace('\\','/')+':/fault-gate']
        services[service]=cfg
    OVERRIDE.write_text(json.dumps({'services':services}),encoding='utf-8')

def save(name,data):(EVIDENCE/(name+'.json')).write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8')

def audit_all(t,expected):
    a=h.drain(t,expected)
    row=h.sql(f"SELECT COUNT(*),COUNT(DISTINCT user_id) FROM grants WHERE merchant_id={t['merchant_id']} AND template_id='{t['id']}' AND state='issued';").splitlines()[-1].split()
    users=int(row[1]);assert int(row[0])==expected
    assert users==expected  # these fixtures have per_user=1
    counts=h.sql(f"SELECT COALESCE(MAX(issued),0) FROM user_limits WHERE merchant_id={t['merchant_id']} AND template_id='{t['id']}';").splitlines()[-1]
    assert int(counts)<=1
    return dict(audit=a,unique_users=users,max_per_user=int(counts))

def main():
    configure();dc('up','-d','--no-deps','app','relay','worker');h.ready();results=[]
    points=[('app','after_lua'),('relay','after_stream_read'),('relay','after_mq_send'),
            ('worker','before_sql_commit'),('worker','after_sql_commit'),('worker','before_projection'),
            ('worker','after_projection'),('worker','before_consume_ack')]
    for role,point in points:
        print('fault',point,flush=True)
        gate=h.LOCAL/'fault-gates'/point;gate.mkdir(parents=True,exist_ok=True)
        for file in ('reached','release'):(gate/file).unlink(missing_ok=True)
        t=h.template(1);user=880001
        event_id=hashlib.sha256(f"{t['merchant_id']}/{user}/{t['id']}/lost-response".encode()).hexdigest()
        configure(role,point,event_id);dc('up','-d','--no-deps',role)
        h.until(lambda:h.request('/health/ready',base=f'http://127.0.0.1:{dict(app=8080,relay=8081,worker=8082)[role]}')[0]==200,60)
        with ThreadPoolExecutor(max_workers=1) as pool:
            request=pool.submit(h.claim,t,user,'lost-response')
            h.until(lambda:(gate/'reached').exists(),60)
            before=h.audit(t)
            dc('kill',role)
            try:response=request.result(timeout=10)[0]
            except Exception:response='connection_lost'
        configure();dc('up','-d','--no-deps',role);h.ready()
        status,replay=h.claim(t,user,'lost-response');assert status==202
        final=audit_all(t,1)
        # Multiple delivery copies, deliberately independent of HTTP replay.
        h.enqueue([replay['event']],3)
        h.until(lambda:h.redis('XLEN','fc:outbox')==0,90)
        assert audit_all(t,1)['audit']['issued']==1
        data=dict(point=point,role=role,http_response=response,before=before,final=final,extra_stream_copies=3)
        save(point,data);results.append(data)
    # A successful Lua write survives API death + actual Redis AOF restart.
    point='after_lua';gate=h.LOCAL/'fault-gates'/point
    for f in ('reached','release'):(gate/f).unlink(missing_ok=True)
    t=h.template(1);configure('app',point,hashlib.sha256(f"{t['merchant_id']}/880002/{t['id']}/aof-response-lost".encode()).hexdigest());dc('up','-d','--no-deps','app');h.ready()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(h.claim,t,880002,'aof-response-lost')
        h.until(lambda:(gate/'reached').exists(),60);dc('kill','app');dc('restart','redis')
        try:future.result(timeout=10)
        except Exception:pass
    configure();dc('up','-d','--no-deps','app');h.ready()
    assert h.claim(t,880002,'aof-response-lost')[0]==202
    data=dict(point='redis_after_lua_aof',final=audit_all(t,1));save(data['point'],data);results.append(data)
    # Redis unavailable before Lua: no reservation, no new grant; same request
    # succeeds after Redis returns. The API stays running.
    t=h.template(1);dc('stop','redis')
    status,_=h.claim(t,880003,'redis-before');assert status==503,status
    dc('start','redis');h.ready();assert h.claim(t,880003,'redis-before')[0]==202
    data=dict(point='redis_before_lua',rejected_status=status,final=audit_all(t,1));save(data['point'],data);results.append(data)
    t=h.template(5);dc('stop','relay')
    events=[]
    for u in range(880010,880015):
        status,r=h.claim(t,u);assert status==202;events.append(r['event'])
    before=h.audit(t);assert before['issued']==0 and before['pending']==5
    # Reverse duplicate ordering while original queue entries are still unread.
    h.enqueue(list(reversed(events)),2);dc('start','relay')
    data=dict(point='unread_duplicate_out_of_order',before=before,final=audit_all(t,5));save(data['point'],data);results.append(data)
    # Timeouts are not permanent rejections: hold an actual row lock longer than
    # the 30s callback deadline and release it; no compensation is permitted.
    t=h.template(1)
    query=f"BEGIN; SELECT id FROM templates WHERE merchant_id={t['merchant_id']} AND id='{t['id']}' FOR UPDATE; SELECT GET_LOCK('fc_matrix_timeout',0); SELECT SLEEP(80); ROLLBACK;"
    locker=subprocess.Popen(h.DC+['exec','-T','mysql','mysql','-uroot','-pflashcoupon','control','-e',query],cwd=h.ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    conn=h.until(lambda: int(h.sql("SELECT IS_USED_LOCK('fc_matrix_timeout');").splitlines()[-1]) if h.sql("SELECT IS_USED_LOCK('fc_matrix_timeout');").splitlines()[-1]!='NULL' else None,10)
    assert h.claim(t,880020)[0]==202;time.sleep(33)
    before=h.audit(t);assert before['issued']==0 and before['redis_remaining']==0 and before['failed']==0
    h.sql(f'KILL {conn};');locker.wait(timeout=10)
    data=dict(point='callback_timeout',before=before,final=audit_all(t,1));save(data['point'],data);results.append(data)
    save('summary',dict(passed=True,scenarios=len(results),results=results))
    print('matrix passed',len(results),flush=True)

if __name__=='__main__':
    try:main()
    finally:
        configure();dc('up','-d','--no-deps','app','relay','worker')
