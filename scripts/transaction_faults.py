"""Real transaction producer death/recovery; isolated topic, no HTTP replay to repair."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import support as h

OUT=h.ROOT/'evidence/observability-2026-11-19/transaction'
OVERRIDE=h.LOCAL/'transaction-faults.compose.json'
DC=h.DC+['-f','compose.observability.yaml','-f','compose.transaction.yaml','-f',str(OVERRIDE)]

def dc(*args):return h.run(DC+list(args))

def configure(point='',event_id=''):
    env={'FAULT_INJECTION_ENABLED':'1' if point else '0','FAULT_POINT':point,'FAULT_EVENT_ID':event_id,'FAULT_GATE_DIR':'/fault-gate','TX_TEST_UNKNOWN':'0','TX_TEST_ROLLBACK':'0'}
    service={'environment':env}
    if point:service['volumes']=[str((h.LOCAL/'fault-gates'/point).resolve()).replace('\\','/')+':/fault-gate']
    OVERRIDE.write_text(json.dumps({'services':{'app':service,'worker':{'environment':{'TX_TEST_UNKNOWN':'0','TX_TEST_ROLLBACK':'0'}}}}),encoding='utf-8')

def main():
    OUT.mkdir(parents=True,exist_ok=True);results=[]
    configure();dc('up','-d','--no-deps','app','relay','worker');h.ready()
    for point in ('tx_after_reservation','tx_after_local_commit'):
        print('transaction fault',point,flush=True)
        t=h.template(1,'transaction');user=900001;request_id='producer-crash'
        eid=hashlib.sha256(f"{t['merchant_id']}/{user}/{t['id']}/{request_id}".encode()).hexdigest()
        gate=h.LOCAL/'fault-gates'/point;gate.mkdir(parents=True,exist_ok=True)
        for name in ('reached','release'):(gate/name).unlink(missing_ok=True)
        configure(point,eid);dc('up','-d','--no-deps','app');h.ready()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(h.request,'/v1/claims/transaction',dict(merchant_id=t['merchant_id'],template_id=t['id'],request_id=request_id),user)
            h.until(lambda:(gate/'reached').exists(),60)
            before=h.audit(t)
            sql_before=h.sql(f"SELECT COUNT(*) FROM transaction_events WHERE id='{eid}';").splitlines()[-1]
            dc('kill','app')
            try:future.result(timeout=10)
            except Exception:pass
        configure();dc('up','-d','--no-deps','app');h.ready()
        # Intentionally NO client retry until all independent facts converge.
        final=h.drain(t,1)
        rows=h.sql(f"SELECT COUNT(*) FROM grants WHERE id='{eid}'; SELECT COUNT(*) FROM transaction_events WHERE id='{eid}' AND decision='accepted';").splitlines()
        assert rows[-1]=='1' and rows[-3]=='1',rows
        status,_=h.request('/v1/claims/transaction',dict(merchant_id=t['merchant_id'],template_id=t['id'],request_id=request_id),user)
        assert status==202,status
        result=dict(point=point,before=before,admission_facts_before_crash=int(sql_before),final=final,recovered_without_client_replay=True,post_recovery_replay_status=status,unique_grant_facts=1,unique_admission_facts=1)
        (OUT/(point+'.json')).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');results.append(result)
    (OUT/'crash-summary.json').write_text(json.dumps(dict(passed=True,scenarios=len(results),results=results),indent=2)+'\n',encoding='utf-8')

if __name__=='__main__':
    try:main()
    finally:
        configure();dc('up','-d','--no-deps','app','relay','worker')
