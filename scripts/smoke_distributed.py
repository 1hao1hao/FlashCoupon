"""Validate deployed roles + complete Jaeger trace using HTTP and optional SSH."""
import argparse
import base64
import hashlib
import hmac
import json
import time
import urllib.request
from pathlib import Path
from distributed_experiment import Target
import support as h

def main():
    p=argparse.ArgumentParser();p.add_argument('--host');p.add_argument('--user');p.add_argument('--port',type=int,default=22);p.add_argument('--password-env',default='FLASHCOUPON_SSH_PASSWORD');p.add_argument('--directory',default='/home/public/bjh/Flashcoupon');args=p.parse_args()
    t=Target(args);location='linux' if args.host else 'local';out=h.ROOT/'evidence/observability-2026-11-19';test_id=f'final-{location}-{time.time_ns()}'
    admin=t.env.get('ADMIN_KEY',h.ADMIN);secret=t.env.get('USER_KEY',h.SECRET)
    def request(path,body=None,headers=None):
        req=urllib.request.Request(t.base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers or {'X-Admin-Key':admin,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=30) as r:return r.status,json.load(r)
    try:
        t.ready();now=int(time.time())
        _,tpl=request('/admin/templates',dict(merchant_id=time.time_ns()%1000000000000,name=test_id,stock=5,per_user=1,threshold=1000,discount=100,starts_at=now-2,ends_at=now+600,valid_until=now+900,mode='async'))
        trace_id=hashlib.sha256(test_id.encode()).hexdigest()[:32]
        for u in range(990001,990006):
            payload=base64.urlsafe_b64encode(f'{u}:{now+3600}'.encode()).rstrip(b'=')
            token=payload.decode()+'.'+hmac.new(secret.encode(),payload,hashlib.sha256).hexdigest()
            status,_=request('/v1/claims',dict(merchant_id=tpl['merchant_id'],template_id=tpl['id'],request_id=test_id),{'Content-Type':'application/json','Authorization':'Bearer '+token,'traceparent':f'00-{trace_id}-0123456789abcdef-01'})
            assert status==202
        def final():
            _,rows=request(f"/admin/audit?merchant_id={tpl['merchant_id']}&template_id={tpl['id']}");a=rows[0]
            return a if a['issued']==a['projected']==5 and a['pending']==0 and a['consistent'] else None
        audit=h.until(final,120)
        def proof():
            try:d=json.load(urllib.request.urlopen(t.jaeger+'/api/v3/traces/'+trace_id,timeout=5))
            except Exception:return None
            rs=d.get('result',{}).get('resourceSpans',[])
            services={a['value'].get('stringValue') for r in rs for a in r.get('resource',{}).get('attributes',[]) if a['key']=='service.name'}
            spans={s['name'] for r in rs for sc in r.get('scopeSpans',[]) for s in sc.get('spans',[])}
            return d if services>={'flashcoupon-api','flashcoupon-relay','flashcoupon-worker'} and spans>={'sql.commit','coupon.projection','redis.finalize'} else None
        trace=h.until(proof,30)
        ui=urllib.request.urlopen(t.jaeger,timeout=10).status;assert ui==200
        all_rows=request('/admin/audit')[1]
        result=dict(passed=True,location=location,test_id=test_id,accepted=5,audit=audit,jaeger_ui_status=ui,three_process_trace=True,global_templates=len(all_rows),global_unsettled=[a for a in all_rows if not a['consistent'] or a['pending']])
        inspect=['docker','inspect','--format','{{.Image}}',t.project+'-app-1']
        result['runtime_image_id']=(t.shell(' '.join(inspect[:-2])+" '{{.Image}}' "+inspect[-1]) if t.remote else h.run(inspect)).strip()
        assert not result['global_unsettled'],result['global_unsettled']
        (out/(location+'-final-deployment.json')).write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
        (out/(location+'-final-trace.json')).write_text(json.dumps(trace,indent=2)+'\n',encoding='utf-8')
        print('Final deployment/trace/audit passed:',location,flush=True)
    finally:t.close()

if __name__=='__main__':main()
