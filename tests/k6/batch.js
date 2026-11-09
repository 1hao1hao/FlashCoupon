import http from 'k6/http';
import { check, sleep } from 'k6';
const file = open('/reports/whitelist.xlsx', 'b');
const base = __ENV.BASE_URL || 'http://app:8080';
const headers = {'X-Admin-Key':__ENV.ADMIN_KEY||'dev-admin-key'};
export const options={vus:1,iterations:1,thresholds:{checks:['rate==1']}};
export default function(){
 const now=Math.floor(Date.now()/1000);
 const t=http.post(base+'/admin/templates',JSON.stringify({merchant_id:now,name:'k6-batch',stock:1000,per_user:1,threshold:1000,discount:100,starts_at:now-2,ends_at:now+600,valid_until:now+3600,mode:'async'}),{headers:{...headers,'Content-Type':'application/json'}}).json();
 const r=http.post(`${base}/admin/batches/${t.merchant_id}/${t.id}`,{file:http.file(file,'whitelist.xlsx')},{headers});
 check(r,{'batch durable import':v=>v.status===202});if(r.status!==202)throw new Error(r.body);
 const id=r.json('id');let b;
 for(let n=0;n<120;n++){b=http.get(base+'/admin/batches/'+id,{headers}).json();if(b.processed===b.total)break;sleep(1)}
 check(b,{'all whitelist users accepted':v=>v.total===100&&v.accepted===100&&v.rejected===0});
 let a;for(let n=0;n<120;n++){a=http.get(base+`/admin/audit?merchant_id=${t.merchant_id}&template_id=${t.id}`,{headers}).json().find(x=>x.template_id===t.id);if(a.pending===0&&a.issued===100)break;sleep(1)}
 check(a,{'batch inventory consistent':v=>v.consistent&&v.issued===100&&v.projected===100&&v.pending===0});
}
export function handleSummary(data){delete data.setup_data;return {'stdout':JSON.stringify({checks:data.metrics.checks.values}),'/reports/batch.json':JSON.stringify(data,null,2)}}
