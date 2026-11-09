import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

const base = __ENV.BASE_URL || 'http://localhost:8080';
const admin = __ENV.ADMIN_KEY || 'dev-admin-key';
const secret = __ENV.USER_KEY || 'dev-user-key';
const mode = __ENV.MODE || 'async';
const count = Number(__ENV.ITERATIONS || 500);
const duplicate = __ENV.DUPLICATE === '1';
const accepted = new Counter('accepted_claims');
const rejected = new Counter('business_rejected');
const limited = new Counter('limited_claims');
const unexpected = new Counter('unexpected_claim_errors');
const finalTPS = new Trend('final_issue_tps');
const drainSeconds = new Trend('drain_seconds');
const acceptanceWindow = new Trend('acceptance_window_seconds');
const latency = new Trend('claim_latency', true);
http.setResponseCallback(http.expectedStatuses(200,201,202,409,429));
export const options = {
  scenarios: { claims: __ENV.CONTENTION==='1' ? {executor:'per-vu-iterations',vus:Number(__ENV.VUS),iterations:1,maxDuration:'5m'} : { executor: 'shared-iterations', vus: Number(__ENV.VUS || 30), iterations: count, maxDuration: '3m' } },
  discardResponseBodies: true,
  summaryTrendStats: ['avg','min','med','max','p(95)','p(99)'],
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'], claim_latency: [`p(99)<${Number(__ENV.LATENCY_BUDGET_MS||10000)}`] },
};
function token(u) {
  const payload = encoding.b64encode(`${u}:${Math.floor(Date.now()/1000)+3600}`, 'rawurl');
  return payload + '.' + crypto.hmac('sha256', secret, payload, 'hex');
}
export function setup() {
  const now = Math.floor(Date.now()/1000);
  const stock = Number(__ENV.STOCK || count);
  const merchant = now * 1000 + Math.floor(Math.random()*999);
  const res = http.post(base+'/admin/templates', JSON.stringify({merchant_id:merchant,name:'k6-'+mode,stock,per_user:1,threshold:1000,discount:100,starts_at:now-2,ends_at:now+1800,valid_until:now+3600,mode}),{headers:{'Content-Type':'application/json','X-Admin-Key':admin},responseType:'text'});
  if(res.status!==201) throw new Error('create failed: '+res.body);
  return { template:res.json(), start:Date.now(), userBase:now*10000 };
}
export default function(data) {
  const n = exec.scenario.iterationInTest;
  const user = data.userBase + (duplicate ? n % Number(__ENV.UNIQUE_USERS || 10) : n) + 1;
  const res = http.post(base+'/v1/claims'+(mode==='direct'?'/direct':''),JSON.stringify({merchant_id:data.template.merchant_id,template_id:data.template.id,request_id:duplicate?'same-request':'req-'+n}),{headers:{'Content-Type':'application/json',Authorization:'Bearer '+token(user)},tags:{operation:'claim'}});
  latency.add(res.timings.duration);
  if(res.status===200||res.status===202) accepted.add(1);else if(res.status===409) rejected.add(1);else if(res.status===429) limited.add(1);else unexpected.add(1);
  check(res,{'claim accepted or business rejected':r=>[200,202,409,429].includes(r.status)});
}
export function teardown(data) {
  acceptanceWindow.add((Date.now()-data.start)/1000);
  const start = Date.now();let row;
  for(let i=0;i<300;i++) {
    const res=http.get(base+`/admin/audit?merchant_id=${data.template.merchant_id}&template_id=${data.template.id}`,{headers:{'X-Admin-Key':admin},responseType:'text'});
    if(res.status!==200) throw new Error('audit failed '+res.body);
    row=res.json().find(r=>r.template_id===data.template.id&&r.merchant_id===data.template.merchant_id);
    if(row&&row.pending===0&&row.issued===row.projected) break;
    sleep(1);
  }
  const elapsed=(Date.now()-data.start)/1000;
  const expected = Math.min(data.template.stock, duplicate ? Number(__ENV.UNIQUE_USERS || 10) : count);
  check(row,{'final inventory consistent':r=>r&&r.consistent&&r.pending===0,'expected final unique issuance':r=>r&&r.issued===expected});
  finalTPS.add(row.issued/elapsed);drainSeconds.add((Date.now()-start)/1000);
  console.log(JSON.stringify({mode,duplicate,template_id:data.template.id,elapsed_seconds:elapsed,final_issue_tps:row.issued/elapsed,audit:row}));
}
export function handleSummary(data) {
  data.measurements={accepted_http_qps:(data.metrics.accepted_claims?.values.count||0)/data.metrics.acceptance_window_seconds.values.avg,final_issue_tps:data.metrics.final_issue_tps.values.avg};
  data.configuration={mode,iterations:count,vus:Number(__ENV.VUS||30),stock:Number(__ENV.STOCK||count),duplicate,contention:__ENV.CONTENTION==='1',latency_budget_ms:Number(__ENV.LATENCY_BUDGET_MS||10000)};
  data.test_template={id:data.setup_data.template.id,merchant_id:data.setup_data.template.merchant_id};delete data.setup_data;
  return {'stdout':JSON.stringify(data.measurements), ['/reports/'+(__ENV.REPORT_NAME||mode+(duplicate?'-duplicate':''))+'.json']:JSON.stringify(data,null,2)};
}
