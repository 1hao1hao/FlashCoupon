// Reuse template creation, exact final issuance/inventory assertions and report schema.
// Fault responses are classified, never counted as successful admission.
import http from 'k6/http';
import {check,sleep} from 'k6';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import exec from 'k6/execution';
import {Counter,Trend} from 'k6/metrics';
import {setup as baseSetup,teardown as baseTeardown,handleSummary as baseSummary,options as baseOptions} from './claims.js';

const accepted=new Counter('ha_initial_accepted');
const unknown=new Counter('ha_initial_unknown');
const refused=new Counter('ha_initial_safe_rejected');
const latency=new Trend('ha_initial_latency',true);
const window=new Trend('ha_initial_window_seconds');
const retries=new Counter('ha_replay_requests');
const base=__ENV.BASE_URL,secret=__ENV.USER_KEY;
const pacing=Number(__ENV.HA_PACING_SECONDS||.1);
export const options={...baseOptions,thresholds:{checks:['rate==1']}};
export const setup=baseSetup;
function request(data,n){
 const u=data.userBase+n+1,payload=encoding.b64encode(`${u}:${Math.floor(Date.now()/1000)+3600}`,'rawurl');
 const tid=crypto.sha256(`${__ENV.TEST_ID}/${n}`,'hex').slice(0,32);
 return {method:'POST',url:base+'/v1/claims',body:JSON.stringify({merchant_id:data.template.merchant_id,template_id:data.template.id,request_id:'req-'+n}),params:{headers:{'Content-Type':'application/json',Authorization:'Bearer '+payload+'.'+crypto.hmac('sha256',secret,payload,'hex'),traceparent:`00-${tid}-0123456789abcdef-01`},responseType:'text',timeout:'8s'}};
}
export default function(data){
 const req=request(data,exec.scenario.iterationInTest),r=http.post(req.url,req.body,req.params);
 latency.add(r.timings.duration);
 if(r.status===202)accepted.add(1);
 else if(r.status===503){let outcome;try{outcome=r.json('outcome')}catch(_){};if(outcome==='safe_rejected')refused.add(1);else unknown.add(1)}
 else unknown.add(1);
 check(r,{'initial response classified; not issuance success':v=>[0,202,503].includes(v.status)});
 sleep(pacing);
}
export function teardown(data){
 window.add((Date.now()-data.start)/1000);
 let pending=Array.from({length:Number(__ENV.ITERATIONS||500)},(_,i)=>i);
 const end=Date.now()+150000;
 while(pending.length&&Date.now()<end){
  const next=[];
  for(let i=0;i<pending.length;i+=30){
   const ids=pending.slice(i,i+30),rs=http.batch(ids.map(n=>request(data,n)));retries.add(ids.length);
   rs.forEach((r,k)=>{if(r.status!==202)next.push(ids[k])});
  }
  pending=next;if(pending.length)sleep(.5);
 }
 check(pending,{'all identical request IDs resolved as accepted':v=>v.length===0});
 baseTeardown(data); // Enforces issued=stock, unique issuance, projection and inventory.
}
export function handleSummary(data){
 const output=baseSummary(data);
 const path=Object.keys(output).find(k=>k!=='stdout');const report=JSON.parse(output[path]);
 report.measurements.accepted_http_qps=(data.metrics.ha_initial_accepted?.values.count||0)/data.metrics.ha_initial_window_seconds.values.avg;
 report.measurements.initial_accepted=data.metrics.ha_initial_accepted?.values.count||0;
 report.measurements.initial_unknown=data.metrics.ha_initial_unknown?.values.count||0;
 report.measurements.initial_safe_rejected=data.metrics.ha_initial_safe_rejected?.values.count||0;
 report.configuration.fault_aware=true;report.configuration.pacing_seconds=pacing;output[path]=JSON.stringify(report,null,2);output.stdout=JSON.stringify(report.measurements);return output;
}
