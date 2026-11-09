"""Portable integration entrypoint. No daemon/volume data is removed."""
import argparse
from support import *

def main():
    p=argparse.ArgumentParser();p.add_argument('--faults',action='store_true');p.add_argument('--benchmark',action='store_true');args=p.parse_args()
    dc('up','-d','--build','mysql','redis','broker','proxy','mq-topic')
    dc('stop','app','relay','worker')
    checks='go test -count=1 ./... && go test -race -count=1 -v ./... && go vet ./... && go test -race -count=1 github.com/apache/rocketmq-client-go/v2/consumer -run TestSubscriptionVersionCopyOnWrite && sh scripts/sdk-counterfactual.sh && go run ./cmd/whitelist'
    try:
        with (LOCAL/'integration-portable.log').open('wb') as log:
            result=subprocess.run(DC+['run','--rm','-e','ROLE=all','toolbox','sh','-c',checks],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
        if result.returncode:raise RuntimeError('Integration failed; see reports/integration-portable.log')
        text=(LOCAL/'integration-portable.log').read_text(errors='replace')
        save('tests.json',{'passed':True,'commands':checks.split(' && '),'test_cases':[line.strip() for line in text.splitlines() if '--- PASS:' in line],'race_detected':False})
        counterfactual=(LOCAL/'sdk-counterfactual.log').read_text(errors='replace')
        save('sdk-regression.json',dict(original_immutable_assertion_failed='published subscription object was mutated' in counterfactual,original_race_detected='WARNING: DATA RACE' in counterfactual,patched_regression_passed=True,patch_source='third_party/ROCKETMQ_PATCH.md'))
    finally:dc('up','-d','--build','app','relay','worker')
    ready()
    health={}
    for role,port in (('api',8080),('relay',8081),('worker',8082)):
        health[role]=request('/health/ready',base=f'http://localhost:{port}')[1]
        status=urllib.request.urlopen(f'http://localhost:{port}/metrics').status;assert status==200
    save('deployment.json',{'processes':health,'metrics_http_status':200,'topology':'single MySQL instance; two logical databases; four user-coupon tables'})
    if args.faults:run([os.sys.executable,str(ROOT/'scripts/faults.py')])
    if args.benchmark:run([os.sys.executable,str(ROOT/'scripts/benchmark.py')])
    status,audits=request('/admin/audit');assert status==200
    assert all(a['consistent'] and a['pending']==0 for a in audits)
    save('final-audit.json',dict(templates=len(audits),inconsistent=0,pending=0,passed=True))
    print('Verification passed. Sanitized evidence:',EVIDENCE)

if __name__=='__main__':main()
