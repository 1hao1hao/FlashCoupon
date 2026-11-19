"""Repeatable new regression checks; full legacy faults: verify.py --faults."""
import json
import subprocess
import time
import urllib.request
import support as h

OUT=h.ROOT/'evidence/observability-2026-11-19'
DC=h.DC+['-f','compose.observability.yaml','-f','compose.transaction.yaml']

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    commands=[['go','test','-count=1','./...'],['go','test','-race','-count=1','./...'],['go','vet','./...']]
    results=[]
    for i,cmd in enumerate(commands):
        with (h.LOCAL/f'observability-check-{i}.log').open('wb') as f:
            r=subprocess.run(h.DC+['run','--rm','-e','INTEGRATION=0','toolbox']+cmd,cwd=h.ROOT,stdout=f,stderr=subprocess.STDOUT)
        assert r.returncode==0,cmd
        results.append(dict(command=' '.join(cmd),exit_code=r.returncode))
    h.run(DC+['stop','app','relay','worker'])
    try:
        with (h.LOCAL/'transaction-integration-final.log').open('wb') as f:
            r=subprocess.run(DC+['run','--rm','-e','ROLE=all','-e','TX_EXPERIMENT_ENABLED=1','toolbox','go','test','-race','-count=1','-v','./internal/app','-run','TestTransactionBrokerIntegration'],cwd=h.ROOT,stdout=f,stderr=subprocess.STDOUT)
        assert r.returncode==0,'real transaction test'
        lines=(h.LOCAL/'transaction-integration-final.log').read_text(errors='replace').splitlines()
        (OUT/'transaction'/'broker-integration.json').write_text(json.dumps(dict(passed=True,exit_code=r.returncode,race=True,tests=[x.strip() for x in lines if '--- PASS:' in x]),indent=2)+'\n',encoding='utf-8')
    finally:h.run(DC+['up','-d','--no-deps','app','relay','worker'])
    h.ready()
    # Real collector loss must not make business admission depend on telemetry.
    h.run(DC+['stop','jaeger'])
    try:
        t=h.template(5)
        for u in range(930001,930006):assert h.claim(t,u)[0]==202
        audit=h.drain(t,5)
    finally:h.run(DC+['start','jaeger'])
    (OUT/'checks.json').write_text(json.dumps(dict(passed=True,commands=results,collector_unavailable=dict(accepted=5,audit=audit)),indent=2)+'\n',encoding='utf-8')
    print('Observability regressions and real transaction integration passed',flush=True)

if __name__=='__main__':main()
