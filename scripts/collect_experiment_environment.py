"""Record hardware/runtime identity and input hashes, never credentials or env."""
import argparse
import hashlib
import json
import platform
import subprocess
from pathlib import Path
import remote_deploy as remote

ROOT=Path(__file__).resolve().parents[1]

def main():
    p=argparse.ArgumentParser();p.add_argument('--host');p.add_argument('--user');p.add_argument('--port',type=int,default=22);p.add_argument('--password-env',default='FLASHCOUPON_SSH_PASSWORD');args=p.parse_args()
    if args.host:
        c=remote.connect(args)
        try:
            hardware=remote.command(c,"uname -srmo; getconf _NPROCESSORS_ONLN; free -b | head -n 2; docker version --format '{{.Server.Version}}'; docker images --format '{{.Repository}}:{{.Tag}} {{.ID}}' | grep -E '^(flashcoupon-runtime|flashcoupon-proxy|mysql:8.0.41|redis:7-alpine|apache/rocketmq:5.3.1|jaegertracing/jaeger:2.21.0)' ")
        finally:c.close()
        location='linux-server'
    else:
        hardware=platform.platform()+'\n'+subprocess.check_output(['docker','info','--format','CPUs={{.NCPU}} memory={{.MemTotal}} server={{.ServerVersion}} OS={{.OperatingSystem}}']).decode()+subprocess.check_output(['reports/tools/k6.exe','version']).decode();location='windows-docker-desktop'
    paths=['compose.yaml','compose.sharding.yaml','compose.observability.yaml','compose.transaction.yaml','tests/k6/claims.js','scripts/distributed_experiment.py','scripts/server_collect.py','scripts/remote_client.py']
    result=dict(location=location,hardware=hardware,input_sha256={x:hashlib.sha256((ROOT/x).read_bytes()).hexdigest() for x in paths},load=dict(requests=500,vus=30,stock=500,per_user=1,rounds=3),notes='Input hashes describe final reproducible harness; initial normal/locked trials preceded diagnostic logging and teardownTimeout extension, without changing successful request semantics.')
    target=ROOT/'evidence/observability-2026-11-19'/(location+'-environment.json');target.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')

if __name__=='__main__':main()
