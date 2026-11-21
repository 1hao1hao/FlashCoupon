"""Deploy ONLY into the supplied SSH directory; no volume or project deletion.
Optional dependency: pip install paramiko. Password is read from an env variable.
"""
import argparse
import io
import os
from pathlib import Path
import secrets
import shlex
import tarfile
import json
import paramiko

ROOT=Path(__file__).resolve().parents[1]

def connect(args):
    client=paramiko.SSHClient()
    client.load_system_host_keys()
    known=ROOT/'reports'/'ssh_known_hosts'
    if known.exists():client.load_host_keys(str(known))
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(args.host,port=args.port,username=args.user,password=os.environ.get(args.password_env),timeout=15,auth_timeout=15)
    client.get_transport().set_keepalive(15)
    known.parent.mkdir(exist_ok=True);client.save_host_keys(str(known))
    return client

def command(client,cmd):
    _,stdout,stderr=client.exec_command(cmd,timeout=1800)
    out=stdout.read().decode(errors='replace');err=stderr.read().decode(errors='replace')
    status=stdout.channel.recv_exit_status()
    if status:raise RuntimeError(f'remote exit {status}: {err[-1200:]}')
    return out

def main():
    p=argparse.ArgumentParser();p.add_argument('--host',required=True);p.add_argument('--user',required=True)
    p.add_argument('--port',type=int,default=22);p.add_argument('--directory',required=True)
    p.add_argument('--password-env',default='FLASHCOUPON_SSH_PASSWORD');p.add_argument('--upload-only',action='store_true')
    p.add_argument('--image-archive',help='Optional docker save TAR; import matching images without registry access')
    args=p.parse_args()
    if not args.directory.startswith('/home/') or '..' in Path(args.directory).parts:raise ValueError('explicit /home/... directory required')
    client=connect(args);q=shlex.quote(args.directory)
    try:
        print(command(client,'uname -m; docker compose version'),flush=True)
        command(client,f'mkdir -p {q}')
        bundle=io.BytesIO()
        with tarfile.open(fileobj=bundle,mode='w:gz') as tar:
            for rel in ['README.md','.env.example','go.mod','go.sum','Dockerfile','compose.yaml','compose.sharding.yaml','compose.observability.yaml','compose.transaction.yaml','compose.redis-ha.yaml','cmd','internal','third_party','deploy','scripts','tests','docs']:
                tar.add(ROOT/rel,arcname=rel,filter=lambda t: None if '__pycache__' in t.name else t)
        sftp=client.open_sftp()
        if args.image_archive:
            sftp.put(str(Path(args.image_archive).resolve()),args.directory+'/images.tar')
        with sftp.file(args.directory+'/source.tgz','wb') as f:f.write(bundle.getvalue())
        # Keep first generated secrets across re-deployments. Never print them.
        try:sftp.stat(args.directory+'/.env')
        except FileNotFoundError:
            with sftp.file(args.directory+'/.env','w') as f:f.write('ADMIN_KEY='+secrets.token_hex(24)+'\nUSER_KEY='+secrets.token_hex(24)+'\nTRACE_SAMPLE_RATIO=1\n')
            sftp.chmod(args.directory+'/.env',0o600)
        override={"name":"flashcoupon-observe","services":{
            'app':{'ports':['127.0.0.1:18080:8080']},'relay':{'ports':['127.0.0.1:18081:8080']},'worker':{'ports':['127.0.0.1:18082:8080']},
            'mysql':{'ports':[]},'redis':{'ports':[]},'proxy':{'ports':[]},'jaeger':{'ports':['127.0.0.1:18486:16686']}
        }}
        # Compose !override prevents additive host port publication from base files.
        text='name: flashcoupon-observe\nservices:\n'
        for name,cfg in override['services'].items():
            text+=f'  {name}:\n    ports: !override '+json.dumps(cfg['ports'])+'\n'
            if name=='proxy':text+='    image: flashcoupon-proxy:latest\n'
        with sftp.file(args.directory+'/compose.remote.yaml','w') as f:f.write(text)
        sftp.close()
        command(client,f'cd {q} && tar -xzf source.tgz')
        if args.image_archive:
            print(command(client,f'cd {q} && docker load -i images.tar'),flush=True)
        if not args.upload_only:
            dc='docker compose -p flashcoupon-observe -f compose.yaml -f compose.sharding.yaml -f compose.observability.yaml -f compose.remote.yaml'
            build_flag='--no-build --pull never' if args.image_archive else '--build'
            print(command(client,f'cd {q} && {dc} up -d {build_flag} app relay worker jaeger && {dc} ps'),flush=True)
        print('Remote source deployed without deleting volumes',flush=True)
    finally:client.close()

if __name__=='__main__':main()
