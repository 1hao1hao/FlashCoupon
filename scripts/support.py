"""Portable stdlib helpers. Requires Python 3.10+ and Docker Compose v2."""
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DC = ['docker', 'compose', '-f', 'compose.yaml', '-f', 'compose.sharding.yaml']
BASE = os.environ.get('BASE_URL', 'http://127.0.0.1:8080')
ADMIN = os.environ.get('ADMIN_KEY', 'dev-admin-key')
SECRET = os.environ.get('USER_KEY', 'dev-user-key')
EVIDENCE = ROOT / 'evidence' / 'engineering-2026-11-09'
LOCAL = ROOT / 'reports'
EVIDENCE.mkdir(parents=True, exist_ok=True)
LOCAL.mkdir(exist_ok=True)

def run(args, *, data=None):
    result = subprocess.run(args, cwd=ROOT, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f'{args[:6]} exit {result.returncode}: {result.stderr.decode(errors="replace")[-1500:]}')
    return result.stdout.decode(errors='replace')

def dc(*args, data=None):
    return run(DC + list(args), data=data)

def token(user):
    payload = base64.urlsafe_b64encode(f'{user}:{int(time.time())+7200}'.encode()).rstrip(b'=')
    return payload.decode() + '.' + hmac.new(SECRET.encode(), payload, hashlib.sha256).hexdigest()

def request(path, body=None, user=None, base=BASE):
    headers = {'X-Admin-Key': ADMIN} if user is None else {'Authorization': 'Bearer ' + token(user)}
    if body is not None:
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(base+path, data=json.dumps(body).encode() if body is not None else None, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            raw=response.read().decode();return response.status, json.loads(raw)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())

def template(stock=100, mode='async', per_user=1):
    now=int(time.time());m=time.time_ns() % 1000000000000
    status,t=request('/admin/templates',dict(merchant_id=m,name='engineering-test',stock=stock,per_user=per_user,threshold=1000,discount=100,starts_at=now-2,ends_at=now+1800,valid_until=now+3600,mode=mode))
    assert status==201,(status,t)
    return t

def audit(t):
    query=urllib.parse.urlencode({'merchant_id':t['merchant_id'],'template_id':t['id']})
    status,rows=request('/admin/audit?'+query);assert status==200,(status,rows);return rows[0]

def until(fn, timeout=300):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            value=fn()
            if value:return value
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(.5)
    raise AssertionError('condition did not converge')

def drain(t,expected):
    def done():
        a=audit(t)
        return a if a['pending']==0 and a['issued']==a['projected']==expected and a['consistent'] else None
    return until(done)

def claim(t,user,request_id='one'):
    return request('/v1/claims'+('/direct' if t['mode']=='direct' else ''),dict(merchant_id=t['merchant_id'],template_id=t['id'],request_id=request_id),user)

def redis(*args):
    return json.loads(dc('exec','-T','redis','redis-cli','--json',*map(str,args)))

def sql(statement):
    return dc('exec','-T','mysql','mysql','-uroot','-pflashcoupon','control',data=statement.encode())

def enqueue(events, repeats=1):
    commands=[]
    for _ in range(repeats):
        for event in events:
            args=[b'XADD',b'fc:outbox',b'*',b'event',json.dumps(event,separators=(',',':')).encode()]
            commands.append(b'*5\r\n'+b''.join(b'$'+str(len(a)).encode()+b'\r\n'+a+b'\r\n' for a in args))
    dc('exec','-T','redis','redis-cli','--pipe',data=b''.join(commands))

def save(name,value):
    (EVIDENCE/name).write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')

def ready():
    for port in (8080,8081,8082):
        until(lambda: request('/health/ready',base=f'http://127.0.0.1:{port}')[0]==200,120)
