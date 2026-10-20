import hashlib,json,tomllib,urllib.error,urllib.request
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args):return None
cfg=tomllib.loads((Path.home()/'Library/Preferences/.wrangler/config/default.toml').read_text())
req=urllib.request.Request('https://api.cloudflare.com/client/v4/accounts/8df0426c80fcd1a04ed439f441db9ff8/workers/scripts/blockzilla-foundation-watch/content/v2',headers={'Authorization':'Bearer '+cfg['oauth_token'],'User-Agent':'Blockzilla-Operations/1.0'})
try:
 with urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect()).open(req,timeout=20) as r:status=r.status;ctype=r.headers.get('Content-Type','');raw=r.read(2*1024**2+1)
except urllib.error.HTTPError as e:print(json.dumps({'http_status':e.code}));raise SystemExit(1)
assert len(raw)<=2*1024**2
out=Path('hivezilla/deploy/foundation-watch/evidence/incident-20261018-recurrence/evening-20261018/worker-after');out.mkdir(exist_ok=False)
msg=BytesParser(policy=default).parsebytes(('Content-Type: '+ctype+'\r\nMIME-Version: 1.0\r\n\r\n').encode()+raw)
parts=[]
if msg.is_multipart():
 for i,p in enumerate(msg.iter_parts()):
  name=p.get_filename() or p.get_param('name',header='content-disposition') or ('part-'+str(i));assert Path(name).name==name
  body=p.get_payload(decode=True);assert isinstance(body,bytes);f=out/name;f.write_bytes(body);f.chmod(0o400);parts.append({'file':name,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()})
else:
 f=out/'index.js';f.write_bytes(raw);f.chmod(0o400);parts.append({'file':'index.js','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
manifest={'schema':'blockzilla-monitor-deployed-content-v1','http_status':status,'content_type':ctype,'parts':parts};(out/'MANIFEST.json').write_text(json.dumps(manifest,indent=2)+'\n');print(json.dumps(manifest))
