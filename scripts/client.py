"""Bounded dependency-free client; credentials stay in private file references."""
import json
import os
from pathlib import Path
import stat
import urllib.error
import urllib.parse
import urllib.request

class Unavailable(Exception): pass

def private_text(path, limit=65536):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd) as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
            raise ValueError('owner-private regular file required')
        value=stream.read(limit+1)
        if len(value)>limit: raise ValueError('private record exceeds limit')
        return value

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None

class Client:
    def __init__(self,base_url,token_file):
        parsed=urllib.parse.urlsplit(base_url)
        if parsed.scheme!='http' or parsed.hostname not in ('127.0.0.1','localhost','::1') or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('','/'):
            raise ValueError('local client requires a configured loopback HTTP endpoint')
        self.base=base_url.rstrip('/'); self.token_file=token_file
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    def call(self,method,path,value=None):
        token=private_text(self.token_file,1024).strip()
        request=urllib.request.Request(self.base+path,method=method,data=json.dumps(value).encode() if value is not None else None,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        try:
            with self.opener.open(request,timeout=5) as response:
                raw=response.read(4*1024*1024+1)
                if len(raw)>4*1024*1024: raise Unavailable()
                return json.loads(raw)
        except (OSError,ValueError,urllib.error.HTTPError):
            raise Unavailable('service operation unconfirmed') from None
