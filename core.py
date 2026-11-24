import re
from collections import Counter

def tick(local, remote=None):
    return local+1 if remote is None else max(local,remote)+1

def words(text):
    return re.findall(r'\b\w+\b',text.casefold())

def split(text,n):
    w=words(text); n=min(n,len(w)); return [' '.join(w[i*len(w)//n:(i+1)*len(w)//n]) for i in range(n)] if n else []

def reduce(tasks):
    c=Counter()
    for t in tasks:
        if t['status']=='COMPLETED': c.update(t['result'])
    return dict(c)

def accept(t,p):
    if t['status']!='RUNNING' or t['attempt']!=p['attempt'] or t['worker']!=p['worker']: return False
    t.update(status='COMPLETED',result=p['result']); return True
