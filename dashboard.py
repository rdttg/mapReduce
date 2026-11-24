import os,json,asyncio
import httpx
from fastapi import FastAPI,HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from node_types import Job
app=FastAPI(); peers={int(k):v for k,v in json.loads(os.environ['PEERS']).items()}
app.mount('/static',StaticFiles(directory='static'),name='static')
@app.get('/')
async def home():return FileResponse('static/index.html')
async def get(i,path):
    try:
        async with httpx.AsyncClient(timeout=1,trust_env=False) as c:return (await c.get(peers[i]+path)).json()
    except (httpx.HTTPError,ValueError):return None
async def discover():
    rows=await asyncio.gather(*(get(i,'/status') for i in peers)); nodes=[dict(r or {'id':i},online=r is not None) for i,r in zip(peers,rows)]; leaders=[n for n in nodes if n.get('ready') and n.get('leader')==n['id']]; return nodes,max(leaders,key=lambda n:n['epoch']) if leaders else None
@app.get('/api/status')
async def status():
    nodes,l=await discover(); st=await get(l['id'],'/state') if l else None; logs=await asyncio.gather(*(get(i,'/events') for i in peers)); return {'nodes':nodes,'leader':l['id'] if l else None,'jobs':list((st or {}).get('jobs',{}).values()),'events':sorted([e for es in logs if es for e in es],key=lambda e:e['time'])[-80:]}
@app.post('/api/jobs')
async def submit(j:Job):
    _,l=await discover()
    if not l:raise HTTPException(503,'Aguardando eleição')
    try:
        async with httpx.AsyncClient(timeout=8,trust_env=False) as c:
            r=await c.post(peers[l['id']]+'/jobs',json=j.model_dump() if hasattr(j,'model_dump') else j.dict());
            if r.status_code>=400:raise HTTPException(r.status_code,r.json().get('detail'))
            return r.json()
    except httpx.HTTPError:raise HTTPException(503,'Líder indisponível')
