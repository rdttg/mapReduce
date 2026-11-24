import asyncio, os, time, json, uuid, copy
from pathlib import Path
from collections import Counter
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from core import tick, words, split, reduce, accept
ID=int(os.getenv('NODE_ID','1')); PEERS={int(k):v for k,v in json.loads(os.environ['PEERS']).items()}; INTERVAL=float(os.getenv('HEARTBEAT_INTERVAL','2')); TIMEOUT=float(os.getenv('FAILURE_TIMEOUT','6'))
PATH=Path(os.getenv('DATA_DIR','/data'))/'state.json'; PATH.parent.mkdir(parents=True,exist_ok=True)
state=json.loads(PATH.read_text()) if PATH.exists() else {'version':[0,0,0],'jobs':{}}
clock=max(state['version']); epoch=tuple(state['version'][:2]); leader=None; ready=False; seen={}; info={}; events=[]; busy={}; election_lock=asyncio.Lock(); mutation=asyncio.Lock(); client=None; election_requested=True

def log(s):
    global clock
    clock=tick(clock); events.append({'node':ID,'clock':clock,'time':time.time(),'text':s}); del events[:-150]; print(f'[{ID} L{clock}] {s}',flush=True)

def save():
    tmp=PATH.with_suffix('.tmp'); tmp.write_text(json.dumps(state)); tmp.replace(PATH)

def alive(i): return i==ID or time.monotonic()-seen.get(i,-1e9)<TIMEOUT

def status():
    return {'id':ID,'leader':leader,'ready':ready,'epoch':list(epoch),'clock':clock,'busy':list(busy),'version':state['version'],'last_heartbeat':{str(i):round(time.monotonic()-v,1) for i,v in seen.items()}}

class Message(BaseModel):
    message_id:str; message_type:str; sender_id:int; receiver_id:int; lamport_timestamp:int; leader_epoch:list[int]; payload:dict=Field(default_factory=dict)
class Job(BaseModel):
    text:str=Field(min_length=1,max_length=100000); fragments:int=Field(default=6,ge=1,le=100); delay:float=Field(default=0,ge=0,le=60)

async def rpc(i,kind,p=None):
    global clock
    clock=tick(clock); m=dict(message_id=str(uuid.uuid4()),message_type=kind,sender_id=ID,receiver_id=i,lamport_timestamp=clock,leader_epoch=list(epoch),payload=p or {})
    try:
        r=await client.post(PEERS[i]+'/message',json=m); r.raise_for_status(); d=r.json(); clock=tick(clock,d['clock']); return d['payload']
    except (httpx.HTTPError,ValueError,KeyError): return None

def response(p):
    global clock
    clock=tick(clock); return {'clock':clock,'payload':p}

async def replicate():
    global state
    state['version']=[*epoch,state['version'][2]+1]; save(); snapshot=copy.deepcopy(state)
    replies=await asyncio.gather(*(rpc(i,'REPLICATE',snapshot) for i in PEERS if i!=ID))
    return any(r and r.get('ok') for r in replies)

async def elect():
    global leader,ready,epoch,state,election_requested
    if election_lock.locked(): return
    async with election_lock:
        election_requested=False; ready=False; leader=None; log('Eleição Bully iniciada')
        higher=[i for i in PEERS if i>ID]
        replies=await asyncio.gather(*(rpc(i,'ELECTION') for i in higher))
        if any(r and r.get('ok') for r in replies):
            # O superior inicia sua própria eleição; aguardar anúncio.
            for _ in range(30):
                await asyncio.sleep(.2)
                if leader is not None: return
            election_requested=True; return
        snapshots=await asyncio.gather(*(rpc(i,'STATE') for i in PEERS if i!=ID))
        async with mutation:
            candidates=[state]+[r['state'] for r in snapshots if r and 'state' in r]
            state=copy.deepcopy(max(candidates,key=lambda s:s['version']))
            epoch=(max([epoch[0]]+[s['version'][0] for s in candidates]+[r['epoch'][0] for r in snapshots if r and 'epoch' in r])+1,ID)
            leader=ID
            for j in state['jobs'].values():
                for t in j['tasks']:
                    if t['status']=='RUNNING': t.update(status='PENDING',worker=None); log('Tarefa reatribuída após eleição: '+t['id'])
            await asyncio.gather(*(rpc(i,'COORDINATOR',{'leader':ID}) for i in PEERS if i!=ID))
            await replicate(); ready=True; log('Novo líder eleito')

async def worker(t,ep,owner):
    key=t['id']+':'+t['attempt']; busy[key]=t['id']; log('MAP iniciado '+t['id'])
    try:
        await asyncio.sleep(t['delay']); result=dict(Counter(words(t['fragment'])))
        p={'job':t['job'],'id':t['id'],'attempt':t['attempt'],'worker':ID,'result':result}
        if owner==ID: await finish(p,ep)
        else: await rpc(owner,'RESULT',p)
        log('MAP finalizado '+t['id'])
    finally: busy.pop(key,None)

async def finish(p,ep):
    async with mutation:
        if leader!=ID or not ready or tuple(ep)!=epoch: return False
        j=state['jobs'].get(p['job']); t=next((t for t in j['tasks'] if t['id']==p['id']),None) if j else None
        if not t or not accept(t,p): return False
        t['clock']=clock; log('Resultado aceito '+t['id'])
        if all(t['status']=='COMPLETED' for t in j['tasks']): j.update(result=reduce(j['tasks']),finished=time.time()); log('REDUCE concluído '+p['job'])
        await replicate(); return True

@asynccontextmanager
async def lifespan(app):
    global client
    client=httpx.AsyncClient(timeout=.8,trust_env=False); tasks=[asyncio.create_task(heartbeat()),asyncio.create_task(schedule())]
    yield
    for t in tasks:t.cancel()
    await asyncio.gather(*tasks,return_exceptions=True); await client.aclose()
app=FastAPI(lifespan=lifespan)
@app.get('/health')
async def health(): return {'ok':True}
@app.get('/status')
async def get_status(): return status()
@app.get('/state')
async def get_state(): return state
@app.get('/events')
async def get_events(): return events
@app.post('/jobs')
async def submit(j:Job):
    async with mutation:
        if leader!=ID or not ready: raise HTTPException(503,'Eleição em andamento')
        if sum(alive(i) for i in PEERS)<2: raise HTTPException(503,'Sem réplica disponível')
        parts=split(j.text,j.fragments)
        if not parts: raise HTTPException(422,'Texto sem palavras')
        jid=uuid.uuid4().hex[:10]; log('Trabalho recebido '+jid)
        state['jobs'][jid]={'id':jid,'text':j.text,'created':time.time(),'finished':None,'result':{},'tasks':[dict(id=f'{jid}-{k+1}',job=jid,fragment=p,status='PENDING',worker=None,attempt='',tries=0,clock=clock,result={},delay=j.delay) for k,p in enumerate(parts)]}
        if not await replicate(): raise HTTPException(503,'Trabalho não confirmado por réplica; consulte o painel antes de reenviar')
        return {'id':jid}
@app.post('/message')
async def message(m:Message):
    global clock,epoch,leader,ready,state,election_requested
    if m.sender_id not in PEERS or m.receiver_id!=ID: raise HTTPException(400,'Nó inválido')
    clock=tick(clock,m.lamport_timestamp); k=m.message_type; p=m.payload; ep=tuple(m.leader_epoch)
    if k in ('HEARTBEAT','REGISTER'):
        if m.sender_id not in seen or not alive(m.sender_id): log('Nó registrado '+str(m.sender_id))
        seen[m.sender_id]=time.monotonic(); info[m.sender_id]=p
        if p.get('ready') and p.get('leader')==m.sender_id and ep>epoch:
            epoch=ep; leader=m.sender_id; ready=False
        if leader is not None and ID>leader: election_requested=True
        return response(status())
    if k=='ELECTION':
        if ID>m.sender_id:
            if leader!=ID or not ready: election_requested=True
            else: asyncio.create_task(rpc(m.sender_id,'COORDINATOR',{'leader':ID}))
            return response({'ok':True,'type':'OK'})
        return response({'ok':False})
    if k=='COORDINATOR':
        if ep>=epoch and p['leader']==m.sender_id:
            epoch=ep; leader=m.sender_id; ready=False; log('Coordenador atualizado: '+str(leader))
            if ID>leader: election_requested=True
        return response({'ok':True})
    if k=='STATE': return response({'state':state,'epoch':list(epoch)})
    if k=='REPLICATE':
        if ep>=epoch and m.sender_id==ep[1] and p['version']>=state['version']:
            epoch=ep; leader=m.sender_id; ready=False; state=copy.deepcopy(p); save(); return response({'ok':True})
        return response({'ok':False})
    if k=='ASSIGN':
        if ep!=epoch or leader!=m.sender_id: return response({'ok':False})
        key=p['id']+':'+p['attempt']
        if key not in busy:
            busy[key]=p['id']; asyncio.create_task(worker(p,ep,m.sender_id))
        return response({'ok':True})
    if k=='RESULT': return response({'ok':await finish(p,ep)})
    raise HTTPException(400,'Mensagem desconhecida')

async def heartbeat():
    global election_requested,leader,ready,epoch
    first=True; previous=set()
    while True:
        ids=[i for i in PEERS if i!=ID]; replies=await asyncio.gather(*(rpc(i,'REGISTER' if first else 'HEARTBEAT',status()) for i in ids)); first=False
        for i,r in zip(ids,replies):
            if r is not None:
                if not alive(i): log('Nó disponível '+str(i))
                seen[i]=time.monotonic(); info[i]=r
                ep=tuple(r['epoch'])
                if r['ready'] and r['leader']==i and ep>epoch: epoch=ep; leader=i; ready=False
        current={i for i in PEERS if alive(i)}
        for i in previous-current: log('Falha detectada NODE '+str(i))
        if leader is None or not alive(leader) or ID>leader: election_requested=True
        if election_requested and not election_lock.locked(): asyncio.create_task(elect())
        if leader==ID and ready and (current-previous or any(info.get(i,{}).get('version',[])<state['version'] for i in current if i!=ID)):
            async with mutation: await replicate()
        previous=current; await asyncio.sleep(INTERVAL)

async def schedule():
    while True:
        await asyncio.sleep(.4)
        if leader!=ID or not ready: continue
        async with mutation:
            if leader!=ID or not ready: continue
            available=[i for i in PEERS if i!=ID and alive(i)] or [ID]
            load={i:0 for i in available}; changed=False
            for j in state['jobs'].values():
                for t in j['tasks']:
                    if t['status']=='RUNNING':
                        if not alive(t['worker']) or time.time()-t.get('assigned',0)>t['delay']+TIMEOUT+5:
                            t.update(status='PENDING',worker=None); changed=True; log('Tarefa reatribuída '+t['id'])
                        elif t['worker'] in load: load[t['worker']]+=1
            assignments=[]
            for j in state['jobs'].values():
                for t in j['tasks']:
                    if t['status']!='PENDING': continue
                    i=min(load,key=lambda x:(load[x],x))
                    if load[i]>=1: continue
                    log('Tarefa distribuída '+t['id']+' para NODE '+str(i)); t.update(status='RUNNING',worker=i,attempt=uuid.uuid4().hex,tries=t['tries']+1,clock=clock,assigned=time.time()); load[i]+=1; changed=True; assignments.append(copy.deepcopy(t))
            if changed: await replicate()
            for t in assignments:
                if t['worker']==ID: asyncio.create_task(worker(t,epoch,ID))
                else:
                    r=await rpc(t['worker'],'ASSIGN',t)
                    if not r or not r.get('ok'):
                        # Timeout de execução permite reatribuir mesmo se resposta se perder.
                        log('Atribuição não confirmada '+t['id'])
