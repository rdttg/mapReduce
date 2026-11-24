"""Testes com três servidores HTTP independentes, sem Docker."""
import os,sys,time,tempfile,subprocess,json
from pathlib import Path
import httpx
base=Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory() as tmp:
    peers={str(i):f'http://127.0.0.1:{18700+i}' for i in range(1,4)}; procs={}; logs=[]
    def start(i):
        f=open(Path(tmp)/f'{i}.log','a');logs.append(f);procs[i]=subprocess.Popen([sys.executable,'-m','uvicorn','node:app','--port',str(18700+i)],cwd=base,env=dict(os.environ,NODE_ID=str(i),PEERS=json.dumps(peers),DATA_DIR=str(Path(tmp)/str(i)),HEARTBEAT_INTERVAL='.5',FAILURE_TIMEOUT='2'),stdout=f,stderr=f)
    def stop(i):procs[i].terminate();procs[i].wait(timeout=5)
    def get(i,path):return httpx.get(peers[str(i)]+path,timeout=1).json()
    def wait(fn,seconds=25):
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            try:
                v=fn()
                if v:return v
            except (httpx.HTTPError,KeyError):pass
            time.sleep(.2)
        raise AssertionError('timeout')
    def elected(i):return get(i,'/status')['ready'] and get(i,'/status')['leader']==i
    def job(i,delay):
        r=httpx.post(peers[str(i)]+'/jobs',json={'text':'Java Python Docker Java Python Java '*4,'fragments':4,'delay':delay},timeout=5);r.raise_for_status();return r.json()['id']
    def state(i,j):return get(i,'/state')['jobs'][j]
    try:
        for i in range(1,4):start(i)
        wait(lambda:elected(3));wait(lambda:get(1,'/status')['leader']==3 and get(2,'/status')['leader']==3);print('PASS election',flush=True)
        j=job(3,0);wait(lambda:state(3,j)['finished']);assert state(3,j)['result']=={'java':12,'python':8,'docker':4};print('PASS map/reduce',flush=True)
        j=job(3,2);wait(lambda:any(t['worker']==1 and t['status']=='RUNNING' for t in state(3,j)['tasks']));stop(1);wait(lambda:state(3,j)['finished']);assert state(3,j)['result']=={'java':12,'python':8,'docker':4};assert any(t['tries']>1 for t in state(3,j)['tasks']);print('PASS worker failure',flush=True)
        start(1);wait(lambda:j in get(1,'/state')['jobs']);print('PASS worker rejoin',flush=True)
        j=job(3,2);wait(lambda:any(t['status']=='RUNNING' for t in state(3,j)['tasks']));stop(3);wait(lambda:elected(2));wait(lambda:state(2,j)['finished']);assert state(2,j)['result']=={'java':12,'python':8,'docker':4};print('PASS leader failure and recovery',flush=True)
        start(3);wait(lambda:elected(3));assert state(3,j)['result']=={'java':12,'python':8,'docker':4};print('PASS former leader rejoin',flush=True)
    except Exception:
        for i in procs:print('LOG',i,(Path(tmp)/f'{i}.log').read_text()[-3000:])
        raise
    finally:
        for i,p in procs.items():
            if p.poll() is None:stop(i)
        for f in logs:f.close()
