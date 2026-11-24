from core import tick,words,split,reduce,accept

def test_lamport():assert tick(3,8)==9 and tick(9)==10

def test_map():assert words('Java, JAVA! pão')==['java','java','pão']

def test_split():assert ' '.join(split('a b c d e',3))=='a b c d e'

def test_idempotency():
    t=dict(status='RUNNING',attempt='new',worker=2)
    assert not accept(t,dict(attempt='old',worker=2,result={'a':1}))
    p=dict(attempt='new',worker=2,result={'a':2});assert accept(t,p);assert not accept(t,p);assert reduce([t])=={'a':2}
