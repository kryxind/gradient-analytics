import tempfile
import unittest
from pathlib import Path
from pipeline import Bar,Cache,Provider,ingest,missing,timestamp

class Fake(Provider):
    name='fake'
    def __init__(self): self.calls=[]
    def max_bars(self): return 2
    def fetch(self,symbol,interval,start,end):
        self.calls.append((start,end))
        return [Bar(t,10,11,9,10,100) for t in range(start,end,3600)]

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.cache=Cache(str(Path(self.tmp.name)/'bars.sqlite'))
        self.api=Fake()
    def tearDown(self):
        self.cache.close();self.tmp.cleanup()
    def test_merge_missing(self):
        self.assertEqual(missing(0,100,[(0,20),(30,40),(35,65),(90,100)]),[(20,30),(65,90)])
    def test_incremental_replay_offline(self):
        start=timestamp('2026-01-01'); end=start+5*3600
        a=ingest(self.api,self.cache,'TEST','1h',start,end,pause=0)
        self.assertEqual(a['bars'],5)
        self.assertEqual(len(self.api.calls),3)
        ingest(self.api,self.cache,'TEST','1h',start,end,offline=True)
        self.assertEqual(len(self.api.calls),3)
        ingest(self.api,self.cache,'TEST','1h',start,end+2*3600,pause=0)
        self.assertEqual(self.api.calls[-1],(end,end+2*3600))
        self.assertEqual(len(self.cache.read('fake','TEST','1h',start,end+2*3600)),7)
    def test_partial_coverage_and_refresh(self):
        start=timestamp('2026-01-01')
        ingest(self.api,self.cache,'TEST','1h',start,start+2*3600,pause=0)
        ingest(self.api,self.cache,'TEST','1h',start+4*3600,start+6*3600,pause=0)
        self.assertEqual(missing(start,start+6*3600,self.cache.intervals('fake','TEST','1h')),[(start+2*3600,start+4*3600)])
        ingest(self.api,self.cache,'TEST','1h',start,start+6*3600,pause=0,refresh_last=1)
        self.assertEqual(len(self.cache.read('fake','TEST','1h',start,start+6*3600)),6)
        self.assertEqual(self.api.calls[-1],(start+5*3600,start+6*3600))
    def test_bad_candle_not_cached(self):
        class Bad(Fake):
            def fetch(self,*args):return [Bar(args[2],10,8,9,10,1)]
        with self.assertRaises(RuntimeError):
            ingest(Bad(),self.cache,'TEST','1h',timestamp('2026-01-01'),timestamp('2026-01-01')+3600,pause=0)
        self.assertEqual(self.cache.intervals('fake','TEST','1h'),[])
    def test_offline_missing(self):
        with self.assertRaises(RuntimeError):
            ingest(self.api,self.cache,'TEST','1h',timestamp('2026-01-01'),timestamp('2026-01-02'),offline=True)

if __name__=='__main__':unittest.main()
