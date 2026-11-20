import tempfile
import threading
import unittest
from pathlib import Path
from pipeline import Bar, Cache, Provider, ingest_many, parse_symbols, timestamp

class MockProvider(Provider):
    name = 'mock'
    calls = []
    lock = threading.Lock()
    def max_bars(self): return 2
    def fetch(self, symbol, interval, start, end):
        with self.lock: self.calls.append((symbol, start, end))
        if symbol == 'BAD': raise RuntimeError('Intentional provider failure')
        return [Bar(t,10,11,9,10,100) for t in range(start,end,3600)]

class MultiTickerTests(unittest.TestCase):
    def setUp(self):
        MockProvider.calls = []
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name)/'market.sqlite')
        self.start = timestamp('2026-11-01')
        self.end = self.start + 4*3600
    def tearDown(self): self.tmp.cleanup()
    def test_symbol_parser(self):
        self.assertEqual(parse_symbols(['aapl,msft','AAPL',' nvda ']), ['AAPL','MSFT','NVDA'])
        with self.assertRaises(ValueError): parse_symbols(['AAPL;INVALID'])
    def test_multi_fetch_cache_incremental_export(self):
        csvdir = str(Path(self.tmp.name)/'exports')
        a = ingest_many(MockProvider,self.db,['AAA','BBB'],'1h',self.start,self.end,
                        workers=2,pause=0,csv_dir=csvdir)
        self.assertEqual(set(a['succeeded']), {'AAA','BBB'})
        self.assertFalse(a['failed'])
        self.assertEqual(len(MockProvider.calls),4)
        self.assertTrue((Path(csvdir)/'mock_AAA_1h.csv').exists())
        with_cache = ingest_many(MockProvider,self.db,['AAA','BBB'],'1h',self.start,self.end,
                                 workers=2,pause=0,offline=True)
        self.assertEqual(len(MockProvider.calls),4)
        self.assertEqual(with_cache['succeeded']['AAA']['bars'],4)
        extended = ingest_many(MockProvider,self.db,['AAA','BBB'],'1h',self.start,self.end+3600,
                               workers=2,pause=0)
        self.assertEqual(len(MockProvider.calls),6)
        self.assertEqual(extended['succeeded']['BBB']['bars'],5)
        cache = Cache(self.db)
        try:
            self.assertEqual(len(cache.read('mock','AAA','1h',self.start,self.end+3600)),5)
        finally: cache.close()
    def test_failure_isolation(self):
        result=ingest_many(MockProvider,self.db,['GOOD','BAD'],'1h',self.start,self.end,
                           workers=2,pause=0)
        self.assertIn('GOOD',result['succeeded'])
        self.assertIn('BAD',result['failed'])
        self.assertEqual(result['succeeded']['GOOD']['bars'],4)
    def test_workers_bound(self):
        with self.assertRaises(ValueError):
            ingest_many(MockProvider,self.db,['AAA'],'1h',self.start,self.end,workers=0)

if __name__ == '__main__': unittest.main()
