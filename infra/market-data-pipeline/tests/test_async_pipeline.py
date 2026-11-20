import asyncio
import tempfile
import unittest
from pathlib import Path

from async_pipeline import AsyncProvider, ingest_batch, validate_bars
from pipeline import Bar, Cache, timestamp


class MockAsyncProvider(AsyncProvider):
    name = 'mockasync'
    def __init__(self): self.calls = []
    def max_bars(self): return 2
    async def fetch(self, session, symbol, interval, start, end):
        self.calls.append((symbol,start,end))
        await asyncio.sleep(0)
        if symbol=='BAD': raise RuntimeError('intentional error')
        return [Bar(ts,10,11,9,10,100) for ts in range(start,end,3600)]


class AsyncPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name)/'cache.sqlite')
        self.start = timestamp('2026-11-01')
        self.end = self.start + 4*3600
        self.provider = MockAsyncProvider()

    async def asyncTearDown(self): self.tmp.cleanup()

    async def test_batch_incremental_and_failure(self):
        result = await ingest_batch(self.provider,self.db,['AAA','BBB','BAD'],'1h',self.start,self.end,workers=2)
        self.assertEqual(set(result['succeeded']), {'AAA','BBB'})
        self.assertIn('BAD',result['failed'])
        self.assertEqual(len([x for x in self.provider.calls if x[0]!='BAD']),4)
        count = len(self.provider.calls)
        result = await ingest_batch(self.provider,self.db,['AAA','BBB'],'1h',self.start,self.end,workers=2,offline=True)
        self.assertFalse(result['failed'])
        self.assertEqual(len(self.provider.calls),count)
        await ingest_batch(self.provider,self.db,['AAA','BBB'],'1h',self.start,self.end+3600,workers=2)
        self.assertEqual(len(self.provider.calls),count+2)
        cache = Cache(self.db)
        try:
            self.assertEqual(len(cache.read('mockasync','AAA','1h',self.start,self.end+3600)),5)
        finally: cache.close()

    async def test_invalid_ohlc(self):
        with self.assertRaises(ValueError):
            validate_bars([Bar(self.start,10,9,8,10,3)],self.start,self.end,3600)

    async def test_refresh_last(self):
        await ingest_batch(self.provider,self.db,['AAA'],'1h',self.start,self.end,workers=1)
        count=len(self.provider.calls)
        await ingest_batch(self.provider,self.db,['AAA'],'1h',self.start,self.end,workers=1,refresh_last=1)
        self.assertEqual(len(self.provider.calls),count+1)


if __name__=='__main__': unittest.main()
