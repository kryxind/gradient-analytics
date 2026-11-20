"""Async HTTP market-data ingestion with SQLite cache, optional Parquet/DuckDB export,
and a foreground polling scheduler. Run `python async_pipeline.py --help`.
"""
from __future__ import annotations
import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import random
import sys
import ssl
import time
from pathlib import Path
from urllib.parse import quote

from pipeline import Bar, Cache, INTERVALS, iso, merge, missing, parse_symbols, timestamp, create_ssl_context

LOG = logging.getLogger('marketdata.async')


async def fetch_json(session, url, *, headers=None, params=None, retries=5):
    import aiohttp
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status in (429, 500, 502, 503, 504):
                    if attempt == retries - 1:
                        raise RuntimeError(f'HTTP {response.status} after {retries} tries: {url}')
                    delay = min(30.0, 2**attempt + random.random())
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try: delay = min(60.0, max(0.1, float(retry_after)))
                        except ValueError: pass
                    LOG.warning('RETRY status=%d attempt=%d/%d delay=%.1fs url=%s', response.status, attempt+1, retries, delay, url)
                else:
                    if response.status >= 400:
                        msg = (await response.text())[:200]
                        raise RuntimeError(f'HTTP {response.status}: {url}: {msg}')
                    return await response.json()
        except (aiohttp.ClientSSLError, ssl.SSLError) as exc:
            raise RuntimeError(f'TLS failure (not retried): {url}: {exc}') from exc
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as exc:
            if attempt == retries - 1:
                raise RuntimeError(f'Network failure: {url}: {exc}') from exc
            delay = min(30.0, 2**attempt + random.random())
            LOG.warning('RETRY network attempt=%d/%d delay=%.1fs url=%s error=%s',attempt+1,retries,delay,url,exc)
        await asyncio.sleep(delay)
    raise RuntimeError('Retry loop exhausted')


class AsyncProvider:
    name: str
    def max_bars(self): return 250
    async def fetch(self, session, symbol: str, interval: str, start: int, end: int) -> list[Bar]:
        raise NotImplementedError


class AsyncCoinbase(AsyncProvider):
    name = 'coinbase'
    # Coinbase Exchange candles supports precisely these granularities.
    allowed = {60, 300, 900, 3600, 21600, 86400}
    async def fetch(self, session, symbol, interval, start, end):
        if INTERVALS[interval] not in self.allowed:
            raise ValueError('Unsupported Coinbase granularity')
        data = await fetch_json(session, f'https://api.exchange.coinbase.com/products/{quote(symbol.upper(),safe="-")}/candles',
                                params={'start':iso(start),'end':iso(end),'granularity':INTERVALS[interval]})
        if not isinstance(data,list): raise RuntimeError(f'Unexpected Coinbase response: {str(data)[:250]}')
        return [Bar(int(r[0]),float(r[3]),float(r[2]),float(r[1]),float(r[4]),float(r[5])) for r in data]


class AsyncAlpaca(AsyncProvider):
    name = 'alpaca'
    frames = {'1m':'1Min','5m':'5Min','15m':'15Min','1h':'1Hour','1d':'1Day'}
    def __init__(self):
        self.key = os.getenv('ALPACA_API_KEY')
        self.secret = os.getenv('ALPACA_SECRET_KEY')
        if not self.key or not self.secret:
            raise RuntimeError('Set ALPACA_API_KEY and ALPACA_SECRET_KEY')
    def max_bars(self): return 10000
    async def fetch(self, session, symbol, interval, start, end):
        if interval not in self.frames: raise ValueError(f'Unsupported Alpaca interval: {interval}')
        url = f'https://data.alpaca.markets/v2/stocks/{quote(symbol.upper(),safe=".-")}/bars'
        headers = {'APCA-API-KEY-ID': self.key,'APCA-API-SECRET-KEY':self.secret}
        rows = []
        token = None
        while True:
            params = {'start':iso(start),'end':iso(end),'timeframe':self.frames[interval],
                      'adjustment':'raw','feed':'iex','limit':10000}
            if token: params['page_token'] = token
            payload = await fetch_json(session,url,headers=headers,params=params)
            for r in payload.get('bars',[]):
                rows.append(Bar(timestamp(r['t']),float(r['o']),float(r['h']),float(r['l']),float(r['c']),float(r['v'])))
            LOG.info('PAGE symbol=%s bars=%d',symbol,len(rows))
            new_token = payload.get('next_page_token')
            if not new_token: break
            if new_token == token: raise RuntimeError('Alpaca returned repeated pagination token')
            token = new_token
        return rows


def validate_bars(bars, start, end, step):
    good = {}
    for b in bars:
        if not (start <= b.ts < end and b.ts % step == 0): continue
        if not (b.low <= min(b.open,b.close) <= max(b.open,b.close) <= b.high):
            raise ValueError(f'Invalid OHLC: {iso(b.ts)}')
        if b.volume < 0: raise ValueError(f'Negative volume: {iso(b.ts)}')
        good[b.ts] = b
    return list(good.values())


async def ingest_one(provider, session, db, symbol, interval, start, end, *, offline=False, refresh_last=0, pause=0):
    step = INTERVALS[interval]
    if start >= end or start % step or end % step: raise ValueError('Invalid or unaligned interval')
    if refresh_last < 0: raise ValueError('refresh_last must be nonnegative')
    cache = Cache(db)
    try:
        coverage = cache.intervals(provider.name,symbol,interval)
        gaps = missing(start,end,coverage)
        if refresh_last:
            gaps = merge(gaps + [(max(start,end-refresh_last*step),end)])
        if offline and gaps: raise RuntimeError(f'Offline cache incomplete for {symbol}: {gaps}')
        total = sum((b-a)//step for a,b in gaps)
        finished = 0
        downloaded = 0
        LOG.info('START %s %s interval=%s cached_ranges=%d missing_windows=%d',provider.name,symbol,interval,len(coverage),len(gaps))
        for left,right in gaps:
            pos = left
            while pos < right:
                stop = min(right,pos+provider.max_bars()*step)
                LOG.info('FETCH %s [%s, %s) progress=%d/%d (%.1f%%)',symbol,iso(pos),iso(stop),finished,total,100*finished/total if total else 100)
                bars = validate_bars(await provider.fetch(session,symbol,interval,pos,stop),pos,stop,step)
                cache.store(provider.name,symbol,interval,pos,stop,bars)
                downloaded += len(bars)
                finished += (stop-pos)//step
                pos = stop
                LOG.info('SAVED %s rows=%d progress=%d/%d (%.1f%%)',symbol,len(bars),finished,total,100*finished/total if total else 100)
                if pause and pos < right: await asyncio.sleep(pause)
        count = len(cache.read(provider.name,symbol,interval,start,end))
        LOG.info('DONE %s cache_rows=%d fetched_rows=%d',symbol,count,downloaded)
        return {'bars':count,'downloaded_or_refreshed':downloaded,'missing_windows':len(gaps)}
    finally: cache.close()


async def ingest_batch(provider, db, symbols, interval, start, end, *, workers=2, offline=False, refresh_last=0, pause=0):
    import aiohttp
    symbols = parse_symbols(symbols)
    if not 1 <= workers <= 8: raise ValueError('workers must be 1..8')
    # Initialize schema and WAL before concurrent per-symbol connections.
    setup_cache = Cache(db)
    setup_cache.close()
    semaphore = asyncio.Semaphore(workers)
    began = time.monotonic()
    done, failures = {}, {}
    timeout = aiohttp.ClientTimeout(total=90,connect=15)
    connector = aiohttp.TCPConnector(limit=workers, ssl=create_ssl_context())
    async with aiohttp.ClientSession(timeout=timeout,connector=connector,headers={'User-Agent':'market-data-pipeline/2.0'}) as session:
        async def task(symbol):
            async with semaphore:
                try:
                    result = await ingest_one(provider,session,db,symbol,interval,start,end,offline=offline,refresh_last=refresh_last,pause=pause)
                    return symbol,result,None
                except Exception as exc:
                    LOG.error('FAILED %s: %s',symbol,exc)
                    return symbol,None,str(exc)
        for coro in asyncio.as_completed([task(s) for s in symbols]):
            symbol,result,error = await coro
            if error: failures[symbol] = error
            else: done[symbol] = result
            LOG.info('BATCH %d/%d finished',len(done)+len(failures),len(symbols))
    return {'requested':len(symbols),'succeeded':done,'failed':failures,'elapsed_seconds':round(time.monotonic()-began,2)}


def export_parquet(db, provider, symbols, interval, start, end, folder):
    """Partitioned per-symbol Parquet snapshots; requires duckdb (`pip install duckdb`)."""
    try: import duckdb
    except ImportError as exc: raise RuntimeError('Install optional dependency: pip install duckdb') from exc
    out = Path(folder)
    out.mkdir(parents=True,exist_ok=True)
    con = duckdb.connect()
    cache = Cache(db)
    paths = []
    try:
        for symbol in symbols:
            records = cache.read(provider,symbol,interval,start,end)
            # Register using SQL bound values; never interpolate user-supplied values into SQL.
            con.execute('CREATE OR REPLACE TEMP TABLE bars (timestamp_utc TIMESTAMPTZ, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE)')
            if records:
                con.executemany('INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?)',
                    [(dt.datetime.fromtimestamp(b.ts,dt.timezone.utc),b.open,b.high,b.low,b.close,b.volume) for b in records])
            dest = out / provider / symbol / f'{interval}.parquet'
            dest.parent.mkdir(parents=True,exist_ok=True)
            con.execute("COPY bars TO '" + str(dest).replace("'", "''") + "' (FORMAT PARQUET, COMPRESSION ZSTD)")
            LOG.info('PARQUET %s rows=%d path=%s',symbol,len(records),dest)
            paths.append(str(dest))
    finally:
        cache.close()
        con.close()
    return paths


async def run(args, provider=None):
    symbols = parse_symbols(args.symbols)
    provider = provider or (AsyncCoinbase() if args.provider=='coinbase' else AsyncAlpaca())
    if args.interval not in INTERVALS: raise ValueError('Unsupported interval')
    start = timestamp(args.start)
    if args.end: end = timestamp(args.end)
    else:
        now = int(time.time())
        step = INTERVALS[args.interval]
        end = now - (now % step)  # exclude currently forming candle
    if end <= start: raise ValueError('End must be after start')
    result = await ingest_batch(provider,args.db,symbols,args.interval,start,end,
                                workers=args.workers,offline=args.offline,refresh_last=args.refresh_last,pause=args.pause)
    if args.parquet_dir:
        # File export is blocking I/O, keep it off the async event loop.
        await asyncio.to_thread(export_parquet,args.db,provider.name,
                                [s for s in symbols if s in result['succeeded']],args.interval,start,end,args.parquet_dir)
    print(json.dumps(result,indent=2))
    return result


async def main_async(args):
    provider = AsyncCoinbase() if args.provider=='coinbase' else AsyncAlpaca()
    while True:
        result = await run(args,provider)
        if not args.every_seconds: return 1 if result['failed'] else 0
        LOG.info('SCHEDULE sleep=%ds; press Ctrl+C to stop',args.every_seconds)
        await asyncio.sleep(args.every_seconds)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Async market data ingest; end exclusive UTC; optional foreground scheduler')
    parser.add_argument('--provider',choices=['coinbase','alpaca'],default='coinbase')
    parser.add_argument('--symbols',nargs='+',required=True)
    parser.add_argument('--interval',choices=list(INTERVALS),default='1h')
    parser.add_argument('--start',required=True)
    parser.add_argument('--end',help='exclusive end; omit for latest fully closed UTC bar')
    parser.add_argument('--db',default='data/market_data.sqlite')
    parser.add_argument('--workers',type=int,default=2)
    parser.add_argument('--refresh-last',type=int,default=0)
    parser.add_argument('--pause',type=float,default=0.1)
    parser.add_argument('--offline',action='store_true')
    parser.add_argument('--parquet-dir')
    parser.add_argument('--every-seconds',type=int,default=0,help='poll repeatedly in foreground; requires omitting --end')
    args = parser.parse_args(argv)
    if args.every_seconds < 0 or (args.every_seconds and args.end):
        parser.error('--every-seconds must be positive and requires omitting --end')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)-7s | %(message)s',datefmt='%H:%M:%S',stream=sys.stdout)
    try: return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        LOG.info('Stopped by user')
        return 0


if __name__=='__main__':
    sys.exit(main())
