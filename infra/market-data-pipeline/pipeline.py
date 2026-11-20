"""Incremental market-data ingestion with durable SQLite cache and visible progress."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime as dt
import json
import logging
import os
import random
import sqlite3
import ssl

import certifi
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

LOG = logging.getLogger('marketdata')
UTC = dt.timezone.utc
INTERVALS = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '6h': 21600, '1d': 86400}


def create_ssl_context() -> ssl.SSLContext:
    """Keep verification enabled; trust system roots and certifi's public roots."""
    context = ssl.create_default_context()
    context.load_verify_locations(cafile=certifi.where())
    # Optional additional trusted CA for managed networks.
    extra_ca = os.getenv('MARKET_DATA_CA_BUNDLE')
    if extra_ca:
        context.load_verify_locations(cafile=extra_ca)
    return context


def timestamp(value: str) -> int:
    if len(value) == 10:
        value += 'T00:00:00+00:00'
    value = value.replace('Z', '+00:00')
    obj = dt.datetime.fromisoformat(value)
    if obj.tzinfo is None:
        obj = obj.replace(tzinfo=UTC)
    return int(obj.timestamp())


def iso(ts: int) -> str:
    return dt.datetime.fromtimestamp(ts, UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def http_json(url: str, headers: dict | None = None, params: dict | None = None,
              retries: int = 5, sleeper=time.sleep) -> object:
    url = url + ('?' + urlencode(params) if params else '')
    context = create_ssl_context()
    for attempt in range(retries):
        try:
            req = Request(url, headers={'User-Agent': 'market-data-pipeline/1.0', 'Accept': 'application/json', **(headers or {})})
            with urlopen(req, timeout=30, context=context) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == retries - 1:
                raise RuntimeError(f'HTTP {exc.code}: {url} (check credentials / plan / symbol)') from exc
            retry_after = exc.headers.get('Retry-After')
            try:
                delay = min(60, max(0.1, float(retry_after))) if retry_after else min(30, 2 ** attempt + random.random())
            except ValueError:
                delay = min(30, 2 ** attempt + random.random())
            LOG.warning('RETRY HTTP status=%d attempt=%d/%d delay=%.1fs url=%s', exc.code, attempt+1, retries, delay, url)
        except (ssl.SSLError, URLError, TimeoutError) as exc:
            if isinstance(getattr(exc, 'reason', exc), ssl.SSLError):
                raise RuntimeError(f'TLS failure (not retried): {url}: {exc}') from exc
            if attempt == retries - 1:
                raise RuntimeError(f'Network failure: {url}: {exc}') from exc
            delay = min(30, 2 ** attempt + random.random())
            LOG.warning('RETRY network attempt=%d/%d delay=%.1fs url=%s error=%s', attempt+1, retries, delay, url, exc)
        sleeper(delay)
    raise RuntimeError('Unreachable')

@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class Provider:
    name: str
    def fetch(self, symbol: str, interval: str, start: int, end: int) -> list[Bar]:
        raise NotImplementedError
    def max_bars(self) -> int:
        return 250


class Coinbase(Provider):
    name = 'coinbase'
    def fetch(self, symbol: str, interval: str, start: int, end: int) -> list[Bar]:
        if interval not in INTERVALS:
            raise ValueError(f'Coinbase unsupported interval: {interval}')
        data = http_json(f'https://api.exchange.coinbase.com/products/{symbol.upper()}/candles',
                         params={'start': iso(start), 'end': iso(end), 'granularity': INTERVALS[interval]})
        if not isinstance(data, list):
            raise RuntimeError(f'Coinbase returned unexpected response: {str(data)[:300]}')
        return [Bar(int(row[0]), float(row[3]), float(row[2]), float(row[1]), float(row[4]), float(row[5])) for row in data]


class Alpaca(Provider):
    name = 'alpaca'
    def __init__(self):
        self.key = os.getenv('ALPACA_API_KEY')
        self.secret = os.getenv('ALPACA_SECRET_KEY')
        if not self.key or not self.secret:
            raise RuntimeError('Set ALPACA_API_KEY and ALPACA_SECRET_KEY (free Alpaca account).')

    def fetch(self, symbol: str, interval: str, start: int, end: int) -> list[Bar]:
        frames = {'1m': '1Min', '5m': '5Min', '15m': '15Min', '1h': '1Hour', '1d': '1Day'}
        if interval not in frames:
            raise ValueError(f'Alpaca unsupported interval: {interval}')
        result: list[Bar] = []
        token = None
        page = 0
        while True:
            args = {'start': iso(start), 'end': iso(end), 'timeframe': frames[interval], 'feed': 'iex', 'limit': 10000,
                    'adjustment': 'raw'}
            if token:
                args['page_token'] = token
            payload = http_json(f'https://data.alpaca.markets/v2/stocks/{symbol.upper()}/bars',
                                headers={'APCA-API-KEY-ID': self.key, 'APCA-API-SECRET-KEY': self.secret}, params=args)
            page += 1
            rows = payload.get('bars', [])
            for row in rows:
                result.append(Bar(timestamp(row['t']), float(row['o']), float(row['h']),
                                  float(row['l']), float(row['c']), float(row['v'])))
            LOG.info('Alpaca page=%d symbol=%s rows=%d total=%d', page, symbol, len(rows), len(result))
            token = payload.get('next_page_token')
            if not token:
                break
        return result

    def max_bars(self) -> int:
        return 10000


class Cache:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute('PRAGMA busy_timeout=30000')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS bars (
            provider TEXT NOT NULL, symbol TEXT NOT NULL, interval TEXT NOT NULL, ts INTEGER NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, volume REAL NOT NULL,
            PRIMARY KEY(provider,symbol,interval,ts));
          CREATE TABLE IF NOT EXISTS coverage (
            provider TEXT NOT NULL, symbol TEXT NOT NULL, interval TEXT NOT NULL,
            start_ts INTEGER NOT NULL, end_ts INTEGER NOT NULL,
            PRIMARY KEY(provider,symbol,interval,start_ts,end_ts));
          CREATE INDEX IF NOT EXISTS coverage_lookup ON coverage(provider,symbol,interval,start_ts,end_ts);
        ''')

    def close(self):
        self.db.close()

    def intervals(self, provider: str, symbol: str, interval: str) -> list[tuple[int,int]]:
        rows = self.db.execute('SELECT start_ts,end_ts FROM coverage WHERE provider=? AND symbol=? AND interval=? ORDER BY start_ts',
                               (provider,symbol,interval)).fetchall()
        return merge(rows)

    def store(self, provider: str, symbol: str, interval: str, start: int, end: int, bars: list[Bar]):
        # Commit bars and coverage atomically; failed or incomplete calls never mark ranges cached.
        with self.db:
            self.db.executemany('''INSERT INTO bars VALUES (?,?,?,?,?,?,?,?,?)
              ON CONFLICT(provider,symbol,interval,ts) DO UPDATE SET
              open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume''',
              [(provider,symbol,interval,b.ts,b.open,b.high,b.low,b.close,b.volume) for b in bars if start <= b.ts < end])
            self.db.execute('INSERT OR IGNORE INTO coverage VALUES (?,?,?,?,?)', (provider,symbol,interval,start,end))
            merged = self.intervals(provider,symbol,interval)
            self.db.execute('DELETE FROM coverage WHERE provider=? AND symbol=? AND interval=?', (provider,symbol,interval))
            self.db.executemany('INSERT INTO coverage VALUES (?,?,?,?,?)', [(provider,symbol,interval,a,b) for a,b in merged])

    def read(self, provider: str, symbol: str, interval: str, start: int, end: int) -> list[Bar]:
        rows = self.db.execute('''SELECT ts,open,high,low,close,volume FROM bars
          WHERE provider=? AND symbol=? AND interval=? AND ts>=? AND ts<? ORDER BY ts''',
          (provider,symbol,interval,start,end)).fetchall()
        return [Bar(*row) for row in rows]


def merge(ranges: list[tuple[int,int]]) -> list[tuple[int,int]]:
    out = []
    for start,end in sorted(ranges):
        if not out or start > out[-1][1]:
            out.append([start,end])
        else:
            out[-1][1] = max(end, out[-1][1])
    return [(a,b) for a,b in out]


def missing(start: int, end: int, cached: list[tuple[int,int]]) -> list[tuple[int,int]]:
    result = []
    cursor = start
    for a,b in merge(cached):
        if b <= cursor or a >= end:
            continue
        if a > cursor:
            result.append((cursor,min(a,end)))
        cursor = max(cursor,min(b,end))
        if cursor >= end:
            break
    if cursor < end:
        result.append((cursor,end))
    return result


def ingest(provider: Provider, cache: Cache, symbol: str, interval: str,
           start: int, end: int, *, offline=False, refresh_last=0, pause=0.1) -> dict:
    if interval not in INTERVALS or start >= end:
        raise ValueError('Invalid interval or date range')
    step = INTERVALS[interval]
    if start % step or end % step:
        raise ValueError('Dates must align to bar boundaries in UTC (for daily, UTC midnight).')
    if refresh_last < 0:
        raise ValueError('refresh_last must be >= 0')
    # Explicit refresh invalidates tail logically, without dropping pre-existing cached data.
    coverage = cache.intervals(provider.name, symbol, interval)
    gaps = missing(start,end,coverage)
    if refresh_last:
        tail = max(start,end-refresh_last*step)
        gaps = merge(gaps + [(tail,end)])
    LOG.info('START provider=%s symbol=%s interval=%s range=[%s,%s) cached_ranges=%d missing_ranges=%d',
             provider.name,symbol,interval,iso(start),iso(end),len(coverage),len(gaps))
    if offline and gaps:
        raise RuntimeError(f'Offline cache incomplete: {[(iso(a),iso(b)) for a,b in gaps]}')
    total = sum((b-a + step-1)//step for a,b in gaps)
    finished = 0
    new_rows = 0
    max_span = provider.max_bars()*step
    for left,right in gaps:
        pos = left
        while pos < right:
            stop = min(right,pos+max_span)
            LOG.info('FETCH %s %s [%s, %s) bars_progress=%d/%d (%.1f%%)',
                     provider.name,symbol,iso(pos),iso(stop),finished,total,
                     100*finished/total if total else 100)
            bars = provider.fetch(symbol,interval,pos,stop)
            # Reject timestamps outside scope, duplicates, impossible candles to avoid poisoning cache.
            good = {}
            for bar in bars:
                if pos <= bar.ts < stop and bar.ts % step == 0:
                    if not (bar.low <= min(bar.open,bar.close) <= max(bar.open,bar.close) <= bar.high):
                        raise RuntimeError(f'Invalid OHLC candle at {iso(bar.ts)}')
                    if bar.volume < 0:
                        raise RuntimeError(f'Negative volume at {iso(bar.ts)}')
                    good[bar.ts] = bar
            cache.store(provider.name,symbol,interval,pos,stop,list(good.values()))
            finished += (stop-pos + step-1)//step
            new_rows += len(good)
            LOG.info('SAVED %d candles; covered=%d/%d (%.1f%%) new_or_updated=%d',
                     len(good),finished,total,100*finished/total if total else 100,new_rows)
            pos = stop
            if pause and pos < right:
                time.sleep(pause)
    final = cache.read(provider.name,symbol,interval,start,end)
    LOG.info('DONE symbol=%s cached_bars=%d fetched_bars=%d requests_finished=%s', symbol,len(final),new_rows, 'yes')
    return {'bars':len(final),'downloaded_or_refreshed':new_rows,'missing_windows':len(gaps)}


def export_csv(cache: Cache, provider: str, symbol: str, interval: str, start: int, end: int, dest: str):
    Path(dest).parent.mkdir(parents=True,exist_ok=True)
    bars = cache.read(provider,symbol,interval,start,end)
    with open(dest,'w',newline='') as f:
        w = csv.writer(f)
        w.writerow(['timestamp_utc','open','high','low','close','volume'])
        for b in bars:
            w.writerow([iso(b.ts),b.open,b.high,b.low,b.close,b.volume])
    LOG.info('EXPORTED %d bars -> %s',len(bars),dest)


def parse_symbols(raw: list[str]) -> list[str]:
    """Accept --symbols AAPL MSFT or --symbols AAPL,MSFT; deduplicate in order."""
    symbols = []
    for entry in raw:
        for value in entry.split(','):
            symbol = value.strip().upper()
            if not symbol:
                continue
            if not all(char.isalnum() or char in '.-_' for char in symbol):
                raise ValueError(f'Invalid symbol: {symbol!r}')
            if symbol not in symbols:
                symbols.append(symbol)
    if not symbols:
        raise ValueError('Specify at least one symbol')
    return symbols


def ingest_many(provider_factory, db: str, symbols: list[str], interval: str,
                start: int, end: int, *, workers=3, offline=False, refresh_last=0,
                pause=0.1, csv_dir: str | None = None) -> dict:
    """Parallel I/O fetching with independent SQLite connections per ticker.

    Failed symbols are recorded; other symbols still finish. Thread pool is bounded
    to avoid overwhelming public/free market-data API rate limits.
    """
    if not 1 <= workers <= 8:
        raise ValueError('workers must be between 1 and 8')
    symbols = parse_symbols(symbols)
    # Initialize the SQLite schema and WAL once before worker threads open connections.
    setup_cache = Cache(db)
    setup_cache.close()
    started = time.monotonic()
    LOG.info('BATCH START tickers=%d workers=%d symbols=%s', len(symbols), workers, ','.join(symbols))

    def worker(symbol):
        provider = provider_factory()
        cache = Cache(db)
        try:
            result = ingest(provider,cache,symbol,interval,start,end,
                            offline=offline,refresh_last=refresh_last,pause=pause)
            if csv_dir is not None:
                export_csv(cache,provider.name,symbol,interval,start,end,
                           str(Path(csv_dir) / f'{provider.name}_{symbol}_{interval}.csv'))
            return result
        finally:
            cache.close()

    successful, failed = {}, {}
    with ThreadPoolExecutor(max_workers=min(workers,len(symbols))) as pool:
        pending = {pool.submit(worker, symbol): symbol for symbol in symbols}
        for future in as_completed(pending):
            symbol = pending[future]
            try:
                successful[symbol] = future.result()
                LOG.info('BATCH OK ticker=%s (%d/%d completed)',symbol,len(successful)+len(failed),len(symbols))
            except Exception as exc:
                failed[symbol] = str(exc)
                LOG.error('BATCH FAILED ticker=%s error=%s (%d/%d completed)',
                          symbol,exc,len(successful)+len(failed),len(symbols))
    summary = {'requested':len(symbols),'succeeded':successful,'failed':failed,
               'elapsed_seconds':round(time.monotonic()-started,2)}
    LOG.info('BATCH DONE succeeded=%d failed=%d elapsed=%.2fs',
             len(successful),len(failed),summary['elapsed_seconds'])
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description='Real market data -> incremental local SQLite cache (end date exclusive)')
    parser.add_argument('--provider',choices=['coinbase','alpaca'],default='coinbase')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--symbol',default=None,help='single ticker (backward compatible)')
    group.add_argument('--symbols',nargs='+',help='multiple tickers: AAPL MSFT or AAPL,MSFT')
    parser.add_argument('--workers',type=int,default=3,help='concurrent tickers (1-8; default 3)')
    parser.add_argument('--csv-dir',default=None,help='export each ticker to this directory')
    parser.add_argument('--interval',choices=list(INTERVALS),default='1h')
    parser.add_argument('--start',required=True,help='UTC YYYY-MM-DD or ISO UTC datetime')
    parser.add_argument('--end',required=True,help='exclusive UTC YYYY-MM-DD or ISO UTC datetime')
    parser.add_argument('--db',default='data/market_data.sqlite')
    parser.add_argument('--csv',default=None,help='optional export path')
    parser.add_argument('--offline',action='store_true',help='fail if requested coverage is missing')
    parser.add_argument('--refresh-last',type=int,default=0,help='re-fetch N most recent bars even if cached')
    parser.add_argument('--pause',type=float,default=0.1,help='seconds between requests')
    parser.add_argument('--verbose',action='store_true')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='%(asctime)s | %(levelname)-7s | %(message)s',datefmt='%H:%M:%S',stream=sys.stdout)
    start,end = timestamp(args.start),timestamp(args.end)
    symbols = parse_symbols(args.symbols if args.symbols else [args.symbol or 'BTC-USD'])
    if args.csv and len(symbols) != 1:
        parser.error('--csv is single-ticker only; use --csv-dir for multiple tickers')
    factory = Coinbase if args.provider == 'coinbase' else Alpaca
    if args.symbols or len(symbols) > 1:
        summary = ingest_many(factory,args.db,symbols,args.interval,start,end,
                              workers=args.workers,offline=args.offline,
                              refresh_last=args.refresh_last,pause=args.pause,
                              csv_dir=args.csv_dir)
        print(json.dumps(summary,indent=2))
        if summary['failed']:
            raise SystemExit(1)
    else:
        provider = factory()
        cache = Cache(args.db)
        try:
            result = ingest(provider,cache,symbols[0],args.interval,start,end,
                            offline=args.offline,refresh_last=args.refresh_last,pause=args.pause)
            if args.csv:
                export_csv(cache,provider.name,symbols[0],args.interval,start,end,args.csv)
            if args.csv_dir:
                export_csv(cache,provider.name,symbols[0],args.interval,start,end,
                           str(Path(args.csv_dir)/f'{provider.name}_{symbols[0]}_{args.interval}.csv'))
            print(json.dumps(result,indent=2))
        finally:
            cache.close()

if __name__ == '__main__':
    main()
