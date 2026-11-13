"""Reference L3 lit order book and midpoint dark pool; prices in integer ticks."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal
import heapq
from typing import Optional


@dataclass
class Order:
    id: str
    symbol: str
    side: str
    qty: int
    price: Optional[int] = None
    tif: str = 'GTC' # GTC / IOC / FOK
    owner: Optional[str] = None
    min_qty: int = 1
    remaining: int = field(init=False)

    def __post_init__(self):
        self.remaining = self.qty
        if self.side not in ('BUY', 'SELL') or self.qty <= 0:
            raise ValueError('Invalid side or quantity')
        if self.tif not in ('GTC', 'IOC', 'FOK'):
            raise ValueError('Invalid TIF')
        if self.price is not None and self.price <= 0:
            raise ValueError('Price must be positive ticks')
        if not 1 <= self.min_qty <= self.qty:
            raise ValueError('Invalid minimum execution quantity')


@dataclass(frozen=True)
class Trade:
    symbol: str
    buy_id: str
    sell_id: str
    qty: int
    price: Decimal
    venue: str


@dataclass
class Result:
    status: str
    trades: list[Trade] = field(default_factory=list)
    resting: int = 0
    canceled: int = 0


class LitBook:
    """
    One symbol, limit order book, price-time priority, lazy O(1) cancellation.

    Trade price = resting order price. Self-trade prevention = cancel incoming remainder.
    IDs unique for the lifetime of the book (including previously canceled orders).
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.levels = {'BUY': {}, 'SELL': {}}
        self.volume = {'BUY': {}, 'SELL': {}}
        self.heaps = {'BUY': [], 'SELL': []}
        self.active: dict[str, Order] = {}
        self.used_ids: set[str] = set()
        self.generation: dict[str, int] = {}
        self.trades: list[Trade] = []

    def _key(self, side: str, price: int):
        return -price if side == 'BUY' else price

    def _best(self, side: str) -> Optional[int]:
        h = self.heaps[side]
        while h:
            price = -h[0] if side == 'BUY' else h[0]
            if price in self.levels[side]:
                return price
            heapq.heappop(h)
        return None

    def _add(self, o: Order):
        side, price = o.side, o.price
        if price not in self.levels[side]:
            self.levels[side][price] = deque()
            self.volume[side][price] = 0
            heapq.heappush(self.heaps[side], self._key(side, price))
        self.generation[o.id] = self.generation.get(o.id, 0) + 1
        self.levels[side][price].append((o.id, self.generation[o.id]))
        self.volume[side][price] += o.remaining
        self.active[o.id] = o

    def _drop_level_if_empty(self, side: str, price: int):
        if self.volume[side].get(price) == 0:
            self.volume[side].pop(price, None)
            self.levels[side].pop(price, None)  # stale IDs discarded with whole level

    def cancel(self, order_id: str, qty: Optional[int] = None) -> int:
        """Cancel all or part; returns canceled quantity. Partial decrease retains FIFO priority."""
        o = self.active.get(order_id)
        if o is None:
            return 0
        if qty is not None and qty <= 0:
            raise ValueError('Cancel quantity must be positive')
        take = o.remaining if qty is None else min(qty, o.remaining)
        o.remaining -= take
        self.volume[o.side][o.price] -= take
        if o.remaining == 0:
            del self.active[o.id]  # lazy unlink from deque
        self._drop_level_if_empty(o.side, o.price)
        return take

    def _available(self, incoming: Order) -> bool:
        """FOK preflight. Stops at an eligible self order because STP cancels the taker."""
        contra = 'SELL' if incoming.side == 'BUY' else 'BUY'
        prices = sorted(self.levels[contra], reverse=(contra == 'BUY'))
        need = incoming.remaining
        for p in prices:
            if incoming.price is not None:
                if incoming.side == 'BUY' and p > incoming.price: break
                if incoming.side == 'SELL' and p < incoming.price: break
            for oid, gen in self.levels[contra][p]:
                maker = self.active.get(oid)
                if maker is None or self.generation[oid] != gen: continue
                if incoming.owner is not None and maker.owner == incoming.owner:
                    return False
                need -= maker.remaining
                if need <= 0: return True
        return False

    def submit(self, o: Order, post_only: bool = False) -> Result:
        if o.symbol != self.symbol or o.id in self.used_ids or o.remaining != o.qty:
            raise ValueError('Wrong symbol, duplicate ID, or reused order')
        if o.price is None and (o.tif == 'GTC' or post_only):
            raise ValueError('Market orders must be IOC/FOK and cannot be post-only')
        self.used_ids.add(o.id)
        contra = 'SELL' if o.side == 'BUY' else 'BUY'
        best = self._best(contra)

        def crosses(p):
            return p is not None and (o.price is None or
                (o.side == 'BUY' and o.price >= p) or
                (o.side == 'SELL' and o.price <= p))

        if post_only and crosses(best):
            return Result('REJECTED_POST_ONLY', canceled=o.remaining)
        if o.tif == 'FOK' and not self._available(o):
            return Result('REJECTED_FOK', canceled=o.remaining)

        result = Result('')
        while o.remaining:
            best = self._best(contra)
            if not crosses(best): break
            queue = self.levels[contra][best]
            while queue and (queue[0][0] not in self.active or self.generation[queue[0][0]] != queue[0][1]):
                queue.popleft()
            if not queue:
                self.volume[contra].pop(best, None)
                del self.levels[contra][best]
                continue
            maker = self.active[queue[0][0]]
            if o.owner is not None and maker.owner == o.owner:
                result.canceled = o.remaining
                o.remaining = 0
                result.status = 'CANCELED_SELF_TRADE'
                break
            fill = min(o.remaining, maker.remaining)
            o.remaining -= fill
            maker.remaining -= fill
            self.volume[contra][best] -= fill
            t = Trade(self.symbol,
                      o.id if o.side == 'BUY' else maker.id,
                      maker.id if o.side == 'BUY' else o.id,
                      fill, Decimal(best), 'LIT')
            result.trades.append(t)
            self.trades.append(t)
            if maker.remaining == 0:
                del self.active[maker.id]
                queue.popleft()
            self._drop_level_if_empty(contra, best)

        if o.remaining:
            if o.tif == 'GTC':
                self._add(o)
                result.resting = o.remaining
            else:
                result.canceled = o.remaining
                o.remaining = 0
        if not result.status:
            result.status = ('RESTING' if result.resting else
                             'FILLED' if result.trades and not result.canceled else
                             'PARTIAL_CANCEL' if result.trades else 'CANCELED')
        return result

    def replace(self, order_id: str, new_qty: int, new_price: Optional[int] = None) -> Result:
        """
        new_qty = desired *remaining* qty; same-price reduction keeps priority.
        Increase or price change loses time priority and may cross the book.
        """
        old = self.active.get(order_id)
        if old is None: raise KeyError(order_id)
        if new_qty <= 0: raise ValueError('Use cancel() to remove an order')
        price = old.price if new_price is None else new_price
        if price == old.price and new_qty <= old.remaining:
            self.cancel(order_id, old.remaining - new_qty) if new_qty < old.remaining else None
            return Result('RESTING', resting=new_qty)
        symbol, side, owner = old.symbol, old.side, old.owner
        self.cancel(order_id)
        self.used_ids.remove(order_id)
        return self.submit(Order(order_id, symbol, side, new_qty, price, 'GTC', owner))

    def top(self, n: int = 5) -> dict:
        return {side: [(p, self.volume[side][p]) for p in
                       sorted(self.levels[side], reverse=(side == 'BUY'))[:n]]
                for side in ('BUY', 'SELL')}

    def l3(self, side: str, price: int) -> list[tuple[str, int]]:
        """Visible active orders at a specific price, FIFO order."""
        return [(oid, self.active[oid].remaining) for oid, gen in
                self.levels[side].get(price, ()) if oid in self.active and self.generation[oid] == gen]


class DarkPool:
    """
    Midpoint-crossing, time-priority dark pool, with optional limit protection.

    A trade must satisfy BOTH counterparties' min_qty (minimum per fill).
    No valid NBBO => no executions. Self-match prevention cancels incoming remainder.
    FOK preflight is atomic. No public displayed book.
    """
    def __init__(self):
        self.queues = defaultdict(lambda: {'BUY': deque(), 'SELL': deque()})
        self.active: dict[str, Order] = {}
        self.used_ids: set[str] = set()
        self.generation: dict[str, int] = {}
        self.nbbo: dict[str, tuple[int, int]] = {}
        self.trades: list[Trade] = []

    def set_nbbo(self, symbol: str, bid: int, ask: int) -> list[Trade]:
        if bid <= 0 or ask <= bid:
            raise ValueError('Require positive, non-crossed, non-locked NBBO')
        self.nbbo[symbol] = (bid, ask)
        return self.cross(symbol)

    def midpoint(self, symbol: str) -> Optional[Decimal]:
        q = self.nbbo.get(symbol)
        return (Decimal(q[0]) + Decimal(q[1])) / 2 if q else None

    def _eligible(self, o: Order, midpoint: Decimal) -> bool:
        return o.price is None or (midpoint <= o.price if o.side == 'BUY' else midpoint >= o.price)

    def cancel(self, order_id: str, qty: Optional[int] = None) -> int:
        o = self.active.get(order_id)
        if o is None: return 0
        if qty is not None and qty <= 0: raise ValueError('Invalid cancellation quantity')
        take = o.remaining if qty is None else min(qty, o.remaining)
        o.remaining -= take
        if not o.remaining:
            del self.active[order_id]  # lazy unlink
        return take

    def _candidate(self, taker: Order, mid: Decimal, simulation: dict[str, int]):
        """Find oldest price- and min-size-eligible maker; skip ineligible orders."""
        opposite = 'SELL' if taker.side == 'BUY' else 'BUY'
        for oid, gen in self.queues[taker.symbol][opposite]:
            maker = self.active.get(oid)
            if maker is None or self.generation[oid] != gen: continue
            available = simulation.get(oid, maker.remaining)
            if not available or not self._eligible(maker, mid): continue
            size = min(taker.remaining, available)
            if size < max(taker.min_qty, maker.min_qty): continue
            return maker, size
        return None, 0

    def _fok_available(self, taker: Order, mid: Decimal) -> bool:
        # simulate without modifying actual resting orders.
        remaining = taker.qty
        simulated: dict[str, int] = {}
        opposite = 'SELL' if taker.side == 'BUY' else 'BUY'
        for oid, gen in self.queues[taker.symbol][opposite]:
            maker = self.active.get(oid)
            if maker is None or self.generation[oid] != gen or not self._eligible(maker, mid): continue
            available = simulated.get(oid, maker.remaining)
            size = min(remaining, available)
            if size < max(taker.min_qty, maker.min_qty): continue
            if taker.owner is not None and maker.owner == taker.owner:
                return False
            remaining -= size
            simulated[oid] = available - size
            if remaining == 0: return True
        return False

    def submit(self, o: Order) -> Result:
        if o.id in self.used_ids or o.remaining != o.qty:
            raise ValueError('Duplicate or reused order')
        self.used_ids.add(o.id)
        mid = self.midpoint(o.symbol)
        if o.tif == 'FOK' and (mid is None or not self._eligible(o, mid) or not self._fok_available(o, mid)):
            return Result('REJECTED_FOK', canceled=o.remaining)
        result = Result('')
        if mid is not None and self._eligible(o, mid):
            while o.remaining:
                maker, fill = self._candidate(o, mid, {})
                if maker is None: break
                if o.owner is not None and maker.owner == o.owner:
                    result.canceled = o.remaining
                    o.remaining = 0
                    result.status = 'CANCELED_SELF_TRADE'
                    break
                maker.remaining -= fill
                o.remaining -= fill
                t = Trade(o.symbol, o.id if o.side == 'BUY' else maker.id,
                          maker.id if o.side == 'BUY' else o.id, fill, mid, 'DARK')
                result.trades.append(t)
                self.trades.append(t)
                if not maker.remaining: del self.active[maker.id]
        if o.remaining:
            if o.tif == 'GTC':
                self.generation[o.id] = self.generation.get(o.id, 0) + 1
                self.queues[o.symbol][o.side].append((o.id, self.generation[o.id]))
                self.active[o.id] = o
                result.resting = o.remaining
            else:
                result.canceled = o.remaining
                o.remaining = 0
        if not result.status:
            result.status = ('RESTING' if result.resting else
                             'FILLED' if result.trades and not result.canceled else
                             'PARTIAL_CANCEL' if result.trades else 'CANCELED')
        return result

    def replace(self, order_id: str, new_qty: int, new_limit: Optional[int] = None,
                change_limit: bool = False) -> Result:
        """
        Same limit + reduction preserves priority; increase or repricing loses priority.
        change_limit=True allows explicitly replacing a limit with None (unpriced midpoint order).
        """
        old = self.active.get(order_id)
        if old is None: raise KeyError(order_id)
        if new_qty <= 0: raise ValueError('Use cancel()')
        price = new_limit if change_limit else old.price
        if price == old.price and new_qty <= old.remaining:
            if new_qty < old.remaining: self.cancel(order_id, old.remaining - new_qty)
            return Result('RESTING', resting=new_qty)
        if new_qty < old.min_qty: raise ValueError('Quantity below min_qty')
        symbol, side, tif, owner, min_qty = old.symbol, old.side, old.tif, old.owner, old.min_qty
        self.cancel(order_id)
        self.used_ids.remove(order_id)
        return self.submit(Order(order_id, symbol, side, new_qty, price, tif, owner, min_qty))

    def cross(self, symbol: str) -> list[Trade]:
        """
        Reattempt to cross resting GTC orders after midpoint/NBBO changes.
        Iterate oldest buyers and select earliest compatible seller.
        """
        mid = self.midpoint(symbol)
        if mid is None: return []
        trades = []
        buyers = self.queues[symbol]['BUY']
        # Snapshot avoids disturbing FIFO; live quantities are mutated in place.
        while True:
            matched = False
            for bid_id, gen in list(buyers):
                buy = self.active.get(bid_id)
                if buy is None or self.generation[bid_id] != gen or not self._eligible(buy, mid): continue
                maker, fill = self._candidate(buy, mid, {})
                if maker is None: continue
                if buy.owner is not None and buy.owner == maker.owner:
                    self.cancel(buy.id)  # resting buy treated as incoming for this crossing pass
                    matched = True
                    break
                buy.remaining -= fill
                maker.remaining -= fill
                trade = Trade(symbol, buy.id, maker.id, fill, mid, 'DARK')
                trades.append(trade)
                self.trades.append(trade)
                if not buy.remaining: del self.active[buy.id]
                if not maker.remaining: del self.active[maker.id]
                matched = True
                break
            if not matched: break
        return trades

    def outstanding(self, symbol: str):
        return {side: [(oid, self.active[oid].remaining) for oid, gen in self.queues[symbol][side]
                       if oid in self.active and self.generation[oid] == gen] for side in ('BUY', 'SELL')}


if __name__ == '__main__':
    lit = LitBook('AAPL')
    assert lit.submit(Order('S1', 'AAPL', 'SELL', 100, 101)).resting == 100
    assert lit.submit(Order('S2', 'AAPL', 'SELL', 60, 101)).resting == 60
    r = lit.submit(Order('B1', 'AAPL', 'BUY', 120, 102, 'IOC'))
    assert [(t.sell_id, t.qty, t.price) for t in r.trades] == [
        ('S1', 100, Decimal(101)), ('S2', 20, Decimal(101))]
    assert lit.l3('SELL', 101) == [('S2', 40)]
    assert lit.cancel('S2', 10) == 10
    assert lit.top(1)['SELL'] == [(101, 30)]
    assert lit.submit(Order('B2', 'AAPL', 'BUY', 20, 100, 'GTC')).resting == 20
    assert lit.replace('B2', 15).resting == 15
    assert lit.submit(Order('X', 'AAPL', 'BUY', 50, 101, 'FOK')).status == 'REJECTED_FOK'
    assert lit.submit(Order('P', 'AAPL', 'BUY', 1, 101), post_only=True).status == 'REJECTED_POST_ONLY'

    dark = DarkPool()
    assert dark.set_nbbo('AAPL', 100, 101) == []
    assert dark.submit(Order('DB1', 'AAPL', 'BUY', 100, min_qty=20)).resting == 100
    assert dark.submit(Order('DS1', 'AAPL', 'SELL', 10)).resting == 10  # too small
    r = dark.submit(Order('DS2', 'AAPL', 'SELL', 60, tif='IOC', min_qty=20))
    assert [(t.qty, t.price) for t in r.trades] == [(60, Decimal('100.5'))]
    assert dark.active['DB1'].remaining == 40
    assert dark.submit(Order('DS3', 'AAPL', 'SELL', 50, tif='FOK')).status == 'REJECTED_FOK'
    assert dark.cancel('DS1') == 10
    print('All sample assertions passed')
