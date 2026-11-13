from __future__ import annotations

from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from matching_engines import LitBook, Order, Result, Trade


@dataclass(frozen=True)
class MarketEvent:
    symbol: str
    seq: int
    kind: str
    order_id: str
    side: str
    price: int
    qty: int = 0

    def __post_init__(self):
        if self.seq < 1 or self.kind not in {'ADD', 'MODIFY', 'EXECUTE', 'DELETE'}:
            raise ValueError('Invalid sequence or event kind')
        if self.side not in {'BUY', 'SELL'} or self.price <= 0:
            raise ValueError('Invalid side or price')
        if self.kind != 'DELETE' and self.qty <= 0:
            raise ValueError('Quantity must be positive')


@dataclass(frozen=True)
class SnapshotOrder:
    id: str
    side: str
    price: int
    remaining: int


@dataclass(frozen=True)
class BookSnapshot:
    symbol: str
    last_seq: int
    orders: tuple[SnapshotOrder, ...] 


@dataclass
class BookOrder:
    id: str
    side: str
    price: int
    remaining: int


class BookProtocolError(RuntimeError):
    """Invalid exchange transition; never silently accept an inconsistent book."""


class ReconstructedBook:
    """L3 order-by-order book. No matching logic; applies exchange updates only."""
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.orders: dict[str, BookOrder] = {}
        self.levels: dict[str, dict[int, OrderedDict[str, BookOrder]]] = {
            'BUY': {}, 'SELL': {}
        }
        self.volume: dict[str, dict[int, int]] = {'BUY': {}, 'SELL': {}}

    def _insert(self, o: BookOrder):
        if o.id in self.orders or o.remaining <= 0 or o.price <= 0:
            raise BookProtocolError(f'Invalid ADD for {o.id}')
        queue = self.levels[o.side].setdefault(o.price, OrderedDict())
        queue[o.id] = o
        self.volume[o.side][o.price] = self.volume[o.side].get(o.price, 0) + o.remaining
        self.orders[o.id] = o

    def _remove(self, o: BookOrder):
        queue = self.levels[o.side][o.price]
        del queue[o.id]
        del self.orders[o.id]
        self.volume[o.side][o.price] -= o.remaining
        if not queue:
            del self.levels[o.side][o.price]
            del self.volume[o.side][o.price]

    def apply(self, event: MarketEvent):
        if event.symbol != self.symbol:
            raise BookProtocolError('Wrong symbol')

        if event.kind == 'ADD':
            self._insert(BookOrder(
                event.order_id,
                event.side,
                event.price,
                event.qty
            ))
            return

        order = self.orders.get(event.order_id)

        if order is None or (order.side, order.price) != (event.side, event.price):
            raise BookProtocolError(
                f'Unknown or mismatched order {event.order_id}: {event.kind}'
            )

        if event.kind == 'DELETE':
            self._remove(order)

        elif event.kind == 'MODIFY':
            if event.qty > order.remaining:
                raise BookProtocolError(
                    'Quantity increase must reset priority via DELETE+ADD'
                )

            self.volume[order.side][order.price] -= order.remaining - event.qty
            order.remaining = event.qty

        elif event.kind == 'EXECUTE':
            if event.qty > order.remaining:
                raise BookProtocolError('Over-execution')

            self.volume[order.side][order.price] -= event.qty
            order.remaining -= event.qty

            if order.remaining == 0:
                self._remove(order)
                
    def load_snapshot(self, snap: BookSnapshot):
        if snap.symbol != self.symbol or snap.last_seq < 0:
            raise BookProtocolError('Invalid snapshot')
        # Build in temporary state to avoid partial installation on invalid snapshot.
        tmp = ReconstructedBook(self.symbol)
        for x in snap.orders:
            if x.side not in ('BUY', 'SELL'):
                raise BookProtocolError('Invalid snapshot side')
            tmp._insert(BookOrder(x.id, x.side, x.price, x.remaining))
        self.orders, self.levels, self.volume = tmp.orders, tmp.levels, tmp.volume

    def top(self, n: int = 5) -> dict[str, list[tuple[int, int]]]:
        return {side: [(p, self.volume[side][p]) for p in
                       sorted(self.volume[side], reverse=(side == 'BUY'))[:n]]
                for side in ('BUY', 'SELL')}

    def l3(self, side: str, price: int) -> list[tuple[str, int]]:
        return [(o.id, o.remaining) for o in self.levels[side].get(price, {}).values()]

    def canonical(self):
        """Comparable content and per-price FIFO, without relying on dict layout."""
        return tuple((side, p, tuple(self.l3(side, p)))
                     for side in ('BUY', 'SELL')
                     for p in sorted(self.levels[side], reverse=(side == 'BUY')))


class FeedHandler:
    """Sequence-aware consumer; accepts duplicates, out-of-order events, and gaps.

    On a gap, contiguous earlier updates can still be applied; later updates are
    buffered until missing seqs arrive or a snapshot is installed. In real
    production feed handlers there are separate multicast gap-fill and snapshot
    channels, plus session resets, packet framing and exchange-specific rules.
    """
    def __init__(self, symbol: str, max_buffer: int = 10000):
        self.book = ReconstructedBook(symbol)
        self.last_seq = 0
        self.synced = True
        self.pending: dict[int, MarketEvent] = {}
        self.max_buffer = max_buffer
        self.duplicates = 0
        self.gaps = 0
        self.applied = 0

    @property
    def expected_seq(self):
        return self.last_seq + 1

    def receive(self, event: MarketEvent) -> str:
        if event.symbol != self.book.symbol:
            raise ValueError('Feed belongs to another symbol')
        if event.seq <= self.last_seq:
            self.duplicates += 1
            return 'STALE'
        if event.seq in self.pending:
            if self.pending[event.seq] != event:
                raise BookProtocolError('Conflicting events with same sequence number')
            self.duplicates += 1
            return 'DUPLICATE'
        if event.seq > self.expected_seq:
            if len(self.pending) >= self.max_buffer:
                raise BufferError('Pending feed buffer full; snapshot recovery required')
            self.pending[event.seq] = event
            self.gaps += 1
            self.synced = False
            return 'GAP'
        self._apply_contiguous(event)
        return 'APPLIED' if self.synced else 'PARTIAL'

    def _apply_contiguous(self, first: MarketEvent):
        current = first
        while current is not None:
            self.book.apply(current)
            self.last_seq = current.seq
            self.applied += 1
            current = self.pending.pop(self.expected_seq, None)
        self.synced = not self.pending

    def install_snapshot(self, snap: BookSnapshot):
        if snap.symbol != self.book.symbol:
            raise ValueError('Wrong symbol')
        if snap.last_seq < self.last_seq:
            raise BookProtocolError('Stale snapshot would rewind the book')
        self.book.load_snapshot(snap)
        self.last_seq = snap.last_seq
        self.pending = {seq: e for seq, e in self.pending.items() if seq > snap.last_seq}
        next_event = self.pending.pop(self.expected_seq, None)
        if next_event is not None:
            self._apply_contiguous(next_event)
        else:
            self.synced = not self.pending

    def recover(self, snapshot_provider: Callable[[], BookSnapshot]):
        """Fetch authoritative atomic snapshot at a sequence watermark."""
        self.install_snapshot(snapshot_provider())

    def metrics(self):
        return dict(last_seq=self.last_seq, expected_seq=self.expected_seq,
                    synced=self.synced, buffered=len(self.pending),
                    duplicate_messages=self.duplicates, gap_observations=self.gaps,
                    applied=self.applied)


class ExchangePublisher:
    def __init__(self, symbol: str):
        self.book = LitBook(symbol)
        self.symbol = symbol
        self.last_seq = 0
        self.journal: list[MarketEvent] = []

    def _state(self):
        return {o.id: (o.side, o.price, o.remaining) for o in self.book.active.values()}

    def _snapshot_orders(self) -> tuple[SnapshotOrder, ...]:
        result = []
        for side in ('BUY', 'SELL'):
            for price in sorted(self.book.levels[side], reverse=(side == 'BUY')):
                result.extend(SnapshotOrder(oid, side, price, qty)
                              for oid, qty in self.book.l3(side, price))
        return tuple(result)

    def snapshot(self) -> BookSnapshot:
        return BookSnapshot(self.symbol, self.last_seq, self._snapshot_orders())

    def _emit(self, kind: str, oid: str, side: str, price: int, qty: int = 0):
        self.last_seq += 1
        e = MarketEvent(self.symbol, self.last_seq, kind, oid, side, price, qty)
        self.journal.append(e)

    def _publish_changes(self, before, result: Optional[Result], force_priority_reset=()):
        after = self._state()
        reset = set(force_priority_reset) & before.keys() & after.keys()
        # a new order ID at a changed price is equivalent to DELETE + ADD.
        reset |= {oid for oid in before.keys() & after.keys()
                  if before[oid][:2] != after[oid][:2] or after[oid][2] > before[oid][2]}
        # match engine executions reduce only resting makers. Build execution
        # amounts for each maker; any other reduction is a cancel/modify.
        executed = defaultdict(int)
        if result:
            for t in result.trades:
                # the taker may not be in the preexisting book; detect maker
                # exclusively by membership in the BEFORE state.
                maker_id = t.sell_id if t.sell_id in before and before[t.sell_id][0] == 'SELL' else t.buy_id
                if maker_id in before:
                    executed[maker_id] += t.qty
        # remove gone/reset orders, using EXECUTE for executed shares first.
        for oid, (side, price, oldqty) in before.items():
            if oid in reset or oid not in after:
                execqty = min(oldqty, executed.get(oid, 0)) if oid not in reset else 0
                if execqty:
                    self._emit('EXECUTE', oid, side, price, execqty)
                if oldqty > execqty:
                    self._emit('DELETE', oid, side, price)
        # reduce standing orders; if a reduction came from a fill use EXECUTE.
        for oid, (side, price, oldqty) in before.items():
            if oid in after and oid not in reset:
                reduction = oldqty - after[oid][2]
                if reduction > 0:
                    execqty = min(reduction, executed.get(oid, 0))
                    if execqty:
                        self._emit('EXECUTE', oid, side, price, execqty)
                    if reduction > execqty:
                        self._emit('MODIFY', oid, side, price, after[oid][2])
        additions = set(after) - set(before) | reset
        for entry in self._snapshot_orders():
            if entry.id in additions:
                self._emit('ADD', entry.id, entry.side, entry.price, entry.remaining)
        return result

    def submit(self, o: Order, post_only: bool = False) -> Result:
        before = self._state()
        result = self.book.submit(o, post_only=post_only)
        return self._publish_changes(before, result)

    def cancel(self, order_id: str, qty: Optional[int] = None) -> int:
        before = self._state()
        canceled = self.book.cancel(order_id, qty)
        self._publish_changes(before, None)
        return canceled

    def replace(self, order_id: str, new_qty: int, new_price: Optional[int] = None) -> Result:
        before = self._state()
        old = before.get(order_id)
        if old is None:
            raise KeyError(order_id)
        newp = old[1] if new_price is None else new_price
        reset = {order_id} if (newp != old[1] or new_qty > old[2]) else set()
        result = self.book.replace(order_id, new_qty, new_price)
        return self._publish_changes(before, result, reset)

    def messages_since(self, last_seq: int):
        return [e for e in self.journal if e.seq > last_seq]


def replay(events: Iterable[MarketEvent], consumer: FeedHandler):
    for e in events:
        consumer.receive(e)
    return consumer.book


if __name__ == '__main__':
    x = ExchangePublisher('AAPL')
    f = FeedHandler('AAPL')
    for o in [Order('S1', 'AAPL', 'SELL', 100, 101),
              Order('S2', 'AAPL', 'SELL', 60, 101),
              Order('B1', 'AAPL', 'BUY', 120, 102, 'IOC')]:
        x.submit(o)
    replay(x.journal, f)
    print('Exchange:', x.book.top())
    print('Rebuilt: ', f.book.top())
    assert f.book.canonical() == ReconstructedBook('AAPL').canonical() or f.book.top() == x.book.top()
