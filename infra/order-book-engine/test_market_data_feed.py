import unittest
from decimal import Decimal
from matching_engines import Order
from market_data_feed import (
    ExchangePublisher, FeedHandler, MarketEvent, BookProtocolError, replay
)


class FeedTests(unittest.TestCase):
    def setUp(self):
        self.x = ExchangePublisher('AAPL')
        self.f = FeedHandler('AAPL')

    def check_equal(self):
        self.assertTrue(self.f.synced)
        self.assertEqual(self.f.last_seq, self.x.last_seq)
        self.assertEqual(self.f.book.top(100), self.x.book.top(100))
        for side in ('BUY', 'SELL'):
            for p in self.x.book.levels[side]:
                self.assertEqual(self.f.book.l3(side, p), self.x.book.l3(side, p))

    def flush(self):
        replay(self.x.messages_since(self.f.last_seq), self.f)
        self.check_equal()

    def test_lifecycle_price_time_and_tif(self):
        self.x.submit(Order('S1', 'AAPL', 'SELL', 100, 101))
        self.x.submit(Order('S2', 'AAPL', 'SELL', 60, 101))
        self.x.submit(Order('S3', 'AAPL', 'SELL', 80, 102))
        self.flush()
        r = self.x.submit(Order('B1', 'AAPL', 'BUY', 200, 103, 'IOC'))
        self.assertEqual([(t.sell_id, t.qty) for t in r.trades], [('S1',100),('S2',60),('S3',40)])
        self.flush()
        self.assertEqual(self.f.book.l3('SELL', 102), [('S3',40)])
        self.x.cancel('S3', 10)
        self.flush()
        self.assertEqual(self.f.book.l3('SELL', 102), [('S3',30)])
        self.x.cancel('S3')
        self.flush()

    def test_replace_reset_and_reductions(self):
        self.x.submit(Order('B1', 'AAPL', 'BUY', 100, 99))
        self.x.submit(Order('B2', 'AAPL', 'BUY', 200, 99))
        self.flush()
        self.x.replace('B1', 50)
        self.flush()
        self.assertEqual(self.f.book.l3('BUY',99), [('B1',50),('B2',200)])
        self.x.replace('B1', 150)
        self.flush()
        self.assertEqual(self.f.book.l3('BUY',99), [('B2',200),('B1',150)])
        self.x.replace('B1', 100, 98)
        self.flush()
        self.assertEqual(self.f.book.l3('BUY',98), [('B1',100)])

    def test_out_of_order_gap_fill_and_duplicate(self):
        for i in range(1, 5):
            self.x.submit(Order(f'B{i}', 'AAPL', 'BUY', 10, 90+i))
        ev = self.x.journal
        self.f.receive(ev[0])
        self.assertEqual(self.f.receive(ev[2]), 'GAP')
        self.assertFalse(self.f.synced)
        self.assertEqual(self.f.receive(ev[1]), 'APPLIED')
        self.assertTrue(self.f.synced)
        self.f.receive(ev[3])
        self.assertEqual(self.f.receive(ev[3]), 'STALE')
        self.check_equal()

    def test_snapshot_recovery_from_dropped_event(self):
        for i in range(1, 5):
            self.x.submit(Order(f'B{i}', 'AAPL', 'BUY', 10, 90+i))
        self.f.receive(self.x.journal[0])
        self.f.receive(self.x.journal[3])  # seq 2,3 missing
        self.assertFalse(self.f.synced)
        self.assertEqual(self.f.book.top()['BUY'], [(91, 10)])
        self.f.recover(self.x.snapshot)
        self.check_equal()
        self.x.cancel('B4', 3)
        self.flush()

    def test_partial_fill_then_replace_and_market_order(self):
        self.x.submit(Order('S1', 'AAPL', 'SELL', 100, 101))
        self.x.submit(Order('S2', 'AAPL', 'SELL', 90, 102))
        self.flush()
        self.x.submit(Order('T1', 'AAPL', 'BUY', 40, None, 'IOC'))
        self.flush()
        self.x.replace('S1', 120)  # quantity increase resets priority
        self.flush()
        self.x.submit(Order('T2', 'AAPL', 'BUY', 150, 103, 'IOC'))
        self.flush()

    def test_invalid_update_rejected(self):
        with self.assertRaises(BookProtocolError):
            self.f.receive(MarketEvent('AAPL', 1, 'EXECUTE', 'unknown', 'BUY', 100, 10))
        self.assertEqual(self.f.last_seq, 0)

    def test_fok_post_only_no_public_update(self):
        self.x.submit(Order('S1', 'AAPL', 'SELL', 50, 101))
        self.flush()
        start = self.x.last_seq
        self.x.submit(Order('B1', 'AAPL', 'BUY', 100, 101, 'FOK'))
        self.x.submit(Order('B2', 'AAPL', 'BUY', 20, 101), post_only=True)
        self.assertEqual(start, self.x.last_seq)
        self.flush()

    def test_snapshot_fifo(self):
        self.x.submit(Order('B1', 'AAPL', 'BUY', 20, 100))
        self.x.submit(Order('B2', 'AAPL', 'BUY', 30, 100))
        self.f.install_snapshot(self.x.snapshot())
        self.check_equal()
        self.assertEqual(self.f.book.l3('BUY',100), [('B1',20), ('B2',30)])


if __name__ == '__main__':
    unittest.main(verbosity=2)
