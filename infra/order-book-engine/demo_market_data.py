from matching_engines import Order
from market_data_feed import ExchangePublisher, FeedHandler, replay

exchange = ExchangePublisher('AAPL')
feed = FeedHandler('AAPL')

# two orders at one ask level, and one more expensive ask
exchange.submit(Order('S1', 'AAPL', 'SELL', 100, 101))
exchange.submit(Order('S2', 'AAPL', 'SELL', 60, 101))
exchange.submit(Order('S3', 'AAPL', 'SELL', 80, 102))

# aggressive order matches price first, FIFO within the same level
result = exchange.submit(Order('B1', 'AAPL', 'BUY', 200, 103, 'IOC'))
print('Trades:', [(t.buy_id, t.sell_id, t.qty, str(t.price)) for t in result.trades])

# the feed sees only outbound exchange events, never the matching-book internals
print('\nExchange event feed:')
for event in exchange.journal:
    print(event)
replay(exchange.journal, feed)
print('\nAuthoritative matching book:', exchange.book.top())
print('Reconstructed feed book: ', feed.book.top())
print('Remaining FIFO at ask 102:', feed.book.l3('SELL', 102))
assert feed.book.top() == exchange.book.top()

# gap + snapshot recovery example
exchange.cancel('S3', 10)
exchange.submit(Order('B2', 'AAPL', 'BUY', 50, 99))
exchange.submit(Order('B3', 'AAPL', 'BUY', 30, 98))
for e in exchange.messages_since(feed.last_seq)[1:]:
    print('\nReceive:', e.seq, feed.receive(e))
print('Before recovery:', feed.metrics())
feed.recover(exchange.snapshot)
print('After recovery: ', feed.metrics())
assert feed.book.top() == exchange.book.top()
