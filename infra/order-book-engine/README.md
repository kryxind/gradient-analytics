# L3 Market Data Feed Handler & Exchange Book Reconstruction

A Python reference implementation of the **market-data consumer** side of an electronic trading system, integrated with a separate **lit matching engine** (`matching_engines.py`). Standard library only.

## Architecture

```text
Incoming orders --> LitBook (authoritative matching engine)
                           |
                           v
                  ExchangePublisher
                     incremental events
               ADD / MODIFY / EXECUTE / DELETE
                           |
                           v
                FeedHandler (consumer)
                 sequence / gap / buffer
                           |
                           v
               ReconstructedBook (L3)
                 FIFO / L2 depth / BBO
                           ^
                           |
               atomic BookSnapshot recovery
```

The two order books must agree but are **not the same object**. The feed consumer only receives external events/snapshots, never direct reads of matching engine state. The lit engine determines trades; the reconstructed book **does not** match any orders.

## Files

- `matching_engines.py`: existing lit and dark matching engines; unchanged.
- `market_data_feed.py`: normalized event schema, exchange publisher adapter, L3 reconstructor, sequence-aware feed handler, snapshots and replay.
- `demo_market_data.py`: end-to-end happy path, intentionally dropped event, recovery.
- `test_market_data_feed.py`: unit tests including price-time, partial fills, replace, duplicate, reorder, gap, snapshot recovery, and rejected orders.

## Run

```bash
python demo_market_data.py
python -m unittest -v test_market_data_feed.py
```

## Market data semantics

Each `MarketEvent` carries `symbol`, `seq`, `kind`, `order_id`, `side`, `price`, `qty`.

- `ADD`: newly **resting** order, with initial remaining quantity. Queue appends at its price.
- `EXECUTE`: maker execution quantity. Removes order if remaining reaches zero.
- `MODIFY`: **absolute new remaining** quantity, decrease only; retains queue priority.
- `DELETE`: remove the entire remaining order, usually cancellation.
- For a price change or quantity increase, publish `DELETE` followed by `ADD` to reset time priority.
- Taker orders that entirely execute or cancel without resting are not added to the public book.
- All prices are **integer ticks** and all quantities are positive integers.
- Exchange sequence numbers increment for **public book events**, not for every submitted order or trade report.

## Sequence processing

- Starts expecting sequence `1` (or install an authoritative snapshot first).
- Duplicates/stale events are ignored and counted.
- Future sequence numbers are buffered until missing data arrives.
- `synced` becomes `False` while a gap remains unresolved; callers should not treat the live state as current while unsynced.
- An atomic exchange snapshot contains the watermark `last_seq` and FIFO order lists; recovery replaces the book and discards buffered messages `<= snapshot.last_seq`, then replays contiguous newer buffered messages.
- Protocol-invalid transitions raise `BookProtocolError`; a production integration should respond with a full resynchronization workflow and alarm.

## Scope and limitations

This is a **deterministic educational feed simulator**, not a live exchange connector or literal Nasdaq ITCH decoder. A real feed handler needs wire-format parsers, packet framing, feed sessions, channel-specific sequence scopes, gap-fill service, snapshot coordination, fault tolerance, hardening, and profiling. The publisher uses an O(total resting orders) state diff per operation for simplicity; production engines emit incremental events inside native matching transitions. `FeedHandler` is single-threaded and has no persistence or networking. Market-data visibility differs by venue; dark-pool hidden orders are intentionally not published to this lit L3 feed.
