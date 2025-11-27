# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

1. Add a way to automatically create tables with indexes. On most tests ,tables are still created with random ids instead of using the proper [ROWID].

2. If we do tuple level [MVCC] we do not need such a tight concerns at the lock manager level. Change it to match expectations. When using [MVCC], the only conflict is the W-W conflict, so R should never be blocked by W and vice-versa.

3. The [run_transaction_async] function does not perform any form of Write Ahead Logging. The basic structure setup for write ahead logging is done but we need to also control it at the transaction level in some way.

