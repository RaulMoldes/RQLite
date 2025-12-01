# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

1. If we do tuple level [MVCC] we do not need such a tight concerns at the lock manager level. Change it to match expectations. When using [MVCC], the only conflict is the W-W conflict, so R should never be blocked by W and vice-versa.


2. Improve the bplustree scans.

3. Complete the planner and executor.
