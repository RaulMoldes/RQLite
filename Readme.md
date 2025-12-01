# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

1. Improve the bplustree scans.

2. Complete the planner and executor.

3. Make workerpools run on actual threads.

. We need to see how to do with reverse iterator on the btree because when starting at pos zero it currently returns 1 key. We should return no keys instead
