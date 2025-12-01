# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

0. Review analyzer failing tests. Probably refactor analyzer and preparator.

1. Improve the bplustree scans.

2. Complete the planner and executor.

3. Make workerpools run on actual threads.
