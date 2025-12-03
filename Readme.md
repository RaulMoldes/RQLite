# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

1. Refactor the planner.

2. Implement the executor.

3. Make workerpools run on actual threads.

4. [LESS IMPORTANT]: Refactor and review analyzer, preparator.
