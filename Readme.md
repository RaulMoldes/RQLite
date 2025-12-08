# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

4. Implement the executor.
    - Seq Scan Done.
    - Filter Done.
    - Project Done.
    - DDL + INSERT DELETE UPDATE ETC (TODO).
    - GROUPBY TODO.
    - JOIN TODO.

5. CLEAN UP EXPRESSION EVALUATION.
7. Make workerpools run on actual threads.
