# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

3. Complete the optimizer with actual statistics information about tables.

4. Implement the executor. The following operators will be implemented at the beginning
    - SeqScan
    - IndexScan
    - Filter
    - Sort
    - GroupBy
    - Join (nested loop)
    - All dml and ddl.

7. Make workerpools run on actual threads.
