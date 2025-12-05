# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

1. Moved the datatype to bytemuck. Maybe we can do something with tuples too.

2. Test that statistics storage does not break anything.
    2.1 Review statistics tests (some of them are flaky.)
    2.2 Improve computation efficiency. Currently it is a shit because we are copying the payload at each iteration.

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
