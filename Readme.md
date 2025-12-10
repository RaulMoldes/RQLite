# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

4. Implement the executor (order of priority):

    - DDL operators.

    - JOIN

    - Currently ```HAVING``` clauses are not even being parsed. Need to fix this.

    - SORT (PARTIALLY DONE):
        - Currently, an in memory quicksort + limit is implemented and tested.
        - Top N operator + external K way merge sort for large inputs are kind of mid-done but not fully tested. However since these are optimizations we can keep this task in the back log for now.

7. Make workerpools run on actual threads.
