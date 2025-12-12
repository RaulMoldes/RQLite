# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

1. Implement the executor (order of priority):

   - JOIN

   - Currently `HAVING` clauses are not even being parsed. Need to fix this.

   - SORT (PARTIALLY DONE):
     - Currently, an in memory quicksort + limit is implemented and tested.
     - We should add external merge sort or some other way of performing sort on disk.


2. Need to create a full test suite for optimizer planner and executor.
