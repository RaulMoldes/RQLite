# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

0. Refactor UPDATE, INSERT and DELETE operators.
0.1. Refactor workerpools to decouple from transaction and make them be proper thread pools.
0.2 Add a complete integration of the Write Ahead Log in the DML/DDL executors.



1. Implement the executor (order of priority):

    - JOIN

    - Currently ```HAVING``` clauses are not even being parsed. Need to fix this.

    - SORT (PARTIALLY DONE):
        - Currently, an in memory quicksort + limit is implemented and tested.
        - We should add external merge sort or some other way of performing sort on disk.
