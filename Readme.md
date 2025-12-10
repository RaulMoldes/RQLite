# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

4. Implement the executor (order of priority):

    - DDL operators (almost).
        - Integrate with executor and add transaction statements.
        - Implement multi column indexes.
        - Implement check constraints (can wait)


    - JOIN

    - Currently ```HAVING``` clauses are not even being parsed. Need to fix this.

    - SORT (PARTIALLY DONE):
        - Currently, an in memory quicksort + limit is implemented and tested.
        - We should add external merge sort or some other way of performing sort on disk.

7. Make workerpools run on actual threads.
