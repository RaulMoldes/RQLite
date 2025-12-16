# To run with miri:



# TODO LIST:

0. Review bplustree tests which are failing after the last refactoring iteration.

0.1. Need to fully implement wal recovery.

0.2. Review storage architeture. I think i could use page zero to store data too as i have done with the WAL.

1. Refactor and properly test (need to create a full test suite) for optimizer planner and executor.

2. Completely mplement the executor (order of priority):

   - Current impl of update/delete does not handle overflow pages properly.

   - JOIN

   - Currently `HAVING` clauses are not even being parsed. Need to fix this.

   - SORT (PARTIALLY DONE):
     - Currently, an in memory quicksort + limit is implemented and tested.
     - We should add external merge sort or some other way of performing sort on disk.
