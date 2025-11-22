# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```
# TODO:
To support MVCC:

1. Tuples must be a linked list of versioned values. Current tuple Layout is the following:
[KEYS][LAST VERSION NB][NULL BITMAP][VALUES .....]

2. The idea is to transform it to this way:
[KEYS][LAST VERSION NB][NULL BITMAP][VALUES LAST V.......][VALUES LAST V - 1....] etc
