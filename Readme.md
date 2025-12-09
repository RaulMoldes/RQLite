# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:


3. We need to think on how to handle traversal and lock acquisition on updates. Traversing the entire tree once for each row seems inefficient and we should be reusing the locks already acquired by the scanner.

4. Implement the executor.
    - GROUPBY TODO.
    - JOIN TODO.

7. Make workerpools run on actual threads.

8. Complete the rest of functions.
