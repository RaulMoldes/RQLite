# To run with miri:

```bash
MIRIFLAGS="-Zmiri-ignore-leaks" cargo +nightly miri test
```

# TODO LIST:

3. Complete the optimizer with actual statistics information about tables.

4. Implement the executor. The following operators will be implemented at the beginning
 PrioridadMejoraImpactoAltaPredicate Pushdown RuleReduce I/O significativamenteAltaProperty Derivation con estadísticasMejores estimaciones de costoMediaSort/Exchange EnforcersCorrectitud para MergeJoinMediaSelectivity EstimationMejores decisiones de planMediaIndex Selection RuleUso de índices existentesBajaTopN OptimizationMejor rendimiento en LIMITBajaUnique/Not-Null propagationOptimizaciones futuras

7. Make workerpools run on actual threads.
