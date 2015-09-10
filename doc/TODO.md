# TODO

1. Add UI to Server library for same-machine visualization / aggregation
2. Add support for LZ4 compression (it is substantially faster than Deflate without a large penalty to compressed size in experimental data).
3. Release standalone collection service (pending other unreleased changes)
4. Updates to standalone service to allow collecting/counting dynamic ETW or log events
5. Modify Data library to store keys as variable-width values for finalized tables. Width determined by total count of values in a given dimension.
   Initial experimentation with real-world data indicates a potential savings of 25-50% on overall key size.
6. Modify client library to use persisted data format when performing aggregation queries (move away from Bond)
7. Use JSON.NET for JSON-format REST queries. Bond has some particular quirks which make it 'not really canonical' JSON (e.g. Bond maps of strings use a peculiar format).
8. Enhanced query support:
   * 'top N' from aggregator.
   * Joins across multiple counters.
   * More sophisticated matching and filtering for individual dimension values.
9. Investigate support for Unix systems. Biggest issue is providing an IPC method faster than HTTP (which is not suitable for extremely high volume data).
10. Consider removing Bond?
11. Moar performance improvements!
    * Can we use something like RocksDB? Is it faster than the current impl?


