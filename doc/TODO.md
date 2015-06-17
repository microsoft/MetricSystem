# TODO

* Add UI to Server library for same-machine visualization / aggregation
* Release standalone collection service (pending other unreleased changes)
* Modifh Data library to store keys as variable-width values for finalized tables. Width determined by total count of values in a given dimension.
  Initial experimentation with real-world data indicates a potential savings of 25-50% on overall key size.
* Modify client library to use persisted data format when performing aggregation queries (move away from Bond)
* Use JSON.NET for JSON-format REST queries. Bond has some particular quirks which make it 'not really canonical' JSON (e.g. Bond maps of strings use a peculiar format).
* Consider removing Bond?
* Add support for LZ4 compression (it is substantially faster than Deflate without a large penalty to compressed size in experimental data).
* Investigate support for Unix systems. Biggest issue is providing an IPC method faster than HTTP (which is not suitable for extremely high volume data).
* Moar performance improvements!

