# DuckDB Paimon Extension

This extension enables [DuckDB](https://duckdb.org/) to read and query [Apache Paimon](https://paimon.apache.org/) format data directly.

## About Apache Paimon

[Apache Paimon](https://paimon.apache.org/) is a lake format that enables building a Realtime Lakehouse Architecture with Flink and Spark for both streaming and batch operations. It innovatively combines lake format and LSM structure, bringing realtime streaming updates into the lake architecture.

## Implementation

This extension is built on top of [paimon-cpp](https://github.com/alibaba/paimon-cpp), an open-source C++ library that provides native access to Paimon format data.

## Features

- ✅ Read Paimon table data
- ✅ Projection pushdown optimization
- ✅ Multiple file format support:
- ✅ Local / OSS file system support

## Getting started

Clone the repository:

```shell
git clone --recurse-submodules https://github.com/polardb/duckdb-paimon.git
cd duckdb-paimon
```

Note that `--recurse-submodules` will ensure DuckDB and paimon-cpp are pulled which are required to build the extension.

## Building

```shell
GEN=ninja make
```

## Running the extension

To run the extension code, simply start the shell with `./build/release/duckdb`. This shell will have the extension pre-loaded.

Now we can use the features from the extension directly in DuckDB. The extension provides a `paimon_scan` function that reads Paimon table data:

```sql
D SELECT * FROM paimon_scan('./data/testdb.db/testtbl');
┌─────────┬───────┬───────┬────────┐
│   f0    │  f1   │  f2   │   f3   │
│ varchar │ int32 │ int32 │ double │
├─────────┼───────┼───────┼────────┤
│ Alice   │     1 │     0 │   11.0 │
│ Bob     │     1 │     1 │   12.1 │
│ Cathy   │     1 │     2 │   13.2 │
│ David   │     2 │     0 │   21.0 │
│ Eve     │     2 │     1 │   22.1 │
│ Frank   │     2 │     2 │   23.2 │
│ Grace   │     3 │     0 │   31.0 │
│ Henry   │     3 │     1 │   32.1 │
│ Iris    │     3 │     2 │   33.2 │
└─────────┴───────┴───────┴────────┘
```

The extension supports projection pushdown for efficient column selection.

## Running the tests

```shell
make test
```
