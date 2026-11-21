# Shopping Sessions

DuckDB implementation for the Shopping Sessions benchmark. The container reads
`DATA_DIR` (default `/data`) and writes the two required Parquet directories to
`OUTPUT_DIR` (default `/output`). It uses `SCRATCH_DIR`, `NCPU`, and `MEM_GB`
when provided by the runner.

Build for the scorer with:

```sh
docker buildx build --platform linux/amd64 -t shopping-sessions .
```

The pipeline strictly parses the one-line CSV dialect, resolves linked IDs,
deduplicates events before validation, sessionizes surviving activity, and
calculates click credit and seven-day campaign assists.

