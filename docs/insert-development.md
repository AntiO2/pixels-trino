# Transactional INSERT: implementation and SQL verification

Related: pixelsdb/pixels-trino#180.

## Implemented path

The connector registers `PixelsPageSinkProvider`, `PixelsInsertPageSink`, and the
`beginInsert`/`finishInsert` callbacks. `PixelsPageEncoder` converts native Trino
Blocks to bounded canonical scalar batches. `PixelsMutationWriter` sends them
over the ingestion RPC adapter with stable stream identities and exact seals.

The coordinator prepares the complete statement, persists an authoritative
COMMIT/ABORT decision, and returns successful commit only after installation and
publication. Installation uses the existing Retina buffers and pixels-index
mappings. No business primary key is required; equal input rows remain distinct.

The implementation is experimental and defaults to `insert.enabled=false`.
It currently uses a LOCAL, fixed-topology transaction domain with retained
journals/plans. Execution-attempt retries, UPDATE/DELETE, production cutover,
rewriting GC, and checkpoint reclamation are not enabled by this milestone.
The SQL harness explicitly starts the new service adapters; integration into the
normal daemon startup/cutover sequence remains a deployment task.

## Full SQL end-to-end test

With JDK 23 on PATH and `PIXELS_HOME` pointing to a built Linux Pixels runtime
(including its native Retina libraries), run from this checkout:

```sh
bash tools/verify-sql-insert.sh /path/to/matching/pixels
```

The script builds both checkouts, resolves the Trino 466 engine graph separately,
starts the backend in a different process, and loads the connector with Trino's
actual `PluginClassLoader`. Plugin runtime dependencies are not removed or exposed
to the engine's Jersey classpath. The plugin directory uses deployment-style JAR
filename ordering, including the existing Spike dependency.

After explicitly building the matching source versions, the build can be skipped:

```sh
SQL_E2E_SKIP_BUILD=1 bash tools/verify-sql-insert.sh /path/to/matching/pixels
```

Additional arguments are forwarded to Maven, for example `-o` or
`-Dmaven.repo.local=/path/to/repository`. `MVN` selects the Maven executable.
`SQL_E2E_WORK_DIR` can select a fresh output directory. Logs, counters, resolved
classpaths and the process exit code are retained there.

### What is real

- Trino 466 coordinator and two worker servers, executing SQL through the actual connector.
- TCP/gRPC clients and service adapters, payload journal, persisted decisions and Abort.
- Retina MemTables, native RGVisibility, SQLite MainIndex, object spills, PixelsWriter and readers.

The external metadata catalog, node directory, and identity-allocation sources
are deterministic fixtures. The harness does not fake the data PageSource,
PageSink, transaction installation, or SQL result. This is an integration test,
not a production-cluster, permanent-volume-loss, or GC certification.

### Assertions

The driver verifies single/multi-row VALUES, duplicate preservation, NULLs,
reordered/omitted columns, zero-row INSERT SELECT, a 500-row generated input,
a 500-row same-table INSERT SELECT, concurrent SQL commits, and file/buffer handoff.
It also fails a statement after rows have reached private server staging and
checks a durable Abort and unchanged public row count. Success emits:

```text
FULL_SQL_INSERT_E2E_PASS ... rows=1008 trinoWorkers=2 ... privateAbortedRows=480 ...
```

Do not treat successful QueryRunner startup, a Maven compile, or staging-only
contracts as this end-to-end result. The script requires the SQL success marker
and a zero process exit status.

## Focused regression tests

`tools/verify-ingest-contract.sh` still exercises the dependency-free writer and
journal contracts. `TestBufferSnapshotRead` covers ordered prefetch consumption,
segment-specific visibility, the final batch, owned read buffers and corrupt
object propagation. `TestPixelsIngestStorage` checks actual storage installation,
existing MainIndex mappings and retry around catalog publication.

The shared protocol reserves `DELETE_ROWS`. Later MergeSink work can use existing
rowIds and the same transaction protocol; it is not implemented by these tests.
