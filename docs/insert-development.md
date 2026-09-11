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

Each concrete PageSink obtains a durable, transaction-local writer assignment
through `AllocateWriter`. Trino 466 derives its PageSinkId from the task ID, which
can be shared by multiple writer operators in that task. The task ID is retained
for tracing, while each physical sink has an independent ingestion sequence space.
An allocation request token returns the same assignment after retransmission or
coordinator restart. This does not change rowId allocation in pixels-index.

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

### Native runtime

For a jemalloc-enabled Retina build, the script preloads the matching allocator
into the backend JVM only. The matching Pixels build uses jemalloc's default
initial-exec TLS model for process-start interposition. Trino and shell helper
processes do not receive a forced allocator override from the script.

Before opening the SQL fixture, the backend performs 1,000 native visibility
allocation, snapshot-read, and release cycles. Verification also requires normal
backend cleanup and a zero exit code; a SQL success marker cannot hide a later
native crash. JVM startup diagnostics and fatal-error logs are retained.

### What is real

- Trino 466 coordinator and two worker servers, executing SQL through the actual connector.
- TCP/gRPC clients and service adapters, payload journal, persisted decisions and Abort.
- Retina MemTables, native RGVisibility, SQLite MainIndex, object spills, PixelsWriter and readers.

The external metadata catalog, node directory, and identity-allocation sources
are deterministic fixtures. The harness uses one Retina owner on the CI host;
Trino's testing servers are hosted by DistributedQueryRunner. It does not fake the
data PageSource, PageSink, transaction installation, or SQL result. This is an
integration test, not a production multi-host, permanent-volume-loss, or GC certification.

### Assertions

The driver verifies single/multi-row VALUES, duplicate preservation, NULLs,
reordered/omitted columns, zero-row INSERT SELECT, a 500-row generated input,
a 500-row same-table INSERT SELECT, concurrent SQL commits, and file/buffer handoff.
It also fails a statement after rows have reached private server staging and
checks a durable Abort and unchanged public row count.

Do not treat successful QueryRunner startup, a Maven compile, or staging-only
contracts as this end-to-end result. The script requires the SQL success marker
and a zero process exit status after backend shutdown.

### Verified CI result

[Full SQL INSERT run 34571751533](https://github.com/AntiO2/pixels-trino/actions/runs/34571751533)
passed on September 11, 2026, with these exact source versions:

| Repository | Tested commit |
|---|---|
| AntiO2/pixels | `1d536d150e6ef47f7c3312514ede3f21023a42d8` |
| AntiO2/pixels-trino | `91297500943dc5fd34c3bcee8cc7ec043bc6b1aa` |

The SQL driver reported:

```text
FULL_SQL_INSERT_E2E_PASS checks=20 rows=1008 trinoWorkers=2 appendRPCs=53 bufferReadRPCs=11 fileVisibilityRPCs=3 pixelsFiles=1 privateAbortedRows=480
```

The file counter above was captured at the assertion point, before further
background materialization and fixture shutdown. The final public row count was
1,008; the 480 rows accepted from the failed statement stayed invisible.

The same job passed seven writer-allocation regression tests and the native
1,000-cycle preflight, then emitted `PIXELS_SQL_FIXTURE_STOPPED` and process status 0.
Its `sql-insert-e2e-evidence` artifact contains exact commit IDs, build logs,
Surefire results, SQL/backend logs, and counters. The workflow fails on build,
assertion, readiness, or process-lifecycle errors.

## Focused regression tests

`tools/verify-ingest-contract.sh` still exercises the dependency-free writer and
journal contracts. `TestIngestWriterAllocation` covers same-task independent
writers, durable retransmission, concurrent allocation, content conflicts,
capacity bounds, and sealing/Abort fences.

`TestBufferSnapshotRead` covers ordered prefetch consumption,
segment-specific visibility, the final batch, owned read buffers and corrupt
object propagation. `TestPixelsIngestStorage` checks actual storage installation,
existing MainIndex mappings and retry around catalog publication.

The shared protocol reserves `DELETE_ROWS`. Later MergeSink work can use existing
rowIds and the same transaction protocol; it is not implemented by these tests.

## Remaining milestones

- [x] Full SQL INSERT integration verification through the real connector and Retina data path.
- [ ] Normal daemon bootstrap and production cutover integration.
- [ ] Transaction checkpoint reclamation and rewriting GC coordination.
- [ ] UPDATE/DELETE MergeSink and row-conflict validation.
- [ ] Execution-attempt retry protocol and replicated durability.
