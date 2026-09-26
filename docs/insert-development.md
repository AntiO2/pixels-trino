# Transactional INSERT: implementation, lifecycle and verification

Related: pixelsdb/pixels-trino#180 and pixelsdb/pixels-trino#179.

## Implemented scope

The connector supports autocommit and explicit multi-statement INSERT VALUES
and INSERT ... SELECT. One explicit transaction may write multiple tables in
the same Pixels catalog and publication domain, and each statement reads its
fixed public snapshot plus prior completed private writes. Trino Pages are encoded into bounded column batches,
routed through authenticated ingestion RPCs and staged in a transaction-private
Retina WAL. The durable coordinator records COMMIT or ABORT before installation.
VISIBLE commit acknowledgement waits until installed rows reach the published
read timestamp. DURABLE acknowledgement returns after the recoverable COMMIT
decision and installation continues in the backend. Applications can close the
current committed prefix and wait for visibility with:

~~~sql
CALL pixels.system.flush_visible_barrier(timeout_seconds => 30);
~~~

The barrier captures its committed boundary when called and does not wait for
later transactions or global idleness. `retina.ingest.file.max.delay.ms`
controls how long an underfilled FILE tail may remain open during normal
background aggregation. The barrier is different: it closes every tail needed
by the captured boundary immediately. Bulk loaders should therefore use
FILE+DURABLE, allow transactions to aggregate, and issue one barrier after the
last accepted transaction instead of one barrier per transaction.

BUFFERED installation reuses existing Retina write buffers. FILE installation
writes bounded row batches directly through PixelsWriter and never exposes a
public MemTable copy. Both representations reuse pixels-index:

- MainIndex: rowId -> RowLocation;
- supported primary and non-unique SinglePointIndex membership;
- existing row-id allocation.

A business primary key is optional. Keyless tables preserve duplicate rows.
Request/writer idempotency never deduplicates equal user values. Each concrete
PageSink obtains a durable transaction-local writer ID through AllocateWriter;
the Trino task ID remains trace metadata, not an ingestion stream identity.

This is a fixed-topology, single-owner, LOCAL durable implementation. It does
not provide replicated coordinator/WAL state, HA failover, task-attempt retry,
UPDATE/DELETE, or unique secondary constraints.

## Normal daemon lifecycle

When retina.ingest.enabled=true, normal TransServer and RetinaServer instances
register coordinator and participant services. The coordinator recovers its
locked state volume before accepting writes. Retina recovers the recovery
checkpoint, Storage-GC WAL, installation plans and mutation journal; it reports
READY only after participant recovery completes.
Coordinator-to-participant RPCs wait for a recovering participant within the
configured transaction deadline, so an early reconciliation connection failure
cannot leak as the first post-READY Prepare failure.

Startup fails closed for missing/corrupt confirmed state, a missing WAL
generation, inconsistent recovery coverage, a state-volume ownership conflict,
timestamp regression, invalid credentials, or unavailable required metadata.
A failed server cannot be restarted inside the same daemon process.

Shutdown first stops RPC admission, then closes the participant, background
GC/checkpoint work, shared write buffers and native visibility. Non-empty
buffers are materialized as Pixels files. Transaction WAL/plan data remains
unless its normal checkpoint handoff already made it reclaimable.

### Required configuration

Provision these settings while the feature remains disabled:

~~~properties
retina.enable=true
retina.ingest.enabled=false
retina.ingest.auth.secret.file=/absolute/secret/path
retina.ingest.coordinator.state.dir=/absolute/state/coordinator
retina.ingest.participant.plan.dir=/absolute/state/plans
retina.ingest.participant.wal.dir=/absolute/state/wal
retina.ingest.cutover.baseline.timestamp=0
retina.ingest.write.representation=BUFFERED
retina.ingest.commit.ack=VISIBLE
retina.ingest.file.target.rows=1000000
retina.ingest.file.max.bytes=536870912
retina.ingest.file.max.delay.ms=30000
retina.ingest.file.pixel.stride=10000
retina.ingest.coordinator.compaction.bytes=16777216
retina.ingest.max.transactions=10000
retina.ingest.terminal.retention.ms=86400000
retina.ingest.terminal.max.transactions=100000
retina.ingest.wal.segment.bytes=67108864
retina.ingest.wal.max.bytes=4294967296
retina.ingest.wal.max.records=10000000
retina.ingest.wal.group.commit.delay.micros=200
retina.ingest.wal.read.cache.max.bytes=67108864
retina.ingest.coordinator.group.commit.delay.micros=200
~~~

The three state directories must be absolute, pairwise non-overlapping,
persistent local volumes owned by exactly one live process. The credential must
contain 24–4096 visible ASCII bytes after trimming; deploy it with owner-only
permissions. RPC transport is plaintext, so use a trusted network or external
TLS boundary.

The WAL read cache is private to the Retina owner and bounded by bytes; it
avoids rereading recently appended batches during installation and private
reads. A sealed batch is served only after its WAL seal is durable. Evicted
batches and batches recovered after restart are read from the WAL.

The coordinator stores transaction changes in a checksummed incremental log
and periodically replaces it with an atomic checkpoint at
`retina.ingest.coordinator.compaction.bytes`. Confirmed corrupt records fail
startup; an incomplete trailing record is discarded during recovery.
The two bounded group-commit delays let concurrent seal and decision writers
share one local synchronization. A zero value disables the wait without
weakening the synchronized COMMIT boundary.

Start the coordinator role before the Retina role. Do not route INSERTs until
Retina reports:

~~~text
Retina service and transactional ingestion are ready
~~~

## Supported offline cutover

Cutover is operator controlled. Never apply it to a live production table
without a backup and rollback window.

1. Disable new Trino writes and drain all legacy CDC/LOAD/Retina writers for
   tables being switched. Stop old producers so no process retains a cached
   transaction-ID segment.
2. Wait for legacy write transactions to terminate and flush existing buffers.
   Verify catalog/files and indexes.
3. Run the existing Retina recovery-checkpoint cycle and record a fixed
   baseline B at least as high as both the legacy published timestamp and
   checkpoint applied timestamp. Pin old files and GC history through B.
4. With writers and transaction daemons stopped, advance the existing etcd
   trans_id value so the first new value is strictly greater than B. Do this as
   a guarded/CAS administrative change; never lower or reuse the key.
5. Set retina.ingest.cutover.baseline.timestamp=B and enable
   retina.ingest.enabled=true on coordinator and Retina. Start coordinator,
   then Retina. Startup rejects a baseline below the legacy publication domain
   or an allocator value at/below the recovered floor.
6. Verify old rows with a pinned snapshot and verify one new INSERT before
   reopening traffic. Keep the backup until a post-cutover checkpoint and
   GC-safe point complete.

With the gate enabled, legacy non-read-only transaction begins and the legacy
Retina updateRecord/streaming mutation RPC are rejected. Disable the gate only
for an explicit offline rollback after stopping new writers; never run both
write paths concurrently.

## Checkpoint and reclamation

COMMIT, PUBLISHED, file close and age are not reclamation conditions. The
durable handoff is:

1. A recovery checkpoint snapshots visibility, file/buffer coverage and pending
   segment replay boundaries, then atomically publishes its pointer.
2. The installer verifies each installed row in durable REGULAR storage,
   MainIndex, supported business indexes and checkpoint coverage.
3. Full batch plans are atomically replaced by compact transaction checkpoints
   containing table identity, commit timestamp, row-id ranges and covered files.
4. The participant durably writes CHECKPOINTED and evicts the transaction's
   cached payload. Background reclamation groups retired transactions and copies
   live records into a new generation once retired payload bytes reach live
   payload bytes. Its pointer is durable before an old WAL is deleted. A restart
   between the fence and reclamation preserves both the fence and live inputs.
5. The coordinator retires PUBLISHED/ABORTED decisions only after every owner
   acknowledges checkpoint/discard.

Late requests remain fenced by retained terminal records. Expired records are
compacted to a durable retired transaction-ID high-water mark, so removed IDs
cannot be recreated. Retention must be at least the transaction lease and
should cover the maximum expected client delay.

MainIndex durability remains the existing SQLite database; it is not copied
into the recovery-checkpoint body. Transaction checkpoints verify that database
before private WAL/plan removal. Storage-GC relocation retains a separate WAL
until the replacement file and recovery checkpoint assume responsibility.

## Rewriting Storage GC

Storage GC continues to use StorageGarbageCollector, existing visibility and
indexes. Rewrites preserve surviving row IDs while relocating MainIndex
positions. Business-index membership therefore remains exact, including
duplicate members in non-unique indexes and keyless tables.

The Storage-GC WAL records old locations before relocation. Recovery rolls back
an unpublished swap or completes a committed swap. Publication is serialized
with process-local read pins; a pinned ReadView retains its physical coverage.
Retired files are not deleted while a read pin or incomplete GC WAL task depends
on them. Exact old-file MainIndex entries are removed only during retirement.

## Verification

Pixels requires JDK 8; Trino 466 requires JDK 23. Build native code from the
matching Pixels source. Never preload jemalloc into Trino/helper processes.

### Normal production service classes

With JDK 8 and pinned etcd 3.5:

~~~sh
PIXELS_HOME=/path/to/matching/runtime \
ETCD_BIN=/path/to/etcd \
bash tools/verify-ingest-daemon.sh
~~~

This starts isolated real etcd and production TransServer, RetinaServer and
ServerContainer in two independent backend JVMs over one locked state volume.
The first process commits 64 rows, commits one still-buffered row, and commits
two FILE transactions into one shared output. It waits for a
recovery checkpoint, verifies that the first transaction's full plan and WAL
payload were physically replaced by compact checkpoint/fence state, checks the
legacy write fence, and shuts down cleanly. Before starting services, the second
process opens the same catalog, plan and WAL state and validates the handoff; it
then recovers the PUBLISHED decisions, reaches READY, shuts down, and reads the
Pixels files directly to prove an exact 73-row multiset without replay duplicates.
It finally corrupts a copied committed-decision state and removes the published
checkpoint body in turn; separate daemon attempts must fail before READY with an
actionable checksum or missing-body error. An offline cutover phase first proves
that a transaction allocator below the configured baseline is rejected. It then
advances the isolated etcd ID domain while services are stopped, preserves all
73 old physical rows, commits one new row above baseline 1000000000, and rechecks
the legacy mutation fence. Catalog persistence and topology discovery remain
fixtures.

~~~text
PIXELS_NORMAL_INGEST_DAEMON_PHASE1_PASS rows=73 sharedFileTransactions=2 checkpointedTransaction=5
PIXELS_NORMAL_INGEST_DAEMON_PASS rows=73 pixelsFiles=3 services=TransServer,RetinaServer checkpointRestart=2 sharedFileTransactions=2
PIXELS_NORMAL_INGEST_CUTOVER_PASS oldRows=73 totalRows=74 baseline=1000000000 commitTimestamp=1000000002 legacyFence=1
PIXELS_NORMAL_INGEST_FAIL_CLOSED_PASS corruptDecision=1 missingCheckpoint=1 allocatorFloor=1
~~~

### Full SQL end-to-end

With JDK 23:

~~~sh
PIXELS_HOME=/path/to/matching/runtime \
bash tools/verify-sql-insert.sh /path/to/matching/pixels
~~~

Use SQL_E2E_SKIP_BUILD=1 only after both projects and native runtime were rebuilt
from exact checked-out sources. SQL_E2E_WORK_DIR selects the evidence directory.
`SQL_E2E_ENGINE_HEAP` (default `3g`) and `SQL_E2E_BACKEND_HEAP` (default `1g`)
size the independent Trino and Retina JVMs. Large-file benchmarks must budget
for concurrent file readers as well as ingestion; the SF10 profile uses `8g`
for the engine and `6g` for Retina.

The harness uses real Trino 466 coordinator/two-worker servers, actual plugin
classloader and connector, TCP/gRPC, decisions/journals, Retina MemTables,
native visibility, SQLite MainIndex and Pixels readers/writers. Catalog, node
directory and external identity allocation are fixtures. It is one-host,
one-Retina-owner integration, not HA certification.

Assertions cover VALUES, duplicate preservation, NULLs, reordered/omitted
columns, zero-row and 500-row INSERT SELECT, same-table INSERT SELECT,
concurrent commits, buffer/file handoff and a 480-row private staging failure
followed by durable Abort. JDBC tests keep one connection across START
TRANSACTION, verify multi-statement/multi-table private reads and atomic
publication, exercise rollback and rejected transaction modes, and run once
with VISIBLE acknowledgement and once with DURABLE plus an explicit visibility
barrier.

The September 11, 2026 baseline
[run 34571751533](https://github.com/AntiO2/pixels-trino/actions/runs/34571751533)
used Pixels 1d536d150e6ef47f7c3312514ede3f21023a42d8 and Pixels-Trino
91297500943dc5fd34c3bcee8cc7ec043bc6b1aa:

~~~text
FULL_SQL_INSERT_E2E_PASS checks=20 rows=1008 trinoWorkers=2 appendRPCs=53 bufferReadRPCs=11 fileVisibilityRPCs=3 pixelsFiles=1 privateAbortedRows=480
~~~

### Focused lifecycle/GC tests

From Pixels:

~~~sh
bash tools/verify-ingest-contract.sh
bash tools/verify-ingest-gc.sh
~~~

TestPixelsIngestStorage uses real Pixels files, native visibility and SQLite to
verify read-pin rollback, stable row relocation, physical retirement,
checkpointed GC-WAL deletion and restart of journal/installer/index objects.
Coordinator tests cover terminal-fence compaction/high-water rejection; journal
tests inject crashes around WAL generation publication. TestRecoveryCheckpoint
covers 29 body-codec, pointer-publication, replacement and corrupt/missing-state
cases; it is part of the Full SQL CI lifecycle regression set.

### TPC-H and TPC-DS INSERT coverage

Run the representative two-worker latency benchmark in an isolated environment:

~~~sh
PIXELS_HOME=/path/to/matching/runtime \
bash tools/benchmark-tpch-tpcds-insert.sh /path/to/matching/pixels
~~~

Against an already running normal Trino and Retina deployment, copy and verify
all eight TPC-H and all 24 TPC-DS tiny tables:

~~~sh
JAVA_HOME=/path/to/jdk-23 \
bash tools/verify-all-tpc-tables.sh \
  jdbc:trino://127.0.0.1:18081 \
  /persistent/pixels/tables \
  tpc_insert_all

JAVA_HOME=/path/to/jdk-23 ALL_TPC_VERIFY_ONLY=true \
bash tools/verify-all-tpc-tables.sh \
  jdbc:trino://127.0.0.1:18081 \
  /persistent/pixels/tables \
  tpc_insert_all
~~~

For the complete SF100 `FILE + DURABLE` measurement, configure the Retina
deployment with `retina.ingest.write.representation=FILE` and
`retina.ingest.commit.ack=DURABLE`, then run:

~~~sh
JAVA_HOME=/path/to/jdk-23 \
TPCH_SCHEMA=sf100 TPCDS_SCHEMA=sf100 \
ALL_TPC_TRANSACTION_ROWS=20000000 \
ALL_TPC_BARRIER_EACH_TRANSACTION=false \
ALL_TPC_VISIBILITY_TIMEOUT_SECONDS=3600 \
bash tools/verify-all-tpc-tables.sh \
  jdbc:trino://127.0.0.1:18081 \
  /persistent/pixels/tables \
  tpc_sf100 | tee tpc-sf100-file-durable.log
~~~

The transaction-row target divides large tables into deterministic numeric-key
ranges. Each range is a separate DURABLE transaction. The SF100 performance
configuration accepts all ranges asynchronously and uses one fixed-boundary
visibility barrier per table; the default correctness configuration retains a
barrier after every transaction. Each table prints its source scan time,
DURABLE accepted time and throughput, visibility time, transaction count and
largest observed transaction. Set `ALL_TPC_RESUME=true` to verify completed
target tables and continue from the first missing deterministic transaction
range in a partially imported table after an interruption. A non-empty partial
range fails closed because each range is one atomic transaction. The prepared-row
and WAL limits must be sized for the largest
observed transaction before starting the run.

### SF10 ingestion and native writer comparison

The representative benchmark projects `lineitem` to `(orderkey, comment)`
and `store_sales` to `(ss_ticket_number, ss_item_sk-as-text)`, with a dataset
prefix on the text column. These results measure those two-column projections.
`INSERT_BENCHMARK_STAGE_SOURCE=true` materializes each projection in Trino's
memory catalog before the ingestion timer, avoiding repeated data generation
for concurrent key ranges. Staging and final checksum verification are timed
separately. The default `false` reads the generators directly.

Use fresh work directories for every measurement. With matching artifacts
already built under JDK 8 (Pixels) and JDK 23 (Trino), run under JDK 23:

~~~sh
export PIXELS_HOME=/path/to/matching/runtime
SQL_E2E_SKIP_BUILD=1 SQL_E2E_WORK_DIR=/tmp/insert-sf10-fresh \
SQL_E2E_TIMEOUT_SECONDS=14400 \
SQL_E2E_ENGINE_HEAP=8g SQL_E2E_BACKEND_HEAP=6g \
SQL_E2E_BACKEND_STOP_TIMEOUT_SECONDS=180 \
TPCH_SCHEMA=sf10 TPCDS_SCHEMA=sf10 \
INSERT_BENCHMARK_STAGE_SOURCE=true \
INSERT_BENCHMARK_CONCURRENCY=8 INSERT_BENCHMARK_TRANSACTION_ROWS=5000000 \
INSERT_BENCHMARK_VISIBILITY_TIMEOUT_SECONDS=7200 \
PIXELS_SQL_FIXTURE_REPRESENTATION=FILE PIXELS_SQL_FIXTURE_COMMIT_ACK=DURABLE \
PIXELS_SQL_FIXTURE_ROUTE_COUNT=4 PIXELS_SQL_FIXTURE_MAX_BATCH_ROWS=65536 \
PIXELS_SQL_FIXTURE_MAX_BATCH_BYTES=16777216 \
PIXELS_SQL_FIXTURE_MAX_PREPARED_ROWS=10000000 \
PIXELS_SQL_FIXTURE_MAX_STATE_BYTES=1073741824 \
PIXELS_SQL_FIXTURE_WAL_MAX_BYTES=68719476736 \
PIXELS_SQL_FIXTURE_WAL_MAX_RECORDS=100000000 \
PIXELS_SQL_FIXTURE_WAL_READ_CACHE_MAX_BYTES=268435456 \
PIXELS_SQL_FIXTURE_FILE_TARGET_ROWS=4000000 \
PIXELS_SQL_FIXTURE_FILE_MAX_BYTES=134217728 \
PIXELS_SQL_FIXTURE_FILE_MAX_DELAY_MS=300000 \
bash tools/benchmark-tpch-tpcds-insert.sh /path/to/pixels

# Build pixels-cli with JDK 8 first; execute this harness with JDK 23.
bash tools/benchmark-native-load.sh /path/to/pixels \
  /tmp/insert-sf10-fresh tpch /tmp/native-tpch-fresh
bash tools/benchmark-native-load.sh /path/to/pixels \
  /tmp/insert-sf10-fresh tpcds /tmp/native-tpcds-fresh
~~~

The native comparison exports the verified projection before its timer, then
runs the existing CLI `SimplePixelsConsumer` with four threads and real
PixelsWriter output. Metadata uses the catalog fixture. CSV preparation is
excluded, input is warm, and output footer row counts are checked. This baseline
does not perform transactional WAL or MainIndex installation.

September 26, 2026, Pixels `b66e3151d6d6a7e072a31155e04b42f84bc03479`,
two Trino workers, one Retina owner, four vnodes, staged input:

| Projection | Rows | DURABLE accepted | Accepted rows/s | Commit-to-visible | Start-to-visible | CLI writer |
|---|---:|---:|---:|---:|---:|---:|
| TPC-H SF10 lineitem | 59,986,052 | 27.910 s | 2,149,214 | 49.768 s | 77.678 s | 26.156 s |
| TPC-DS SF10 store_sales | 28,800,991 | 8.412 s | 3,423,594 | 14.932 s | 23.344 s | 6.798 s |

Accepted time ends after all INSERTs return with durable COMMIT decisions.
Commit-to-visible starts there and ends when the fixed-boundary barrier returns.
It is a latency, not a throughput. Source staging took 4.798 s and 15.276 s;
verification took 49.647 s and 40.237 s respectively. Both count and
order-independent all-projected-column checksums matched. This comparison still
shows a 2.97x / 3.43x start-to-visible gap against the CLI writer.

The optimized path uses a bounded private batch cache after durable sealing,
batch MainIndex installation for fresh allocations, cached batch type parsing,
and grouped WAL reclamation. Recovery retains exact-location verification;
cache misses and restarts read the WAL. Further profiling should target batch
allocation/encoding and MainIndex-buffer installation before increasing queue
limits or reporting accepted throughput as completed materialization.

Validation for this revision:

- `verify-sql-insert.sh` passed all four `BUFFERED|FILE` × `VISIBLE|DURABLE`
  configurations, each with 40 checks and explicit/autocommit statements.
- Pixels `verify-ingest-gc.sh` passed 19 journal cases and 13 GC cases,
  including five crash boundaries.
- Pixels `verify-ingest-daemon.sh` passed checkpoint/restart, shared-file
  recovery, offline cutover and corrupt/missing-state rejection using normal
  TransServer and RetinaServer services.
- JDK 8 Maven tests `TestLocalMutationJournal,TestRetinaPrivateRead,`
  `TestRetinaIngestParticipantCheckpoint,TestPixelsIngestStorage,`
  `TestCoordinatorStateStore,TestCoordinatorDurabilityBoundaries` passed
  (27 tests) with `-DskipTests=false -Dsurefire.failIfNoSpecifiedTests=false`
  and `-pl pixels-daemon -am`.

The SF10 evidence directory is `/tmp/pixels-staged-sf10-20260926`, containing
`sql.log`, `backend.log`, `status.properties` and `exit-code=0`. The native
results are in `/tmp/pixels-native-tpch-sf10-final-20260926/load.log` and
`/tmp/pixels-native-tpcds-sf10-20260926/load.log`. SQL and daemon evidence use
the `/tmp/pixels-group-gc-*-20260926` directories. These are local evidence
paths; the commands above reproduce the benchmark in a fresh directory.

### Small transaction benchmark

`SmallTransactionsInsert` drives independent INSERTs through Trino JDBC and
reports the accepted boundary separately from the final visibility barrier:

~~~sh
JAVA_HOME=/path/to/jdk-23 \
SMALL_INSERT_TRANSACTIONS=1000 \
SMALL_INSERT_CONCURRENCY=16 \
SMALL_INSERT_TRANSACTION_MODE=AUTOCOMMIT \
bash tools/benchmark-small-inserts.sh \
  jdbc:trino://127.0.0.1:18081 benchmark_schema events pixels
~~~

Set `SMALL_INSERT_TRANSACTION_MODE=EXPLICIT` to execute each INSERT followed by
JDBC `commit()` on the same connection. On the September 19, 2026 local
two-worker FILE+DURABLE run, 1000 measured single-row transactions plus 100
warmups formed one Pixels file:

| Mode | Accepted | Accepted throughput | Commit P50 | Commit-to-visible latency |
|---|---:|---:|---:|---:|
| AUTOCOMMIT | 19.950 s | 50.13 rows/s | 285.772 ms | 2,855.058 ms |
| EXPLICIT | 20.127 s | 49.69 rows/s | 297.985 ms | 2,621.465 ms |

September 26 rerun on Pixels `b66e3151` and connector `3dee5d2`, using fresh
FILE+DURABLE fixtures, four vnodes, two Trino workers, concurrency 16,
100 warmups and 1000 measured single-row transactions:

| Mode | Accepted | Transactions/s | Commit P50 | Commit P99 | Commit-to-visible |
|---|---:|---:|---:|---:|---:|
| AUTOCOMMIT | 19.820 s | 50.45 | 305.838 ms | 597.800 ms | 2,601.967 ms |
| EXPLICIT | 18.840 s | 53.08 | 287.614 ms | 583.512 ms | 2,551.180 ms |

Run with `SQL_E2E_MAIN_CLASS=io.pixelsdb.pixels.trino.testing.SmallTransactionsInsert`,
`SQL_E2E_PASS_PATTERN=SMALL_TRANSACTION_INSERT_PASS`,
`SMALL_INSERT_CREATE_TABLE=false`, `SMALL_INSERT_TRANSACTIONS=1000`,
`SMALL_INSERT_CONCURRENCY=16` and the selected
`SMALL_INSERT_TRANSACTION_MODE=AUTOCOMMIT|EXPLICIT` through
`tools/verify-sql-insert.sh`. Set `PIXELS_SQL_FIXTURE_REPRESENTATION=FILE`,
`PIXELS_SQL_FIXTURE_COMMIT_ACK=DURABLE`, `PIXELS_SQL_FIXTURE_ROUTE_COUNT=4`,
`PIXELS_SQL_FIXTURE_FILE_MAX_DELAY_MS=300000`, `SQL_E2E_BACKEND_HEAP=2g`
and `SQL_E2E_TIMEOUT_SECONDS=1800`. Use a fresh `SQL_E2E_WORK_DIR` per run.
The JDBC harness validates final count and ID sum; both modes produced four
files. Evidence is in `/tmp/pixels-small-autocommit-20260926` and
`/tmp/pixels-small-explicit-20260926`.

Small-transaction throughput has not materially improved with the bulk-path
optimizations. Profile query planning/scheduling and transaction RPC stages
separately before attributing its fixed cost to fsync. For pipeline design,
[RocksDB pipelined writes](https://github.com/facebook/rocksdb/wiki/Pipelined-Write)
illustrate overlapping the next WAL group with installation of the previous
group. In FILE mode the installation stage remains direct file materialization:
`TestPixelsIngestStorage` asserts unchanged shared MemTable row counts and no
new object-staging blocks across two FILE transaction contributions.

The diagnostic rerun in `/tmp/pixels-small-stages-20260926` additionally records
JDBC-reported query queued/CPU time and the explicit `Connection.commit()` call
separately. For 1000 explicit single-row transactions it measured 50.58
transactions/s, client P50 299.593 ms, INSERT query elapsed P50 258 ms,
queued P50 0 ms, query CPU P50 2 ms and explicit commit P50 33.758 ms.
These separate percentiles are not additive; query CPU is not coordinator
planning time. The run used the FILE writer's reusable per-file column batch,
which also passed the real-storage test and 40-check FILE+DURABLE SQL regression
(`/tmp/pixels-vector-file-sql-20260926`). No small-transaction throughput gain
is established by this change.

Set `SMALL_INSERT_QUERY_DIAGNOSTICS=true` for the embedded JDBC harness to
capture engine query phases after each measured INSERT. It remains disabled
for normal benchmark runs and external JDBC deployments. The diagnostic run
`/tmp/pixels-small-query-phases-20260926` collected 1000 samples: planning P50
40.549 ms, starting P50 0.791 ms, execution P50 218.546 ms and finishing P50
0.128 ms. Analysis P50 was 35.367 ms. Trino 466 measures execution from the
start of planning through query end, so these phase statistics overlap and
must not be added. Execution includes waiting, not just task CPU. The next profiling target is worker scheduling and
per-RPC latency within execution, rather than assuming that WAL flush dominates.

Run the second command after the configured buffer flush interval and again
after stopping and restarting the normal Coordinator and Retina JVMs. It
compares row counts and an order-independent checksum of every column. Before
restart, record the recovery-checkpoint pointer and verify that WAL and
installer state have actually shrunk; a READY message alone is insufficient.

`retina.ingest.max.prepared.rows` limits one prepared transaction, not a table
or MemTable. Size it above the largest supported single INSERT while retaining
a bounded failure domain. TPC-DS tiny `customer_demographics` contains
1,920,800 rows, so the default 1,000,000-row limit deliberately rejects that
one-statement load; the all-table verification uses 3,000,000.

On September 13, 2026 the normal-daemon run inserted 2,695,496 rows across all
32 tables. After the 60-second forced flush, WAL fell from 218 MiB to 12 KiB and
installer state from 128 KiB to 16 KiB. A process restart loaded the published
checkpoint with zero pending replay segments, and all 32 row multisets matched.

The same run exposed and fixed two recovery-only failures: replay now preserves
the recorded file identity when a plan crosses a file boundary, and local
object reads copy reader-owned direct/mapped memory before closing the reader.
These are required for deterministic recovery; neither may be replaced by
allocating a fresh file or returning a buffer owned by a closed reader.

### Local Trino smoke test

For a deployment whose Trino HTTP port is 18081:

~~~sh
/path/to/trino --server http://127.0.0.1:18081 \
  --catalog pixels --schema demo
~~~

Then create a dedicated path and table and exercise immediate visibility:

~~~sql
CREATE SCHEMA IF NOT EXISTS pixels.demo;
CREATE TABLE pixels.demo.insert_smoke (
  id bigint,
  label varchar,
  amount decimal(12, 2),
  created date
) WITH (
  storage = 'file',
  paths = 'file:///persistent/pixels/tables/demo/insert_smoke'
);
INSERT INTO pixels.demo.insert_smoke VALUES
  (1, 'alpha', DECIMAL '12.30', DATE '2026-09-13'),
  (2, NULL, NULL, NULL);
SELECT * FROM pixels.demo.insert_smoke ORDER BY id;
~~~

Create a new empty directory for a new table. Do not point `paths` at another
table or change the offline-cutover baseline while services are live.

## Remaining limitations

- No replicated coordinator/participant log or automatic HA owner failover.
- Fixed topology, a persistent catalog fixture and process-local read leases;
  multi-owner GC is not certified.
- No Trino task-attempt retry protocol.
- UPDATE/DELETE MergeSink is not implemented.
- Unique secondary constraints are rejected.
- Full SQL still uses catalog, topology and external-ID fixtures; production
  metadata/container deployment needs an environment-specific test.
