# Transactional INSERT: implementation, lifecycle and verification

Related: pixelsdb/pixels-trino#180 and pixelsdb/pixels-trino#179.

## Implemented scope

The connector supports single-statement autocommit INSERT VALUES and
INSERT ... SELECT. Trino Pages are encoded into bounded column batches,
routed through authenticated ingestion RPCs and staged in a transaction-private
Retina WAL. The durable coordinator records COMMIT or ABORT before installation.
A successful commit is returned only after installed rows reach the published
read timestamp.

Installation reuses existing Retina write buffers and pixels-index:

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
retina.ingest.terminal.retention.ms=86400000
retina.ingest.terminal.max.transactions=100000
retina.ingest.wal.segment.bytes=67108864
retina.ingest.wal.max.bytes=4294967296
retina.ingest.wal.max.records=10000000
~~~

The three state directories must be absolute, pairwise non-overlapping,
persistent local volumes owned by exactly one live process. The credential must
contain 24–4096 visible ASCII bytes after trimming; deploy it with owner-only
permissions. RPC transport is plaintext, so use a trusted network or external
TLS boundary.

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
4. The participant then writes CHECKPOINTED and compacts live WAL records into
   a new generation. Its pointer is durable before an old WAL is deleted.
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
The first process commits 64 rows, commits one still-buffered row, waits for a
recovery checkpoint, verifies that the first transaction's full plan and WAL
payload were physically replaced by compact checkpoint/fence state, checks the
legacy write fence, and shuts down cleanly. Before starting services, the second
process opens the same catalog, plan and WAL state and validates the handoff; it
then recovers both PUBLISHED decisions, reaches READY, shuts down, and reads the
Pixels files directly to prove an exact 65-row multiset without replay duplicates.
It finally corrupts a copied committed-decision state and removes the published
checkpoint body in turn; separate daemon attempts must fail before READY with an
actionable checksum or missing-body error. An offline cutover phase first proves
that a transaction allocator below the configured baseline is rejected. It then
advances the isolated etcd ID domain while services are stopped, preserves all
65 old physical rows, commits one new row above baseline 1000000000, and rechecks
the legacy mutation fence. Catalog persistence and topology discovery remain
fixtures.

~~~text
PIXELS_NORMAL_INGEST_DAEMON_PHASE1_PASS rows=65 checkpointedTransaction=1
PIXELS_NORMAL_INGEST_DAEMON_PASS rows=65 pixelsFiles=2 services=TransServer,RetinaServer checkpointRestart=2
PIXELS_NORMAL_INGEST_CUTOVER_PASS oldRows=65 totalRows=66 baseline=1000000000 commitTimestamp=1000000002 legacyFence=1
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

The harness uses real Trino 466 coordinator/two-worker servers, actual plugin
classloader and connector, TCP/gRPC, decisions/journals, Retina MemTables,
native visibility, SQLite MainIndex and Pixels readers/writers. Catalog, node
directory and external identity allocation are fixtures. It is one-host,
one-Retina-owner integration, not HA certification.

Assertions cover VALUES, duplicate preservation, NULLs, reordered/omitted
columns, zero-row and 500-row INSERT SELECT, same-table INSERT SELECT,
concurrent commits, buffer/file handoff and a 480-row private staging failure
followed by durable Abort.

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

## Remaining limitations

- No replicated coordinator/participant log or automatic HA owner failover.
- Fixed topology, a persistent catalog fixture and process-local read leases;
  multi-owner GC is not certified.
- No Trino task-attempt retry protocol.
- UPDATE/DELETE MergeSink is not implemented.
- Unique secondary constraints are rejected.
- Full SQL still uses catalog, topology and external-ID fixtures; production
  metadata/container deployment needs an environment-specific test.
