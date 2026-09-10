# Retina INSERT: worker stream writer

Related: pixelsdb/pixels-trino#180.

`PixelsMutationWriter` is the query-local transport writer for pre-encoded,
pre-routed batches. It uses the shared `pixels-common.ingest` contract from the
matching Pixels branch. It neither holds a PixelsWriter/MemTable nor maintains
row IDs or indexes.

The writer supplies per-stream sequence numbers and hash-chain seals, preserves
equal-valued rows, bounds retained payload bytes and stream count, serializes
transport requests, and propagates failures. Callers must await outstanding
append futures before exceeding the configured payload budget; a rejected
admission does not consume a sequence.

`finish()` closes local admission, waits for outstanding appends, and verifies
all durable stream receipts. It is safe to call while the final append is still
in flight. It does not commit a transaction. `abort()` stops local submission;
the coordinator must separately request an authoritative Abort. In-flight RPCs
may have reached the server, so transport cancellation is never proof of rollback.

The transport implementation must register/authorize streams and fence stale
owners. The interface deliberately contains no worker-side commit operation.

## Verification

With the matching Pixels checkout:

```sh
bash tools/verify-ingest-contract.sh /path/to/pixels
```

This runs 13 writer contract cases and a real writer-to-local-journal staging
round trip: 500 equal-valued rows, 50 retransmitted batches, four streams, and
restart recovery. It does not test Trino SQL execution or distributed commit.
The module includes JUnit 5 wrappers for the same writer cases.

## SQL integration status

The writer is not registered in `PixelsConnector` and SQL INSERT remains
unsupported. Page encoding, routing metadata, RPC transport, PageSink/provider,
`beginInsert`/`finishInsert`, durable transaction coordination, publication, and
recovery integration are not implemented in this slice. Enabling writes before
those components exist would incorrectly acknowledge transactions.

The stream protocol reserves both `APPEND_ROWS` and `DELETE_ROWS`; future MergeSink
work can reuse this writer while resolving old rows through existing MainIndex
and updating existing SinglePointIndex implementations.
