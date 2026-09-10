/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.trino.write;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.pixelsdb.pixels.common.ingest.*;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.common.ingest.wire.*;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.ingest.IngestRows;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Worker-side bounded encoding and streaming. A sink seals streams; it never commits a transaction. */
public final class PixelsInsertPageSink implements ConnectorPageSink {
    private final PixelsMutationWriter writer;
    private final PixelsPageEncoder encoder;
    private final TableSpec table;
    private final TableIndex primary;
    private final int maxRows;
    private final int maxBytes;
    private final AtomicLong completedBytes = new AtomicLong();
    private final AtomicLong retainedBytes = new AtomicLong();
    private long batchCounter;
    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
    private CompletableFuture<Collection<Slice>> finishing;
    private volatile boolean aborted;

    public PixelsInsertPageSink(
            PixelsInsertTableHandle handle,
            long writerId,
            TypeManager types,
            MutationTransport transport,
            IngestOptions options)
            throws IOException {
        table = handle.decodeTable();
        primary = IngestRows.primary(table);
        encoder = new PixelsPageEncoder(table, handle.getInputColumns(), types);
        maxRows = options.maxBatchRows;
        maxBytes = options.maxBatchBytes;
        batchCounter = writerId;
        writer =
                new PixelsMutationWriter(
                        handle.getTransactionId(),
                        writerId,
                        table.getTableId(),
                        table.getSchemaVersion(),
                        ColumnBatchCodec.FORMAT,
                        Math.multiplyExact((long) maxBytes, 2),
                        options.maxStreams,
                        transport);
    }

    @Override
    public synchronized CompletableFuture<?> appendPage(Page page) {
        if (aborted || finishing != null) {
            throw new IllegalStateException("INSERT sink is not accepting pages");
        }
        if (!tail.isDone()) {
            throw new IllegalStateException("Caller must honor INSERT backpressure");
        }
        if (tail.isCompletedExceptionally()) {
            return tail;
        }
        // RLE/dictionary pages can have a small retained size but a huge expanded size.
        // Encode incrementally, one bounded row window at a time, and await each window.
        retainedBytes.set(page.getRetainedSizeInBytes());
        tail = appendWindow(page, 0).whenComplete((ignored, error) -> retainedBytes.set(0));
        return tail;
    }

    private CompletableFuture<Void> appendWindow(Page page, int start) {
        if (start == page.getPositionCount()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            if (aborted) {
                throw new CancellationException("INSERT sink aborted");
            }
            Map<Integer, List<byte[][]>> groups = new LinkedHashMap<>();
            int remainingBytes = maxBytes - 12;
            int end = start;
            int noKeyShard =
                    table.getRoutes(Math.floorMod(batchCounter++, table.getRoutesCount()))
                            .getShardId();
            while (end < page.getPositionCount() && end - start < maxRows) {
                byte[][] row = encoder.encodeRow(page, end);
                long rowBytes = 4L * row.length;
                for (byte[] value : row) {
                    if (value != null) {
                        rowBytes += value.length;
                    }
                }
                if (rowBytes > maxBytes - 12L) {
                    throw new IOException("One INSERT row exceeds the configured batch limit");
                }
                if (rowBytes > remainingBytes && end > start) {
                    break;
                }
                int shard =
                        primary == null
                                ? noKeyShard
                                : RetinaUtils.getBucketIdFromByteBuffer(
                                        IngestRows.indexKey(primary, row));
                IngestWire.route(table, shard);
                groups.computeIfAbsent(shard, ignored -> new ArrayList<>()).add(row);
                remainingBytes -= (int) rowBytes;
                end++;
            }
            CompletableFuture<Void> writes = CompletableFuture.completedFuture(null);
            for (Map.Entry<Integer, List<byte[][]>> group : groups.entrySet()) {
                byte[] payload =
                        ColumnBatchCodec.encode(
                                group.getValue(), table.getColumnsCount(), maxBytes);
                int count = group.getValue().size();
                int shard = group.getKey();
                writes =
                        writes.thenCompose(
                                        ignored ->
                                                writer.append(
                                                        MutationStreamId.Kind.APPEND_ROWS,
                                                        shard,
                                                        count,
                                                        payload))
                                .thenRun(() -> completedBytes.addAndGet(payload.length));
            }
            int next = end;
            // Avoid unbounded recursion when a local transport completes synchronously.
            return writes.thenComposeAsync(ignored -> appendWindow(page, next));
        } catch (Exception e) {
            CompletableFuture<Void> failure = new CompletableFuture<>();
            failure.completeExceptionally(e);
            return failure;
        }
    }

    @Override
    public synchronized CompletableFuture<Collection<Slice>> finish() {
        if (finishing != null) {
            return finishing;
        }
        if (aborted) {
            throw new IllegalStateException("INSERT sink aborted");
        }
        finishing =
                tail.thenCompose(ignored -> writer.finish())
                        .thenApply(
                                receipts -> {
                                    List<Slice> fragments = new ArrayList<>();
                                    for (MutationStreamSeal receipt : receipts) {
                                        fragments.add(
                                                Slices.wrappedBuffer(
                                                        IngestWire.encode(receipt).toByteArray()));
                                    }
                                    return fragments;
                                });
        return finishing;
    }

    @Override
    public synchronized void abort() {
        aborted = true;
        writer.abort();
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes.get();
    }

    @Override
    public long getMemoryUsage() {
        return retainedBytes.get() + Math.multiplyExact((long) maxBytes, 2);
    }
}
