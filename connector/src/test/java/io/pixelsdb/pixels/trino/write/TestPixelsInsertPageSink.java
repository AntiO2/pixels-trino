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

import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.MutationTransport;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.ingest.IngestProto.Route;
import io.pixelsdb.pixels.ingest.IngestProto.TableColumn;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import io.trino.spi.Page;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.junit.jupiter.api.Assertions.*;

public class TestPixelsInsertPageSink
{
    @Test
    public void emptySinksDoNotAllocateWriters() throws Exception
    {
        AtomicLong allocations = new AtomicLong();
        List<Long> streams = new ArrayList<>();
        PixelsInsertPageSink empty = sink(allocations, streams);
        empty.appendPage(new Page(0)).join();
        assertTrue(empty.finish().join().isEmpty());
        assertEquals(0, allocations.get());
        assertEquals(0, empty.getMemoryUsage());
        PixelsInsertPageSink aborted = sink(allocations, streams);
        aborted.abort();
        assertThrows(IllegalStateException.class, () -> aborted.appendPage(new Page(1)));
        assertEquals(0, allocations.get());
        assertTrue(streams.isEmpty());
    }

    @Test
    public void nonemptySinksAllocateIndependentWritersOnce() throws Exception
    {
        AtomicLong allocations = new AtomicLong();
        List<Long> streams = new ArrayList<>();
        PixelsInsertPageSink first = sink(allocations, streams);
        PixelsInsertPageSink second = sink(allocations, streams);
        first.appendPage(new Page(1)).join();
        first.appendPage(new Page(1)).join();
        second.appendPage(new Page(1)).join();
        assertEquals(2, allocations.get());
        assertEquals(List.of(1L, 1L, 2L), streams);
        assertEquals(1, first.finish().join().size());
        assertEquals(1, second.finish().join().size());
        assertSame(first.finish(), first.finish());
    }

    private static PixelsInsertPageSink sink(AtomicLong allocations, List<Long> streams) throws Exception
    {
        return sink(1, 1, allocations, streams, new ArrayList<>());
    }

    @Test
    public void transactionLocalWriterIdsDoNotConcentrateKeylessRows() throws Exception
    {
        final int vnodeCount = 4;
        final int transactions = 32;
        List<Integer> shards = new ArrayList<>();
        for (long transaction = 1; transaction <= transactions; transaction++) {
            PixelsInsertPageSink sink = sink(transaction, vnodeCount,
                    new AtomicLong(), new ArrayList<>(), shards);
            sink.appendPage(new Page(1)).join();
            sink.finish().join();
        }
        assertEquals(vnodeCount, new HashSet<>(shards).size());
    }

    private static PixelsInsertPageSink sink(long transaction, int routes, AtomicLong allocations,
            List<Long> streams, List<Integer> shards) throws Exception
    {
        TableSpec.Builder table = TableSpec.newBuilder().setTableId(1).setSchemaVersion(1)
                .addColumns(TableColumn.newBuilder().setId(1).setName("id").setType("bigint"));
        for (int shard = 0; shard < routes; shard++) {
            table.addRoutes(Route.newBuilder().setShardId(shard));
        }
        PixelsInsertTableHandle handle = new PixelsInsertTableHandle(transaction, 1,
                Base64.getEncoder().encodeToString(table.build().toByteArray()), List.of());
        TypeManager types = (TypeManager) Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(), new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("fromSqlType")) {
                        return BIGINT;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        MutationTransport transport = new MutationTransport() {
            @Override
            public CompletableFuture<Void> append(MutationBatch batch) {
                streams.add(batch.getStreamId().getWriterId());
                shards.add(batch.getStreamId().getShardId());
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<MutationStreamSeal> seal(MutationStreamSeal expected) {
                return CompletableFuture.completedFuture(expected);
            }
        };
        return new PixelsInsertPageSink(handle, allocations::incrementAndGet, types, transport, new IngestOptions());
    }
}
