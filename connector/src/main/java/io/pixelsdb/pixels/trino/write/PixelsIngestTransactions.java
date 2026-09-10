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

import static io.trino.spi.StandardErrorCode.*;

import com.google.inject.Inject;
import com.google.protobuf.Empty;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.pixelsdb.pixels.common.ingest.rpc.*;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import io.pixelsdb.pixels.trino.*;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/** Coordinator-local handles over server-owned transactions. Pin renewals are independent of SQL commit calls. */
public final class PixelsIngestTransactions implements Closeable {
    private static final Logger LOG = Logger.get(PixelsIngestTransactions.class);

    private static final class ReadSession {
        final PixelsTransactionHandle handle;
        final Map<String, ReadPin> pins;
        volatile Transaction write;
        volatile RuntimeException renewalFailure;

        ReadSession(PixelsTransactionHandle handle, Map<String, ReadPin> pins) {
            this.handle = handle;
            this.pins = pins;
        }
    }

    private final boolean enabled;
    private final IngestOptions options;
    private final IngestClient client;
    private final TypeManager types;
    private final Map<Long, ReadSession> reads = new ConcurrentHashMap<>();
    private final ScheduledExecutorService renewals;

    @Inject
    public PixelsIngestTransactions(PixelsTrinoConfig config, TypeManager types) {
        this.enabled = config.isInsertEnabled();
        this.types = types;
        if (!enabled) {
            options = null;
            client = null;
            renewals = null;
            return;
        }
        try {
            options = new IngestOptions();
            if (!options.enabled
                    || !Boolean.parseBoolean(IngestOptions.property("retina.enable", "false"))
                    || !Boolean.parseBoolean(
                            IngestOptions.property("retina.buffer.split.enable", "true"))
                    || config.getCloudFunctionSwitch()
                            != PixelsTrinoConfig.CloudFunctionSwitch.OFF) {
                throw new IOException(
                        "INSERT requires transactional Retina, buffer splits, and"
                            + " cloud.function.switch=off");
            }
            client = IngestClient.fromConfig();
            renewals =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "pixels-ingest-leases");
                                t.setDaemon(true);
                                return t;
                            });
            long interval =
                    Math.max(
                            250,
                            Math.min(options.readLeaseMillis, options.transactionLeaseMillis) / 4);
            renewals.scheduleWithFixedDelay(this::renew, interval, interval, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new TrinoException(
                    CONFIGURATION_INVALID, "Cannot initialize transactional INSERT", e);
        }
    }

    public boolean enabled() {
        return enabled;
    }

    public IngestOptions options() {
        requireEnabled();
        return options;
    }

    public IngestClient client() {
        requireEnabled();
        return client;
    }

    private void requireEnabled() {
        if (!enabled) {
            throw new TrinoException(
                    NOT_SUPPORTED, "Enable insert.enabled to use transactional Retina INSERT");
        }
    }

    public void beginRead(PixelsTransactionHandle handle) {
        if (!enabled) {
            return;
        }
        Map<String, ReadPin> pins = new LinkedHashMap<>();
        try {
            Publication publication =
                    client.coordinator().getPublication(Empty.getDefaultInstance());
            if (handle.getTimestamp() > publication.getPublishedTimestamp()) {
                throw new IOException("Unpublished read timestamp");
            }
            for (Route route : publication.getRoutesList()) {
                String owner = IngestWire.owner(route);
                if (!pins.containsKey(owner)) {
                    ReadPin request =
                            ReadPin.newBuilder()
                                    .setTransactionId(handle.getTransId())
                                    .setReadTimestamp(handle.getTimestamp())
                                    .build();
                    pins.put(owner, client.participant(owner).pinRead(request));
                }
            }
            if (pins.isEmpty()) {
                throw new IOException("No Retina owners are available");
            }
            Map<String, String> tokens = new HashMap<>();
            pins.forEach((owner, pin) -> tokens.put(owner, pin.getToken()));
            handle.setIngestReadTokens(tokens);
            reads.put(handle.getTransId(), new ReadSession(handle, pins));
        } catch (Exception e) {
            pins.forEach(
                    (owner, pin) -> {
                        try {
                            client.participant(owner).releaseRead(pin);
                        } catch (Exception ignored) {
                        }
                    });
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "Cannot acquire a consistent Retina read view", e);
        }
    }

    private ReadSession session(PixelsTransactionHandle handle) {
        ReadSession read = reads.get(handle.getTransId());
        if (read == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Missing ingest read session");
        }
        if (read.renewalFailure != null) {
            throw read.renewalFailure;
        }
        return read;
    }

    public PixelsInsertTableHandle beginInsert(
            PixelsTransactionHandle handle,
            PixelsTableHandle table,
            List<PixelsColumnHandle> columns) {
        requireEnabled();
        if (!handle.isAutoCommit()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "INSERT currently requires a single-statement autocommit transaction");
        }
        ReadSession read = session(handle);
        synchronized (read) {
            if (read.write != null) {
                throw new TrinoException(NOT_SUPPORTED, "Only one write statement is allowed");
            }
            try {
                Transaction tx =
                        client.coordinator()
                                .beginWrite(
                                        BeginWriteRequest.newBuilder()
                                                .setRequestId("trino:" + handle.getTransId())
                                                .setReadTimestamp(handle.getTimestamp())
                                                .setSchemaName(table.getSchemaName())
                                                .setTableName(table.getTableName())
                                                .build());
                read.write = tx;
                if (tx.getState() != TransactionState.OPEN) {
                    throw new IOException("Write transaction is no longer OPEN");
                }
                new PixelsPageEncoder(
                        tx.getTable(),
                        columns,
                        types); // validate supported types and input mapping before tasks start
                return new PixelsInsertTableHandle(
                        tx.getTransactionId(),
                        Base64.getEncoder().encodeToString(tx.getTable().toByteArray()),
                        columns);
            } catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot begin Retina INSERT", e);
            }
        }
    }

    public void finishInsert(
            PixelsTransactionHandle handle,
            PixelsInsertTableHandle target,
            Collection<Slice> fragments) {
        ReadSession read = session(handle);
        if (read.write == null || read.write.getTransactionId() != target.getTransactionId()) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "INSERT transaction identity mismatch");
        }
        try {
            if (fragments.size() > options.maxStreams) {
                throw new IOException("Too many writer fragments");
            }
            PrepareWriteRequest.Builder request =
                    PrepareWriteRequest.newBuilder().setTransactionId(target.getTransactionId());
            for (Slice fragment : fragments) {
                if (fragment.length() > 65536) {
                    throw new IOException("Oversized writer receipt");
                }
                request.addSeals(StreamSeal.parseFrom(fragment.getBytes()));
            }
            read.write = client.coordinator().prepareWrite(request.build());
        } catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Retina INSERT preparation failed", e);
        }
    }

    public void commit(PixelsTransactionHandle handle) {
        if (!enabled) {
            return;
        }
        ReadSession read = session(handle);
        if (read.write == null) {
            return;
        }
        long id = read.write.getTransactionId();
        try {
            Transaction outcome = client.coordinator().commitWrite(IngestWire.id(id));
            read.write = outcome;
            if (outcome.getState() != TransactionState.PUBLISHED) {
                throw new IOException("Commit has not reached publication");
            }
        } catch (Exception e) {
            try {
                read.write = client.coordinator().getWrite(IngestWire.id(id));
                if (read.write.getState() == TransactionState.PUBLISHED) {
                    return;
                }
            } catch (Exception statusFailure) {
                e.addSuppressed(statusFailure);
            }
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "INSERT outcome requires reconciliation for txId="
                            + id
                            + "; do not assume rollback or blindly resubmit the SQL",
                    e);
        }
    }

    public void abort(PixelsTransactionHandle handle) {
        if (!enabled) {
            return;
        }
        ReadSession read = reads.get(handle.getTransId());
        if (read == null || read.write == null) {
            return;
        }
        try {
            read.write =
                    client.coordinator().abortWrite(IngestWire.id(read.write.getTransactionId()));
            if (IngestWire.committed(read.write)) {
                LOG.warn(
                        "Cancellation arrived after COMMIT for ingest txId=%s",
                        read.write.getTransactionId());
            }
        } catch (Exception e) {
            LOG.warn(
                    e,
                    "Abort decision requires reconciliation for ingest txId=%s",
                    read.write.getTransactionId());
        }
    }

    public void endRead(PixelsTransactionHandle handle) {
        ReadSession read = reads.remove(handle.getTransId());
        if (read == null) {
            return;
        }
        read.pins.forEach(
                (owner, pin) -> {
                    try {
                        client.participant(owner).releaseRead(pin);
                    } catch (Exception e) {
                        LOG.debug(e, "Read pin release deferred to expiry");
                    }
                });
    }

    private void renew() {
        for (ReadSession read : reads.values()) {
            try {
                if (read.renewalFailure != null) {
                    continue;
                }
                for (Map.Entry<String, ReadPin> entry : read.pins.entrySet()) {
                    client.participant(entry.getKey()).renewRead(entry.getValue());
                }
                Transaction write = read.write;
                if (write != null
                        && !IngestWire.committed(write)
                        && write.getState() != TransactionState.ABORTED) {
                    client.coordinator().touchWrite(IngestWire.id(write.getTransactionId()));
                }
            } catch (Exception e) {
                read.renewalFailure =
                        new TrinoException(
                                GENERIC_INTERNAL_ERROR,
                                "Retina read/write lease could not be renewed",
                                e);
            }
        }
    }

    @Override
    public void close() {
        if (!enabled) {
            return;
        }
        renewals.shutdownNow();
        for (ReadSession read : new ArrayList<>(reads.values())) {
            abort(read.handle);
            endRead(read.handle);
        }
        client.close();
    }
}
