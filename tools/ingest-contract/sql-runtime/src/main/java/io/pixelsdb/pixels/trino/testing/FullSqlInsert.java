package io.pixelsdb.pixels.trino.testing;

import static io.trino.testing.TestingSession.testSessionBuilder;

import io.trino.metadata.HandleResolver;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;

import java.net.URL;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Runs actual SQL against the real connector, RPC participant, and Pixels storage. */
public final class FullSqlInsert {
    private static final java.util.concurrent.atomic.AtomicInteger checks =
            new java.util.concurrent.atomic.AtomicInteger();

    private static void equal(long actual, long expected, String context) {
        if (actual != expected) {
            throw new AssertionError(context + ": expected=" + expected + ", actual=" + actual);
        }
        checks.incrementAndGet();
    }

    private static long scalar(DistributedQueryRunner runner, String sql) {
        return ((Number) runner.execute(sql).getOnlyValue()).longValue();
    }

    private static void insert(DistributedQueryRunner runner, String sql, long rows) {
        MaterializedResult result = runner.execute(sql);
        equal(result.getUpdateCount().orElseThrow(), rows, sql);
        System.out.println("SQL_PASS affected=" + rows + " sql=" + sql);
    }

    private static Properties status(Path control) throws Exception {
        Properties result = new Properties();
        try (java.io.InputStream input =
                Files.newInputStream(control.resolve("status.properties"))) {
            result.load(input);
        }
        return result;
    }

    private static long metric(Path control, String name) throws Exception {
        return Long.parseLong(status(control).getProperty(name));
    }

    public static void main(String[] args) throws Exception {
        Path control = Paths.get(args[0]);
        List<URL> pluginUrls = new ArrayList<>();
        for (String path :
                Files.readString(Paths.get(args[1])).trim().split(java.io.File.pathSeparator)) {
            pluginUrls.add(Paths.get(path).toUri().toURL());
        }
        try (PluginClassLoader pluginLoader =
                        PluginManager.createClassLoader("pixels-sql-e2e", pluginUrls);
                DistributedQueryRunner runner =
                        DistributedQueryRunner.builder(
                                        testSessionBuilder()
                                                .setCatalog("pixels")
                                                .setSchema("s")
                                                .build())
                                .setWorkerCount(2)
                                .build()) {
            for (io.trino.server.testing.TestingTrinoServer server : runner.getServers()) {
                server.getInstance(com.google.inject.Key.get(HandleResolver.class))
                        .registerClassLoader(pluginLoader);
            }
            Plugin plugin =
                    (Plugin)
                            pluginLoader
                                    .loadClass("io.pixelsdb.pixels.trino.PixelsPlugin")
                                    .getConstructor()
                                    .newInstance();
            try (io.trino.spi.classloader.ThreadContextClassLoader ignored =
                    new io.trino.spi.classloader.ThreadContextClassLoader(pluginLoader)) {
                runner.installPlugin(plugin);
            }
            runner.createCatalog(
                    "pixels",
                    "pixels",
                    Map.of("cloud.function.switch", "off", "insert.enabled", "true"));
            equal(scalar(runner, "SELECT count(*) FROM t"), 0, "empty keyless table");
            insert(runner, "INSERT INTO t VALUES (1, 'a')", 1);
            equal(
                    scalar(runner, "SELECT count(*) FROM t WHERE id=1 AND label='a'"),
                    1,
                    "immediate read after commit");
            insert(runner, "INSERT INTO t VALUES (2, 'b'), (2, 'b'), (3, NULL)", 3);
            equal(
                    scalar(runner, "SELECT count(*) FROM t WHERE id=2 AND label='b'"),
                    2,
                    "duplicate values preserved");
            insert(runner, "INSERT INTO t (label, id) VALUES ('reordered', 4)", 1);
            insert(runner, "INSERT INTO t (id) VALUES (5)", 1);
            equal(
                    scalar(runner, "SELECT count(*) FROM t WHERE label IS NULL"),
                    2,
                    "omitted columns and SQL NULL");
            insert(runner, "INSERT INTO t SELECT 99, 'empty' WHERE false", 0);
            insert(
                    runner,
                    "INSERT INTO t SELECT n+1000, 'bulk' FROM UNNEST(sequence(1, 500)) AS u(n)",
                    500);
            equal(scalar(runner, "SELECT count(*) FROM t"), 506, "all bulk rows visible");
            equal(
                    scalar(runner, "SELECT sum(id) FROM t WHERE label='bulk'"),
                    625250,
                    "bulk values round trip");
            insert(
                    runner,
                    "INSERT INTO t SELECT id+100000, label FROM t WHERE id BETWEEN 1001 AND 1500",
                    500);
            equal(
                    scalar(runner, "SELECT count(*) FROM t"),
                    1006,
                    "INSERT SELECT from Pixels snapshot");
            ExecutorService writers = Executors.newFixedThreadPool(2);
            try {
                Future<?> a =
                        writers.submit(
                                () ->
                                        insert(
                                                runner,
                                                "INSERT INTO t VALUES (200000, 'concurrent-a')",
                                                1));
                Future<?> b =
                        writers.submit(
                                () ->
                                        insert(
                                                runner,
                                                "INSERT INTO t VALUES (200001, 'concurrent-b')",
                                                1));
                a.get(60, TimeUnit.SECONDS);
                b.get(60, TimeUnit.SECONDS);
            } finally {
                writers.shutdownNow();
            }
            equal(scalar(runner, "SELECT count(*) FROM t"), 1008, "concurrent SQL commits");
            long acceptedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (metric(control, "acceptedRows") < 1008 && System.nanoTime() < acceptedDeadline) {
                Thread.sleep(50);
            }
            equal(
                    metric(control, "acceptedRows"),
                    1008,
                    "accepted rows before the failing statement");
            long abortsBefore = metric(control, "abortedTransactions");
            boolean rejected = false;
            try {
                runner.execute(
                        "INSERT INTO t SELECT n, IF(n=500, rpad('x',8192,'x'), 'pending') FROM"
                            + " UNNEST(sequence(1,500)) AS u(n)");
            } catch (RuntimeException expected) {
                if (!expected.getMessage()
                        .contains("One INSERT row exceeds the configured batch limit")) {
                    throw new AssertionError(
                            "Unexpected failure instead of mid-stream encoder rejection", expected);
                }
                rejected = true;
                System.out.println("SQL_EXPECTED_REJECTION " + expected.getMessage());
            }
            if (!rejected) {
                throw new AssertionError("Oversized append must fail");
            }
            equal(
                    scalar(runner, "SELECT count(*) FROM t"),
                    1008,
                    "no partial statement after failed append");
            long abortDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (metric(control, "abortedTransactions") <= abortsBefore
                    && System.nanoTime() < abortDeadline) {
                Thread.sleep(50);
            }
            if (metric(control, "abortedTransactions") <= abortsBefore) {
                throw new AssertionError("Authoritative ABORT was not recorded");
            }
            if (metric(control, "acceptedRows") <= 1008) {
                throw new AssertionError("Failure test did not reach private server-side staging");
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (metric(control, "pixelsFiles") == 0 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            if (metric(control, "pixelsFiles") == 0) {
                throw new AssertionError("No Pixels files were materialized");
            }
            equal(
                    scalar(runner, "SELECT count(*) FROM t"),
                    1008,
                    "file and buffer handoff does not lose or duplicate rows");
            if (metric(control, "appendRPCs") == 0
                    || metric(control, "bufferReadRPCs") == 0
                    || metric(control, "fileVisibilityRPCs") == 0) {
                throw new AssertionError(
                        "Both RPC buffer reads and physical file reads must be exercised");
            }
            System.out.println(
                    "FULL_SQL_INSERT_E2E_PASS checks="
                            + checks
                            + " rows=1008 trinoWorkers=2"
                            + " appendRPCs="
                            + metric(control, "appendRPCs")
                            + " bufferReadRPCs="
                            + metric(control, "bufferReadRPCs")
                            + " fileVisibilityRPCs="
                            + metric(control, "fileVisibilityRPCs")
                            + " pixelsFiles="
                            + metric(control, "pixelsFiles")
                            + " privateAbortedRows="
                            + (metric(control, "acceptedRows") - 1008)
                            + " storage="
                            + status(control).getProperty("dataRoot"));
        }
    }
}
