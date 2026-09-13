package io.pixelsdb.pixels.trino.testing;

import static io.trino.testing.TestingSession.testSessionBuilder;

import io.trino.metadata.HandleResolver;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** End-to-end generated TPC data INSERT benchmark with concurrent visibility checks. */
public final class TpchTpcdsInsertBenchmark {
    private record SourceResult(long rows, String checksum, long elapsedMillis) {}

    private record Dataset(
            String name,
            String source,
            String projection,
            String targetPredicate) {}

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int positiveInteger(String name, int defaultValue) {
        int value = Integer.parseInt(environment(name, Integer.toString(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String identifier(String name, String defaultValue) {
        String value = environment(name, defaultValue);
        if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(name + " is not a SQL identifier: " + value);
        }
        return value;
    }

    private static long scalar(DistributedQueryRunner runner, String sql) {
        return ((Number) runner.execute(sql).getOnlyValue()).longValue();
    }

    private static SourceResult sourceResult(DistributedQueryRunner runner, Dataset dataset) {
        long start = System.nanoTime();
        MaterializedRow row =
                runner.execute(
                                "SELECT count(*), to_hex(checksum(ROW(" + dataset.projection()
                                        + "))) FROM " + dataset.source())
                        .getMaterializedRows()
                        .get(0);
        return new SourceResult(
                ((Number) row.getField(0)).longValue(),
                String.valueOf(row.getField(1)),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    private static String targetChecksum(DistributedQueryRunner runner, Dataset dataset) {
        return String.valueOf(
                runner.execute(
                                "SELECT to_hex(checksum(ROW(id, label))) FROM pixels.s.t WHERE "
                                        + dataset.targetPredicate())
                        .getOnlyValue());
    }

    private static long targetCount(DistributedQueryRunner runner, Dataset dataset) {
        return scalar(
                runner,
                "SELECT count(*) FROM pixels.s.t WHERE " + dataset.targetPredicate());
    }

    private static long millis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static long metric(Path control, String name) throws Exception {
        Properties properties = new Properties();
        try (java.io.InputStream input =
                Files.newInputStream(control.resolve("status.properties"))) {
            properties.load(input);
        }
        return Long.parseLong(properties.getProperty(name));
    }

    private static long runDataset(
            DistributedQueryRunner runner, Dataset dataset, int visibilityPollMillis)
            throws Exception {
        SourceResult source = sourceResult(runner, dataset);
        if (source.rows() <= 0) {
            throw new AssertionError(dataset.name() + " source is empty");
        }
        if (targetCount(runner, dataset) != 0) {
            throw new AssertionError(dataset.name() + " target tag is not empty");
        }

        AtomicBoolean stop = new AtomicBoolean();
        AtomicBoolean partialVisibility = new AtomicBoolean();
        AtomicLong firstVisible = new AtomicLong(Long.MIN_VALUE);
        CountDownLatch observerReady = new CountDownLatch(1);
        ExecutorService observer = Executors.newSingleThreadExecutor();
        Future<?> observation =
                observer.submit(
                        () -> {
                            boolean first = true;
                            try {
                                while (!stop.get()) {
                                    long count = targetCount(runner, dataset);
                                    if (first) {
                                        observerReady.countDown();
                                        first = false;
                                    }
                                    if (count != 0 && count != source.rows()) {
                                        partialVisibility.set(true);
                                    }
                                    if (count == source.rows()) {
                                        firstVisible.compareAndSet(Long.MIN_VALUE, System.nanoTime());
                                        return;
                                    }
                                    Thread.sleep(visibilityPollMillis);
                                }
                            } catch (Throwable failure) {
                                throw new RuntimeException(failure);
                            } finally {
                                observerReady.countDown();
                            }
                        });
        if (!observerReady.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError(dataset.name() + " visibility observer did not start");
        }

        long insertStart = System.nanoTime();
        MaterializedResult insert =
                runner.execute(
                        "INSERT INTO pixels.s.t SELECT " + dataset.projection()
                                + " FROM " + dataset.source());
        long insertReturned = System.nanoTime();
        long affected = insert.getUpdateCount().orElseThrow();
        if (affected != source.rows()) {
            throw new AssertionError(
                    dataset.name() + " affected=" + affected + ", source=" + source.rows());
        }

        long immediateStart = System.nanoTime();
        long immediateCount = targetCount(runner, dataset);
        String immediateChecksum = targetChecksum(runner, dataset);
        long immediateEnd = System.nanoTime();
        if (immediateCount != source.rows() || !source.checksum().equals(immediateChecksum)) {
            throw new AssertionError(
                    dataset.name() + " immediate visibility mismatch: rows=" + immediateCount
                            + ", checksum=" + immediateChecksum + ", expected=" + source.checksum());
        }

        observation.get(30, TimeUnit.SECONDS);
        stop.set(true);
        observer.shutdownNow();
        if (partialVisibility.get()) {
            throw new AssertionError(dataset.name() + " exposed a partial transaction");
        }
        long visibleAt = firstVisible.get();
        if (visibleAt == Long.MIN_VALUE) {
            throw new AssertionError(dataset.name() + " observer never saw the committed rows");
        }

        long insertNanos = insertReturned - insertStart;
        double rowsPerSecond = source.rows() * 1_000_000_000.0 / insertNanos;
        String result = String.format(
                java.util.Locale.ROOT,
                "TPC_INSERT_RESULT dataset=%s rows=%d sourceScanMs=%d insertMs=%d rowsPerSecond=%.2f "
                        + "observerVisibleRelativeToReturnMs=%d immediateVisibilityUpperBoundMs=%d "
                        + "immediateQueryMs=%d partialRowsObserved=0 checksum=%s%n",
                dataset.name(),
                source.rows(),
                source.elapsedMillis(),
                millis(insertNanos),
                rowsPerSecond,
                millis(visibleAt - insertReturned),
                millis(immediateEnd - insertReturned),
                millis(immediateEnd - immediateStart),
                source.checksum());
        System.out.println(result);
        return source.rows();
    }

    public static void main(String[] args) throws Exception {
        Path control = Paths.get(args[0]);
        int workers = positiveInteger("INSERT_BENCHMARK_WORKERS", 2);
        int pollMillis = positiveInteger("INSERT_BENCHMARK_VISIBILITY_POLL_MS", 25);
        String tpchSchema = identifier("TPCH_SCHEMA", "tiny");
        String tpcdsSchema = identifier("TPCDS_SCHEMA", "tiny");

        List<URL> pluginUrls = new ArrayList<>();
        for (String path :
                Files.readString(Paths.get(args[1])).trim().split(java.io.File.pathSeparator)) {
            pluginUrls.add(Paths.get(path).toUri().toURL());
        }
        try (PluginClassLoader pluginLoader =
                        PluginManager.createClassLoader("pixels-tpc-insert", pluginUrls);
                DistributedQueryRunner runner =
                        DistributedQueryRunner.builder(
                                        testSessionBuilder()
                                                .setCatalog("pixels")
                                                .setSchema("s")
                                                .build())
                                .setWorkerCount(workers)
                                .build()) {
            for (io.trino.server.testing.TestingTrinoServer server : runner.getServers()) {
                server.getInstance(com.google.inject.Key.get(HandleResolver.class))
                        .registerClassLoader(pluginLoader);
            }
            Plugin pixels =
                    (Plugin)
                            pluginLoader
                                    .loadClass("io.pixelsdb.pixels.trino.PixelsPlugin")
                                    .getConstructor()
                                    .newInstance();
            try (io.trino.spi.classloader.ThreadContextClassLoader ignored =
                    new io.trino.spi.classloader.ThreadContextClassLoader(pluginLoader)) {
                runner.installPlugin(pixels);
            }
            runner.createCatalog(
                    "pixels",
                    "pixels",
                    Map.of("cloud.function.switch", "off", "insert.enabled", "true"));
            runner.installPlugin(new TpchPlugin());
            runner.createCatalog("tpch", "tpch", Map.of("tpch.splits-per-node", "4"));
            runner.installPlugin(new TpcdsPlugin());
            runner.createCatalog("tpcds", "tpcds");

            Dataset tpch =
                    new Dataset(
                            "tpch." + tpchSchema + ".lineitem",
                            "tpch." + tpchSchema + ".lineitem",
                            "orderkey, CAST('tpch:' || comment AS varchar)",
                            "label LIKE 'tpch:%'");
            Dataset tpcds =
                    new Dataset(
                            "tpcds." + tpcdsSchema + ".store_sales",
                            "tpcds." + tpcdsSchema + ".store_sales",
                            "ss_ticket_number, CAST('tpcds:' || COALESCE(CAST(ss_item_sk AS varchar), '<null>') AS varchar)",
                            "label LIKE 'tpcds:%'");

            long tpchRows = runDataset(runner, tpch, pollMillis);
            long tpcdsRows = runDataset(runner, tpcds, pollMillis);
            long totalRows = tpchRows + tpcdsRows;
            if (scalar(runner, "SELECT count(*) FROM pixels.s.t") != totalRows) {
                throw new AssertionError("combined target row count mismatch");
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while ((metric(control, "pixelsFiles") == 0
                            || metric(control, "activeTransactions") != 0)
                    && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            if (metric(control, "pixelsFiles") == 0) {
                throw new AssertionError("benchmark did not materialize a Pixels file");
            }
            if (metric(control, "activeTransactions") != 0) {
                throw new AssertionError("benchmark transactions were not checkpointed and retired");
            }
            if (targetCount(runner, tpch) != tpchRows
                    || targetCount(runner, tpcds) != tpcdsRows) {
                throw new AssertionError("file/buffer handoff changed the target multiset");
            }
            System.out.println(
                    "TPC_INSERT_BENCHMARK_PASS tpchRows=" + tpchRows
                            + " tpcdsRows=" + tpcdsRows
                            + " totalRows=" + totalRows
                            + " workers=" + workers);
        }
    }
}
