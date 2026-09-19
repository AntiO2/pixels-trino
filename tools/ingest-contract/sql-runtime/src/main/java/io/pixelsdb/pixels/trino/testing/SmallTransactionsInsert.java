/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 * Pixels is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 */
package io.pixelsdb.pixels.trino.testing;

import io.trino.metadata.HandleResolver;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.testing.TestingSession.testSessionBuilder;

/** End-to-end Trino JDBC benchmark for independent small INSERT transactions. */
public final class SmallTransactionsInsert
{
    private static final int DEFAULT_TRANSACTIONS = 10_000;
    private static final int DEFAULT_ROWS_PER_TRANSACTION = 1;
    private static final int DEFAULT_CONCURRENCY = 16;
    private static final int DEFAULT_WARMUP_TRANSACTIONS = 100;
    private static final int DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 3_600;

    private enum TransactionMode
    {
        AUTOCOMMIT,
        EXPLICIT
    }

    private SmallTransactionsInsert() {}

    public static void main(String[] args) throws Exception
    {
        if (args.length == 2)
        {
            runEmbedded(args[1]);
            return;
        }
        if (args.length != 4)
        {
            throw new IllegalArgumentException(
                    "Usage: SmallTransactionsInsert jdbc-url schema table user");
        }
        String jdbcUrl = args[0];
        String schema = identifier(args[1]);
        String table = identifier(args[2]);
        String user = args[3];
        int transactions = positive("SMALL_INSERT_TRANSACTIONS", DEFAULT_TRANSACTIONS);
        int rowsPerTransaction = positive(
                "SMALL_INSERT_ROWS_PER_TRANSACTION", DEFAULT_ROWS_PER_TRANSACTION);
        int concurrency = positive("SMALL_INSERT_CONCURRENCY", DEFAULT_CONCURRENCY);
        int warmupTransactions = nonNegative(
                "SMALL_INSERT_WARMUP_TRANSACTIONS", DEFAULT_WARMUP_TRANSACTIONS);
        int visibilityTimeoutSeconds = positive(
                "SMALL_INSERT_VISIBILITY_TIMEOUT_SECONDS",
                DEFAULT_VISIBILITY_TIMEOUT_SECONDS);
        if (visibilityTimeoutSeconds > DEFAULT_VISIBILITY_TIMEOUT_SECONDS)
        {
            throw new IllegalArgumentException(
                    "SMALL_INSERT_VISIBILITY_TIMEOUT_SECONDS must not exceed "
                            + DEFAULT_VISIBILITY_TIMEOUT_SECONDS);
        }
        TransactionMode mode = TransactionMode.valueOf(
                environment("SMALL_INSERT_TRANSACTION_MODE", TransactionMode.AUTOCOMMIT.name())
                        .toUpperCase(Locale.ROOT));
        String qualifiedTable = "pixels." + quote(schema) + "." + quote(table);
        boolean createTable = Boolean.parseBoolean(
                environment("SMALL_INSERT_CREATE_TABLE", "true"));

        if (createTable)
        {
            try (Connection connection = connection(jdbcUrl, user);
                    Statement statement = connection.createStatement())
            {
                statement.execute("CREATE SCHEMA IF NOT EXISTS pixels." + quote(schema));
                statement.execute("CREATE TABLE " + qualifiedTable
                        + " (id BIGINT, payload VARCHAR)");
            }
        }

        runTransactions(
                jdbcUrl, user, qualifiedTable, mode, warmupTransactions,
                rowsPerTransaction, concurrency, -warmupTransactions);

        long startNanos = System.nanoTime();
        List<Long> latencies = runTransactions(
                jdbcUrl, user, qualifiedTable, mode, transactions,
                rowsPerTransaction, concurrency, 0);
        long acceptedNanos = System.nanoTime() - startNanos;
        long acceptedRows = Math.multiplyExact((long) transactions, rowsPerTransaction);

        long barrierStart = System.nanoTime();
        try (Connection connection = connection(jdbcUrl, user);
                Statement statement = connection.createStatement())
        {
            statement.execute("CALL pixels.system.flush_visible_barrier(timeout_seconds => "
                    + visibilityTimeoutSeconds + ")");
        }
        long visibleAfterAcceptedNanos = System.nanoTime() - barrierStart;

        long expectedCount = acceptedRows + Math.multiplyExact(
                (long) warmupTransactions, rowsPerTransaction);
        long expectedSum = expectedSum(transactions, rowsPerTransaction, 0)
                + expectedSum(warmupTransactions, rowsPerTransaction, -warmupTransactions);
        try (Connection connection = connection(jdbcUrl, user);
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(
                        "SELECT count(*), coalesce(sum(id), BIGINT '0') FROM " + qualifiedTable))
        {
            if (!result.next()
                    || result.getLong(1) != expectedCount
                    || result.getLong(2) != expectedSum)
            {
                throw new AssertionError("Small transaction result mismatch");
            }
        }

        Collections.sort(latencies);
        double acceptedSeconds = acceptedNanos / 1_000_000_000.0;
        double totalVisibleSeconds =
                (acceptedNanos + visibleAfterAcceptedNanos) / 1_000_000_000.0;
        String result = String.format(Locale.ROOT,
                "SMALL_TRANSACTION_INSERT_PASS mode=%s transactions=%d rowsPerTransaction=%d "
                        + "concurrency=%d acceptedRows=%d acceptedMs=%.3f acceptedRowsPerSecond=%.2f "
                        + "commitP50Ms=%.3f commitP95Ms=%.3f commitP99Ms=%.3f "
                        + "visibleAfterAcceptedMs=%.3f visibleRowsPerSecond=%.2f",
                mode, transactions, rowsPerTransaction, concurrency, acceptedRows,
                acceptedNanos / 1_000_000.0, acceptedRows / acceptedSeconds,
                percentileMillis(latencies, 50), percentileMillis(latencies, 95),
                percentileMillis(latencies, 99), visibleAfterAcceptedNanos / 1_000_000.0,
                acceptedRows / totalVisibleSeconds);
        System.out.println(result);
    }

    private static List<Long> runTransactions(
            String jdbcUrl,
            String user,
            String qualifiedTable,
            TransactionMode mode,
            int transactions,
            int rowsPerTransaction,
            int concurrency,
            int transactionBase) throws Exception
    {
        if (transactions == 0)
        {
            return new ArrayList<>();
        }
        AtomicLong next = new AtomicLong();
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>(transactions));
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(concurrency, transactions));
        List<Future<Void>> workers = new ArrayList<>();
        for (int clientId = 0; clientId < Math.min(concurrency, transactions); clientId++)
        {
            workers.add(executor.submit((Callable<Void>) () -> {
                try (Connection connection = connection(jdbcUrl, user);
                        Statement statement = connection.createStatement())
                {
                    connection.setAutoCommit(mode == TransactionMode.AUTOCOMMIT);
                    while (true)
                    {
                        long ordinal = next.getAndIncrement();
                        if (ordinal >= transactions)
                        {
                            return null;
                        }
                        long transactionId = transactionBase + ordinal;
                        String sql = insertSql(
                                qualifiedTable, transactionId, rowsPerTransaction);
                        long start = System.nanoTime();
                        try
                        {
                            statement.executeUpdate(sql);
                            if (mode == TransactionMode.EXPLICIT)
                            {
                                connection.commit();
                            }
                        }
                        catch (Exception e)
                        {
                            if (mode == TransactionMode.EXPLICIT)
                            {
                                connection.rollback();
                            }
                            throw e;
                        }
                        latencies.add(System.nanoTime() - start);
                    }
                }
            }));
        }
        executor.shutdown();
        for (Future<Void> worker : workers)
        {
            worker.get();
        }
        if (!executor.awaitTermination(1, TimeUnit.MINUTES))
        {
            throw new IllegalStateException("Small transaction workers did not terminate");
        }
        return latencies;
    }

    private static String insertSql(
            String qualifiedTable, long transactionId, int rows)
    {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(qualifiedTable)
                .append(" VALUES ");
        for (int row = 0; row < rows; row++)
        {
            if (row > 0)
            {
                sql.append(',');
            }
            long id = Math.addExact(Math.multiplyExact(transactionId, (long) rows), row);
            sql.append('(').append(id).append(",'small-transaction')");
        }
        return sql.toString();
    }

    private static long expectedSum(int transactions, int rows, int transactionBase)
    {
        long first = Math.multiplyExact((long) transactionBase, rows);
        long count = Math.multiplyExact((long) transactions, rows);
        return Math.multiplyExact(count, Math.addExact(Math.multiplyExact(2L, first), count - 1L)) / 2L;
    }

    private static Connection connection(String jdbcUrl, String user) throws Exception
    {
        return DriverManager.getConnection(jdbcUrl, user, null);
    }

    private static double percentileMillis(List<Long> sortedNanos, int percentile)
    {
        int index = Math.max(0, (int) Math.ceil(sortedNanos.size() * percentile / 100.0) - 1);
        return sortedNanos.get(index) / 1_000_000.0;
    }

    private static int positive(String name, int defaultValue)
    {
        int value = Integer.parseInt(environment(name, Integer.toString(defaultValue)));
        if (value <= 0)
        {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static int nonNegative(String name, int defaultValue)
    {
        int value = Integer.parseInt(environment(name, Integer.toString(defaultValue)));
        if (value < 0)
        {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static String environment(String name, String defaultValue)
    {
        return System.getenv().getOrDefault(name, defaultValue);
    }

    private static String identifier(String value)
    {
        if (!value.matches("[A-Za-z_][A-Za-z0-9_]*"))
        {
            throw new IllegalArgumentException("Invalid SQL identifier: " + value);
        }
        return value;
    }

    private static String quote(String value)
    {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static void runEmbedded(String pluginClasspath) throws Exception
    {
        List<URL> pluginUrls = new ArrayList<>();
        for (String path : Files.readString(Paths.get(pluginClasspath)).trim()
                .split(java.io.File.pathSeparator))
        {
            pluginUrls.add(Paths.get(path).toUri().toURL());
        }
        try (PluginClassLoader pluginLoader =
                        PluginManager.createClassLoader("pixels-small-insert", pluginUrls);
                DistributedQueryRunner runner = DistributedQueryRunner.builder(
                                testSessionBuilder().setCatalog("pixels").setSchema("s").build())
                        .setWorkerCount(2)
                        .build())
        {
            for (io.trino.server.testing.TestingTrinoServer server : runner.getServers())
            {
                server.getInstance(com.google.inject.Key.get(HandleResolver.class))
                        .registerClassLoader(pluginLoader);
            }
            Plugin plugin = (Plugin) pluginLoader
                    .loadClass("io.pixelsdb.pixels.trino.PixelsPlugin")
                    .getConstructor()
                    .newInstance();
            try (io.trino.spi.classloader.ThreadContextClassLoader ignored =
                    new io.trino.spi.classloader.ThreadContextClassLoader(pluginLoader))
            {
                runner.installPlugin(plugin);
            }
            runner.createCatalog("pixels", "pixels",
                    java.util.Map.of("cloud.function.switch", "off", "insert.enabled", "true"));
            String jdbcUrl = runner.getCoordinator().getBaseUrl().toString()
                    .replaceFirst("^http", "jdbc:trino") + "/pixels/s";
            main(new String[] {jdbcUrl, "s", "t", "pixels-small-insert"});
        }
    }
}
