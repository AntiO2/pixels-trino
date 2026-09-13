package io.pixelsdb.pixels.trino.testing;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** Normal-server smoke test for every table and scalar type in TPC-H and TPC-DS. */
public final class AllTpcTablesInsert {
    private record Column(String name, String type) {}

    private record Dataset(String catalog, String schema, List<String> tables) {}

    private static final Dataset TPCH =
            new Dataset(
                    "tpch",
                    "tiny",
                    List.of("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"));

    private static final Dataset TPCDS =
            new Dataset(
                    "tpcds",
                    "tiny",
                    List.of(
                            "call_center", "catalog_page", "catalog_returns", "catalog_sales",
                            "customer", "customer_address", "customer_demographics", "date_dim",
                            "household_demographics", "income_band", "inventory", "item",
                            "promotion", "reason", "ship_mode", "store", "store_returns",
                            "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
                            "web_sales", "web_site"));

    private static String quote(String value) {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static String qualified(String... parts) {
        return java.util.Arrays.stream(parts)
                .map(AllTpcTablesInsert::quote)
                .collect(java.util.stream.Collectors.joining("."));
    }

    private static List<Column> describe(Statement statement, String source) throws Exception {
        List<Column> columns = new ArrayList<>();
        try (ResultSet result = statement.executeQuery("DESCRIBE " + source)) {
            while (result.next()) {
                columns.add(new Column(result.getString(1), result.getString(2)));
            }
        }
        if (columns.isEmpty()) {
            throw new AssertionError("No columns for " + source);
        }
        return columns;
    }

    private static long count(Statement statement, String table) throws Exception {
        try (ResultSet result = statement.executeQuery("SELECT count(*) FROM " + table)) {
            if (!result.next()) {
                throw new AssertionError("Missing count result for " + table);
            }
            return result.getLong(1);
        }
    }

    private static String checksum(Statement statement, String table, List<Column> columns)
            throws Exception {
        String row = columns.stream()
                .map(Column::name)
                .map(AllTpcTablesInsert::quote)
                .collect(java.util.stream.Collectors.joining(", "));
        try (ResultSet result = statement.executeQuery(
                "SELECT to_hex(checksum(ROW(" + row + "))) FROM " + table)) {
            if (!result.next()) {
                throw new AssertionError("Missing checksum result for " + table);
            }
            return result.getString(1);
        }
    }

    private static long copy(
            Statement statement,
            Dataset dataset,
            String targetSchema,
            Path dataRoot,
            String table,
            boolean verifyOnly)
            throws Exception {
        String source = qualified(dataset.catalog(), dataset.schema(), table);
        String targetName = dataset.catalog() + "_" + table;
        String target = qualified("pixels", targetSchema, targetName);
        List<Column> columns = describe(statement, source);
        long sourceRows = count(statement, source);
        String sourceChecksum = checksum(statement, source, columns);
        long start = System.nanoTime();
        long affected = sourceRows;
        if (!verifyOnly) {
            String definitions = columns.stream()
                    .map(column -> quote(column.name()) + " " + column.type())
                    .collect(java.util.stream.Collectors.joining(", "));
            String path = dataRoot.resolve(targetSchema).resolve(targetName).toUri().toString();
            statement.execute(
                    "CREATE TABLE " + target + " (" + definitions
                            + ") WITH (storage='file', paths='"
                            + path.replace("'", "''") + "')");
            affected = statement.executeUpdate(
                    "INSERT INTO " + target + " SELECT * FROM " + source);
        }
        long elapsed = System.nanoTime() - start;
        long targetRows = count(statement, target);
        String targetChecksum = checksum(statement, target, columns);
        if (affected != sourceRows || targetRows != sourceRows
                || !sourceChecksum.equals(targetChecksum)) {
            throw new AssertionError(
                    source + " mismatch: sourceRows=" + sourceRows + ", affected=" + affected
                            + ", targetRows=" + targetRows + ", sourceChecksum=" + sourceChecksum
                            + ", targetChecksum=" + targetChecksum);
        }
        System.out.printf(
                Locale.ROOT,
                "TPC_TABLE_%s_PASS source=%s rows=%d columns=%d elapsedMs=%d checksum=%s%n",
                verifyOnly ? "RECOVERY" : "INSERT",
                source,
                sourceRows,
                columns.size(),
                TimeUnit.NANOSECONDS.toMillis(elapsed),
                sourceChecksum);
        return sourceRows;
    }

    private static String requiredIdentifier(String value, String description) {
        if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(description + " is not a SQL identifier: " + value);
        }
        return value;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            throw new IllegalArgumentException(
                    "Usage: AllTpcTablesInsert <jdbc-url> <data-root> [target-schema] [user]");
        }
        String targetSchema = requiredIdentifier(
                args.length >= 3 ? args[2] : "tpc_insert_all", "target schema");
        String user = args.length == 4 ? args[3] : System.getProperty("user.name", "pixels");
        Path dataRoot = Path.of(args[1]).toAbsolutePath().normalize();
        boolean verifyOnly = Boolean.parseBoolean(
                System.getenv().getOrDefault("ALL_TPC_VERIFY_ONLY", "false"));
        long rows = 0;
        int tables = 0;
        try (Connection connection = DriverManager.getConnection(args[0], user, null);
                Statement statement = connection.createStatement()) {
            if (!verifyOnly) {
                statement.execute("CREATE SCHEMA " + qualified("pixels", targetSchema));
            }
            for (Dataset dataset : List.of(TPCH, TPCDS)) {
                for (String table : dataset.tables()) {
                    rows += copy(
                            statement, dataset, targetSchema, dataRoot, table, verifyOnly);
                    tables++;
                }
            }
        }
        System.out.printf(
                Locale.ROOT,
                "ALL_TPC_TABLES_%s_PASS tables=%d rows=%d targetSchema=%s dataRoot=%s%n",
                verifyOnly ? "RECOVERY" : "INSERT",
                tables,
                rows,
                targetSchema,
                dataRoot);
    }
}
