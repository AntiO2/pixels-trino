package io.pixelsdb.pixels.trino.testing;

import io.trino.metadata.HandleResolver;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.testing.DistributedQueryRunner;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.trino.testing.TestingSession.testSessionBuilder;

/** Runs the normal JDBC all-column benchmark against the isolated SQL fixture. */
public final class AllTpcTablesBenchmark {
    private static final int WORKERS = 2;
    private static final String SOURCE_SPLITS_PER_NODE = "4";

    public static void main(String[] args) throws Exception {
        Properties status = new Properties();
        try (var input = Files.newInputStream(Path.of(args[0], "status.properties"))) {
            status.load(input);
        }
        List<URL> urls = new ArrayList<>();
        for (String entry : Files.readString(Path.of(args[1])).trim().split(java.io.File.pathSeparator)) {
            urls.add(Path.of(entry).toUri().toURL());
        }
        try (PluginClassLoader loader = PluginManager.createClassLoader("pixels-all-tpc", urls);
                DistributedQueryRunner runner = DistributedQueryRunner.builder(
                                testSessionBuilder().setCatalog("pixels").setSchema("s").build())
                        .setWorkerCount(WORKERS).build()) {
            for (var server : runner.getServers()) {
                server.getInstance(com.google.inject.Key.get(HandleResolver.class)).registerClassLoader(loader);
            }
            Plugin plugin = (Plugin) loader.loadClass("io.pixelsdb.pixels.trino.PixelsPlugin")
                    .getConstructor().newInstance();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(loader)) {
                runner.installPlugin(plugin);
            }
            runner.createCatalog("pixels", "pixels",
                    Map.of("cloud.function.switch", "off", "insert.enabled", "true"));
            runner.installPlugin(new TpchPlugin());
            runner.createCatalog("tpch", "tpch", Map.of("tpch.splits-per-node", SOURCE_SPLITS_PER_NODE));
            runner.installPlugin(new TpcdsPlugin());
            runner.createCatalog("tpcds", "tpcds");
            String jdbcUrl = runner.getCoordinator().getBaseUrl().toString()
                    .replaceFirst("^http", "jdbc:trino") + "/pixels/s";
            AllTpcTablesInsert.main(new String[] {
                    jdbcUrl, Path.of(status.getProperty("dataRoot"), "all-tpc").toString(),
                    "tpc_insert_all", "pixels"});
        }
    }
}
