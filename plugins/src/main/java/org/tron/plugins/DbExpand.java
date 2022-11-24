package org.tron.plugins;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.StringUtils;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;


@CommandLine.Command(name = "ep", aliases = "expand",
        description = "expand db size .")
public class DbExpand implements Callable<Integer> {

    private static final String PROPERTIES_CONFIG_KEY = "storage.properties";
    private static final String DB_DIRECTORY_CONFIG_KEY = "storage.db.directory";
    private static final String DEFAULT_DB_DIRECTORY = "database";
    private static final String NAME_CONFIG_KEY = "name";
    private static final String PATH_CONFIG_KEY = "path";
    private static final String RATE_CONFIG_KEY = "rate";
    private static final int BATCH = 256;
    private Random random = new Random(System.currentTimeMillis());


    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--database-directory"},
            defaultValue = "output-directory",
            converter = Db.PathConverter.class,
            description = "database directory path. Default: ${DEFAULT-VALUE}")
    static Path database;

    @CommandLine.Option(names = {"-c", "--config"},
            defaultValue = "config.conf",
            converter = ConfigConverter.class,
            order = Integer.MAX_VALUE,
            description = " config file. Default: ${DEFAULT-VALUE}")
    Config config;

    @CommandLine.Option(names = {"-h", "--help"}, help = true, description = "display a help message")
    boolean help;

    @Override
    public Integer call() {
        if (help) {
            spec.commandLine().usage(System.out);
            return 0;
        }
        List<? extends Config> dbs = config.getConfigList(PROPERTIES_CONFIG_KEY).stream()
                .filter(c -> c.hasPath(NAME_CONFIG_KEY) && c.hasPath(PATH_CONFIG_KEY) && c.hasPath(RATE_CONFIG_KEY))
                .collect(Collectors.toList());
        long start = System.currentTimeMillis();
        ProgressBar.wrap(dbs.stream(), "Expand task").forEach(this::run);
        long cost = System.currentTimeMillis() - start;
        spec.commandLine().getOut().println(String.format("Expand db done,cost:%s seconds", cost / 1000));
        return 0;
    }

    private void run(Config c) {
        int rate = c.getInt(RATE_CONFIG_KEY);
        String name = c.getString(NAME_CONFIG_KEY);
        String dbPath = config.hasPath(DB_DIRECTORY_CONFIG_KEY)
                ? config.getString(DB_DIRECTORY_CONFIG_KEY) : DEFAULT_DB_DIRECTORY;
        long start = System.currentTimeMillis();
        DB levelDb = null;
        try {
            levelDb = openLevelDb(Paths.get(database.toString(),dbPath,c.getString(PATH_CONFIG_KEY)));
        } catch (Exception e) {
            spec.commandLine().getErr().println(String.format("Open db %s error %s."
                    , name, e.getStackTrace()));
            e.printStackTrace();
        }
        doExpand(levelDb, name, rate);
        long cost = System.currentTimeMillis() - start;
        spec.commandLine().getOut().println(String.format("Expand all db %s done,cost:%s seconds", name, cost / 1000.0));
    }

    private void doExpand(DB levelDb, String name, int rate) {
        long expandKeyCount = 0;
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = levelDb.iterator(
                new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                addShuffleKv(entry, rate, keys, values);
                expandKeyCount += rate;
                if (keys.size() >= BATCH) {
                    try {
                        batchInsert(levelDb, keys, values);
                    } catch (Exception e) {
                        spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                                , name, e.getStackTrace()));
                    }
                }
            }

            if (!keys.isEmpty()) {
                try {
                    batchInsert(levelDb, keys, values);
                } catch (Exception e) {
                    spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                            , name, e.getStackTrace()));
                }
            }
        } catch (Exception e) {
            spec.commandLine().getErr().println(String.format("Expand %s error %s."
                    , name, e.getStackTrace()));
        } finally {
            try {
                levelDb.close();
            } catch (Exception e1) {
                spec.commandLine().getErr().println(String.format("Close %s error %s."
                        , name, e1.getStackTrace()));
            }
            spec.commandLine().getOut().println(String.format("Expand db %s done,expand count:%s", name, expandKeyCount));
        }
    }

    private void addShuffleKv(Map.Entry<byte[], byte[]> entry, int rate, List<byte[]> keys, List<byte[]> values) {
        for (int i = rate; i > 0; i--) {
            byte[] key = shuffleBytes(entry.getKey());
            byte[] value = entry.getValue();
            keys.add(key);
            values.add(value);
        }
    }

    private byte[] shuffleBytes(byte[] item) {
        int posBegin = random.nextInt(item.length);
        int posEnd = random.nextInt(item.length);
        byte temp = item[posBegin];
        item[posBegin] = item[posEnd];
        item[posEnd] = temp;
        byte[] copyItem = new byte[item.length];
        System.arraycopy(item,0,copyItem,0,copyItem.length);
        return copyItem;
    }

    private void batchInsert(DB db, List<byte[]> keys, List<byte[]> values)
            throws Exception {
        WriteBatch batch = db.createWriteBatch();
        try {
            for (int i = 0; i < keys.size(); i++) {
                byte[] k = keys.get(i);
                byte[] v = values.get(i);
                batch.put(k, v);
            }
            db.write(batch);
        } finally {
            batch.close();
        }
        keys.clear();
        values.clear();
    }

    public DB openLevelDb(Path p) throws Exception {
        // 暂不考虑特殊库
        DB database = factory.open(p.toFile(), getDefaultLevelDbOptions());
        return database;
    }

    public static org.iq80.leveldb.Options getDefaultLevelDbOptions() {
        org.iq80.leveldb.Options dbOptions = new org.iq80.leveldb.Options();
        dbOptions.createIfMissing(true);
        dbOptions.paranoidChecks(true);
        dbOptions.verifyChecksums(true);
        dbOptions.compressionType(CompressionType.SNAPPY);
        dbOptions.blockSize(4 * 1024);
        dbOptions.writeBufferSize(10 * 1024 * 1024);
        dbOptions.cacheSize(10 * 1024 * 1024L);
        dbOptions.maxOpenFiles(1000);
        dbOptions.maxBatchSize(64_000);
        dbOptions.maxManifestSize(128);
        dbOptions.fast(false);
        return dbOptions;
    }

    static class ConfigConverter implements CommandLine.ITypeConverter<Config> {
        private final Exception notFind =
                new IllegalArgumentException("There is no database to be expanded,please check.");

        ConfigConverter() {
        }

        @Override
        public Config convert(String value) throws Exception {
            Config config = parseConfig(value);
            checkConfigValid(config);
            return config;
        }

        private Config parseConfig(String value) throws IOException {
            File file = Paths.get(value).toFile();
            if ((!file.exists()) || (!file.isFile())) {
                throw new IOException("DB config [" + value + "] not exist!");
            }
            return ConfigFactory.parseFile(Paths.get(value).toFile());
        }

        private void checkConfigValid(Config config) throws Exception {
            if (!config.hasPath(PROPERTIES_CONFIG_KEY)) {
                throw notFind;
            }
            List<? extends Config> dbs = config.getConfigList(PROPERTIES_CONFIG_KEY).stream()
                    .filter(c -> c.hasPath(NAME_CONFIG_KEY) && c.hasPath(PATH_CONFIG_KEY) && c.hasPath(RATE_CONFIG_KEY))
                    .collect(Collectors.toList());
            if (dbs.isEmpty()) {
                throw notFind;
            }
            Set<String> toBeExpand = new HashSet<>();
            for (Config c : dbs) {
                if (StringUtils.isEmpty(c.getString(PATH_CONFIG_KEY))) {
                    continue;
                }
                if (!toBeExpand.add(c.getString(PATH_CONFIG_KEY))) {
                    throw new IllegalArgumentException(
                            "DB config has duplicate key:[" + c.getString(NAME_CONFIG_KEY)
                                    + "],please check! ");
                }
            }
        }
    }

}
