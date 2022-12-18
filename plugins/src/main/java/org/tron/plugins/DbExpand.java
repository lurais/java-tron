package org.tron.plugins;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.StringUtils;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;


@CommandLine.Command(name = "ep", aliases = "expand",
        description = "expand db size .")
public class DbExpand implements Callable<Integer> {

    private static final String PROPERTIES_CONFIG_KEY = "storage.expandproperties";
    private static final String DB_DIRECTORY_CONFIG_KEY = "storage.db.directory";
    private static final String DEFAULT_DB_DIRECTORY = "database";
    private static final String NAME_CONFIG_KEY = "name";
    private static final String PATH_CONFIG_KEY = "path";
    private static final String SECOND_PATH_CONFIG_KEY = "secondPath";
    private static final String RATE_CONFIG_KEY = "rate";
    private static final String OP_CONFIG_KEY = "op";// 0直接膨胀 1混合 2先冷后热
    private static final int BATCH = 1024;
    private Random random = new Random(System.currentTimeMillis());
    public static final byte ADD_PRE_FIX_BYTE_MAINNET = (byte) 0x41;
    private static Set<String> structureNames = new HashSet<>();




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


    static {
        structureNames.addAll(Arrays.asList("account"));
    }

    @Override
    public Integer call() {
        if (help) {
            spec.commandLine().usage(System.out);
            return 0;
        }
        List<? extends Config> dbConfigs = config.getConfigList(PROPERTIES_CONFIG_KEY).stream()
                .filter(c -> c.hasPath(NAME_CONFIG_KEY) && c.hasPath(PATH_CONFIG_KEY) && c.hasPath(RATE_CONFIG_KEY))
                .collect(Collectors.toList());
        long start = System.currentTimeMillis();
        dbConfigs.parallelStream().forEach(this::run);
        //ProgressBar.wrap(dbConfigs.stream(), "Expand task").forEach(this::run);
        long cost = System.currentTimeMillis() - start;
        spec.commandLine().getOut().println(String.format("Expand all db done,cost:%s seconds", cost / 1000));
        return 0;
    }

    private void run(Config c) {
        int op = c.getInt(OP_CONFIG_KEY);
        int rate = c.getInt(RATE_CONFIG_KEY);
        String name = c.getString(NAME_CONFIG_KEY);
        String dbPath = config.hasPath(DB_DIRECTORY_CONFIG_KEY)
                ? config.getString(DB_DIRECTORY_CONFIG_KEY) : DEFAULT_DB_DIRECTORY;
        long start = System.currentTimeMillis();
        DB levelDb = null, secondDb = null;
        try {
            levelDb = openLevelDb(Paths.get(database.toString(),dbPath,c.getString(PATH_CONFIG_KEY)));
            secondDb = parseSecondDb(op,c,dbPath);
        } catch (Exception e) {
            spec.commandLine().getErr().println(String.format("Open db %s error."
                    , name, e.getStackTrace()));
            e.printStackTrace();
        }
        spec.commandLine().getOut().println(String.format("Expand db %s begin......", name));
        doExpand(levelDb,secondDb, name, rate, op);
        long cost = System.currentTimeMillis() - start;
        spec.commandLine().getOut().println(String.format("Expand db %s done,cost:%s seconds", name, cost / 1000.0));
    }

    private void mergeDb(DB originDb, DB targetDb) {
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = originDb.iterator(
                new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                keys.add(entry.getKey());
                values.add(entry.getValue());
                if (keys.size() >= BATCH) {
                    try {
                        batchInsert(targetDb, keys, values);
                    } catch (Exception e) {
                        spec.commandLine().getErr().println(String.format("Batch insert kv error %s."
                                , e.getMessage()));
                    }
                }
            }

            if (!keys.isEmpty()) {
                try {
                    batchInsert(targetDb, keys, values);
                } catch (Exception e) {
                    spec.commandLine().getErr().println(String.format("Batch insert kv error %s."
                            , e.getMessage()));
                }
            }
        } catch (Exception e) {
            spec.commandLine().getErr().println(String.format("Merge error %s."
                    , e.getMessage()));
        } finally {
            spec.commandLine().getOut().println("Merge db done");
        }
    }

    private DB parseSecondDb(int op, Config c, String dbPath) throws Exception {
        //直接膨胀
        if(op==0){
            return null;
        }
        //先冷后热（扩展冷库，merge）
        //混合，直接新库 SECOND_PATH_CONFIG_KEY
        //导出新库
        return openLevelDb(Paths.get(database.toString(),dbPath,c.getString(NAME_CONFIG_KEY)+"_second"));
    }

    private void doExpand(DB levelDb,DB secondDb, String name, int rate,int op) {
        long expandKeyCount = 0;
        List<byte[]> statKeys = new ArrayList<>(100);
        Double statCount = 0.0;
        double statAllTime = 0L;
        long iterCount = 0L;
        Boolean hasPrintStat = Boolean.FALSE;
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);
        Set<String> levelPrintSet = new HashSet<>();
        String maxLevel = "--------------------------------------------------";

        double currentMax = 0.0;

        try (DBIterator levelIterator = levelDb.iterator(
                new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                iterCount+=1;
                if(statCount>=1000.0 && !hasPrintStat){
                    String currentLevel = levelDb.getProperty("leveldb.stats");
                    maxLevel=parseLevel(currentLevel.split("\n")[currentLevel.split("\n").length-1]).trim();
                    statAllTime = statQueryTime(levelDb,statKeys);
                    spec.commandLine().getOut().println(name + " leveldb stat ,currentLevel="+maxLevel+",avg get:"+statAllTime/statCount+", statCount="+statCount+", iterCount="+iterCount);
                    hasPrintStat=Boolean.TRUE;
                }
                addShuffleKv(op,name,entry, rate, keys, values,statCount);
                for(byte[] key:keys) {
                    if (statCount < 1000.0) {
                        statKeys.add(key);
                        statCount++;
                    }
                }
                expandKeyCount += rate;
                if (keys.size() >= BATCH) {
                    try {
                        currentMax = statQueryTime(secondDb,keys)/keys.size() > currentMax? statQueryTime(secondDb,keys)/keys.size():currentMax;
                        List<byte[]> tempKeys = copyList(keys);
                        batchInsert(op==0?levelDb:secondDb, keys, values);
                        statQueryTime(secondDb,tempKeys);
                        String currentLevel = secondDb.getProperty("leveldb.stats");
                        maxLevel=parseLevel(currentLevel.split("\n")[currentLevel.split("\n").length-1]);
                        if(!levelPrintSet.contains(maxLevel)){
                            StringBuilder sb = new StringBuilder();
                            levelPrintSet.stream().forEach(item->sb.append(item+","));
                            statAllTime = statQueryTime(secondDb,statKeys);
                            spec.commandLine().getOut().println(name + " leveldb level up,currentLevel="+maxLevel+",avg get:"+statAllTime/statCount+", statCount="+statCount+", iterCount="+iterCount+", set="+sb.toString());
                            levelPrintSet.add(maxLevel);
                        }
                    } catch (Exception e) {
                        spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                                , name, e.getStackTrace()));
                    }
                }
            }

            statAllTime = statQueryTime(secondDb,statKeys);
            spec.commandLine().getOut().println(name + " leveldb level final,currentLevel="+maxLevel+",avg get:"+statAllTime/statCount+", statCount="+statCount+", iterCount="+iterCount);

            if (!keys.isEmpty()) {
                try {
                    batchInsert(op==0?levelDb:secondDb, keys, values);
                } catch (Exception e) {
                    spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                            , name, e.getStackTrace()));
                }
            }
            if(op==2){
                mergeDb(levelDb,secondDb);
            }
        } catch (Exception e) {
            spec.commandLine().getErr().println(String.format("Expand %s error %s."
                    , name, e.getStackTrace()));
        } finally {
            try {
                levelDb.close();
                if(secondDb!=null) {
                    secondDb.close();
                }
                JniDBFactory.popMemoryPool();
            } catch (Exception e1) {
                spec.commandLine().getErr().println(String.format("Close %s error %s."
                        , name, e1.getStackTrace()));
            }
            spec.commandLine().getOut().println(String.format("Expand db %s done,expand count:%s", name, expandKeyCount));
        }
    }

    private String parseLevel(String lastLine) {
        if(lastLine==null||lastLine.length()<=0){
            return "";
        }
        return lastLine.trim().charAt(0)+"";
    }

    private double statQueryTime(DB secondDb, List<byte[]> statKeys) {
        double result = 0L;
        for(byte[] key:statKeys){
            long current = System.nanoTime();
            byte[] cur = secondDb.get(key);
            result+=(System.nanoTime() - current)/1000000.0;
        }
        return result;
    }

    private boolean levelUp(DB secondDb, String maxLevel) {
        String currentLevel = secondDb.getProperty("leveldb.stats");
        if(currentLevel==null||currentLevel.isEmpty()) return Boolean.FALSE;
        if(parseLevel(currentLevel.split("\n")[currentLevel.split("\n").length-1]).trim().equals(maxLevel)){
            return Boolean.FALSE;
        }else{
            return Boolean.TRUE;
        }
    }

    private void addShuffleKv(int op, String name, Map.Entry<byte[], byte[]> entry, int rate,
                              List<byte[]> keys, List<byte[]> values,double statCount) {
        if(op==3){
            keys.add(entry.getKey());
            values.add(entry.getValue());
            if(statCount<1000){
                for (int i = rate; i > 0; i--) {
                    byte[] key = structureKey(name) ? generateStructure(name): shuffleBytes(entry.getKey());
                    byte[] value = entry.getValue();
                    keys.add(key);
                    values.add(value);
                }
            }
            return;
        }
        for (int i = rate; i > 0; i--) {
            byte[] key = structureKey(name) ? generateStructure(name): shuffleBytes(entry.getKey());
            byte[] value = entry.getValue();
            keys.add(key);
            values.add(value);
        }
        // 混合
        if(op==1){
            keys.add(entry.getKey());
            values.add(entry.getValue());
        }
    }

    private byte[] generateStructure(String name) {
        switch (name){
            case "account":
                return generateAddress(32);
            default:
                throw new RuntimeException("Db not supported!");
        }
    }

    private boolean structureKey(String name) {
        return structureNames.contains(name);
    }

    public static byte[] generateAddress(int length) {
        // generate the random number
        byte[] result = new byte[length];
        new Random().nextBytes(result);
        result[0] = ADD_PRE_FIX_BYTE_MAINNET;
        return result;
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

    private List<byte[]> copyList(List<byte[]> keys){
        List<byte[]> res = Lists.newArrayList();
        for(byte[] key:keys){
            byte[] copyItem = new byte[key.length];
            System.arraycopy(key,0,copyItem,0,copyItem.length);
            res.add(copyItem);
        }
        return res;
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
        //特殊库处理
        return factory.open(p.toFile(), getDefaultLevelDbOptions());
    }

    public static org.iq80.leveldb.Options getDefaultLevelDbOptions() {
        org.iq80.leveldb.Options dbOptions = new org.iq80.leveldb.Options();
        dbOptions.createIfMissing(true);
        dbOptions.paranoidChecks(true);
        dbOptions.verifyChecksums(true);
        dbOptions.compressionType(CompressionType.SNAPPY);
        dbOptions.blockSize(4 * 1024);
        dbOptions.writeBufferSize(10 * 1024 * 1024);
        dbOptions.cacheSize(0L);
        dbOptions.maxOpenFiles(1000);
        dbOptions.maxBatchSize(64_000);
        dbOptions.maxManifestSize(128);
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
