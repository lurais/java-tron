package org.tron.plugins;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import ch.qos.logback.core.encoder.ByteArrayUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;


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
    private static final String LEVEL_ENGINE = "LEVELDB";
    private static final String ROCKS_ENGINE = "ROCKSDB";
    private static final int BATCH = 1000;
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

    Random r = new Random();


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
        if(op==20){
            doExpandLevelAndRocksDB(c, name, dbPath);
            return;
        }else if(op==21){
            doTestFileGetInRocksDB(c,name,dbPath);
            return;
        }
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
        if(op == 3){
            doTestTopKGet(levelDb,secondDb,name,rate);
        }else if(op==4) {
            doExpandReverse(levelDb,secondDb,name,rate);
        }else if(op==5) {
            doTestSerialGetInSecond(levelDb,levelDb,name);
        }else if(op==6){
            doExpandIter(levelDb,secondDb,name);
        }else if(op==7){
            doExportKeys(levelDb,secondDb,name);
        }else if(op==8){
            doTestFileDBGetInSecond(secondDb,name,LEVEL_ENGINE);
        }else{
            doExpand(levelDb, secondDb, name, rate, op,Paths.get(database.toString(), dbPath,
                    c.getString(PATH_CONFIG_KEY)+"_rdb_second"));
        }
        long cost = System.currentTimeMillis() - start;
        spec.commandLine().getOut().println(String.format("Expand db %s done,cost:%s seconds", name, cost / 1000.0));
    }

    private void doTestFileGetInRocksDB(Config c, String name, String dbPath) {
        try(
            RocksDB rdb = initDB(Paths.get(database.toString(), dbPath,
                c.getString(PATH_CONFIG_KEY)+"_second"));){
            doTestFileDBGetInSecond(rdb,name,ROCKS_ENGINE);
            return;
        }catch(Exception e){
            spec.commandLine().getErr().println(String.format("Open db %s error."
                , name, e.getStackTrace()));
            throw new RuntimeException(e);
        }
    }

    private void doExpandLevelAndRocksDB(Config c, String name, String dbPath) {
        try(DB leveldb = openLevelDb(Paths.get(database.toString(), dbPath,
            c.getString(PATH_CONFIG_KEY)));
            DB leveldbSecond = openLevelDb(Paths.get(database.toString(), dbPath,
                c.getString(PATH_CONFIG_KEY)+"_second"));
            DB leveldbThird = openLevelDb(Paths.get(database.toString(), dbPath,
                c.getString(PATH_CONFIG_KEY)+"_third"));
            RocksDB rdb = initDB(Paths.get(database.toString(), dbPath,
                c.getString(PATH_CONFIG_KEY)+"_rdb"));
            RocksDB rdbSecond = initDB(Paths.get(database.toString(), dbPath,
                c.getString(NAME_CONFIG_KEY)+"_rdb_second"));){
            migrate(leveldb,rdb,null);
            migrate(leveldbSecond,rdbSecond,leveldb);
            migrate(leveldb,rdbSecond,null);
            mergeDb(leveldbSecond,leveldbThird,leveldb);
            mergeDb(leveldb,leveldbThird,null);
            return;
        }catch(Exception e){
            spec.commandLine().getErr().println(String.format("Open db %s error."
                , name, e.getStackTrace()));
            throw new RuntimeException(e);
        }
    }

    /**
     * leveldb trans to rdb
     * @param leveldb source
     * @param rdb target
     * @param except not trans
     */
    private void migrate(DB leveldb, RocksDB rdb, DB except) {
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = leveldb.iterator(
            new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                if(except!=null && except.get(entry.getKey())!=null) {
                    continue;
                }
                keys.add(entry.getKey());
                values.add(entry.getValue());
                if (keys.size() >= BATCH) {
                    try {
                        batchInsert(rdb, keys, values);
                    } catch (Exception e) {
                        spec.commandLine().getErr().println(String.format("Batch insert kv error %s."
                            , e.getMessage()));
                    }
                }
            }

            if (!keys.isEmpty()) {
                try {
                    batchInsert(rdb, keys, values);
                } catch (Exception e) {
                    spec.commandLine().getErr().println(String.format("Batch insert kv error %s."
                        , e.getMessage()));
                }
            }
        } catch (Exception e) {
            spec.commandLine().getErr().println(String.format("Merge error %s."
                , e.getMessage()));
        } finally {
            spec.commandLine().getOut().println("Migrate db done");
        }
    }

    private void doTestFileDBGetInSecond(Object db, String name, String engine) {
        try{
        File current = new File(".");
        for(File file:getFileAll(current,new ArrayList<>(),"level")){
            doTestFileGet(db,name,file,engine);
        }
        }catch (IOException e){
            spec.commandLine().getErr().println(String.format("Open file %s error %s."
                    , name+"keys", e.getStackTrace()));
        }finally {
            try {
                if(engine.equals(LEVEL_ENGINE)){
                    ((DB)db).close();
                }else{
                    ((RocksDB)db).close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void doTestFileGet(Object db, String name,File keysFile,String engine) throws IOException {
        BufferedReader reader = null;
        String perline;
        List<byte[]> keys = new ArrayList<>(BATCH + 30);
        try {
            reader = new BufferedReader(new FileReader(keysFile));
            while ((perline = reader.readLine()) != null) {
                byte[] origin = ByteArrayUtil.hexStringToByteArray(perline);
                keys.add(origin);
                if (keys.size() % 50 == 0) {
                    keys.add(structureKey(name) ? generateStructure(name) : shuffleBytes(origin));
                }
                if (keys.size() >= BATCH) {
                    statGetPerformance(db, keysFile.getName(), keys,engine);
                    keys.clear();
                }
            }
            if (keys.size() > 0) {
                statGetPerformance(db, keysFile.getName(), keys,engine);
                keys.clear();
            }
        }catch (Exception e){
            if(reader!=null){
                reader.close();
            }
            throw e;
        }
    }

    public static ArrayList<File> getFileAll(File file,ArrayList<File> fileList,String pos) {
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                continue;
            } else {
                if(files[i].getName().contains(pos)) {
                    fileList.add(files[i]);
                }
            }
        }
        return fileList;
    }

    private void doExportKeys(DB levelDb, DB secondDb, String name){
        long allStat = 0;
        File keysFile = new File(name+"keys");
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(name + "keys"));
        }catch (IOException e){
            spec.commandLine().getErr().println(String.format("Open file %s error %s."
                    , name+"keys", e.getStackTrace()));
        }
        List<byte[]> keys = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = levelDb.iterator(
                new org.iq80.leveldb.ReadOptions().fillCache(true))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                if(r.nextInt(1000) < 10){
                    keys.add(entry.getKey());
                }
                if (keys.size() >= BATCH) {
                    try {
                        writeToFile(out,keys);
                        allStat+=keys.size();
                        keys.clear();
                    } catch (Exception e) {
                        spec.commandLine().getErr().println(String.format("Batch add keys %s error %s."
                                , name, e.getStackTrace()));
                    }
                }
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
                out.close();
                JniDBFactory.popMemoryPool();
            } catch (Exception e1) {
                spec.commandLine().getErr().println(String.format("Close %s error %s."
                        , name, e1.getStackTrace()));
            }
            spec.commandLine().getOut().println(String.format("Export keys %s done,expand count:%s", name, allStat));
        }
    }

    private void writeToFile(BufferedWriter bufferedWriter, List<byte[]> keys) throws IOException {
        if(keys.size()<1){
            return;
        }
        StringBuilder sb = new StringBuilder();
        for(byte[] key:keys){
            sb.append(ByteArrayUtil.toHexString(key)+"\r\n");
        }
        bufferedWriter.write(sb.toString());
    }

    private void doExpandIter(DB levelDb, DB secondDb, String name) {
        try {
            mergeDb(levelDb, secondDb,null);
            levelDb.close();
            secondDb.close();
        }catch (Exception e){
            spec.commandLine().getOut().println(String.format("Expand db %s error", name));
        }
    }

    private void doExpandReverse(DB levelDb, DB secondDb, String name, int rate) {
        mergeDbReverse(levelDb,secondDb);
        spec.commandLine().getOut().println(String.format("merge db %s done", name));
        doTestSerialGetInSecond(levelDb,secondDb,name);
    }

    private void doTestSerialGetInSecond(DB levelDb, DB secondDb, String name) {
        long allStat = 0;
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);

        List<byte[]> statKeys = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = levelDb.iterator(
            new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                keys.add(entry.getKey());
                values.add(entry.getValue());
                allStat += 1;
                if (keys.size() >= BATCH) {
                    try {
                        if(r.nextInt(10000) < 10){
                            keys.stream().forEach(key->statKeys.add(key));
                            statGetPerformance(secondDb,name,statKeys,LEVEL_ENGINE);
                        }
                        statKeys.clear();
                        keys.clear();
                        values.clear();
                    } catch (Exception e) {
                        spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                            , name, e.getStackTrace()));
                    }
                }
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
            spec.commandLine().getOut().println(String.format("Expand db %s done,expand count:%s", name, allStat));
        }
    }

    private void doTestTopKGet(DB levelDb, DB secondDb, String name, int rate) {
        long allStat = 0;
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);

        List<byte[]> statKeys = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = levelDb.iterator(
            new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToFirst();

            while (levelIterator.hasNext() && allStat< BATCH) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                addShuffleKv(3,name,entry, rate, keys, values);
                allStat += rate;
                if (keys.size() >= BATCH) {
                    try {
                        keys.stream().forEach(key->statKeys.add(key));
                        batchInsert(secondDb, keys, values);
                    } catch (Exception e) {
                        spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                            , name, e.getStackTrace()));
                    }
                }
            }

            if (!keys.isEmpty()) {
                try {
                    keys.stream().forEach(key->statKeys.add(key));
                    batchInsert(secondDb, keys, values);
                } catch (Exception e) {
                    spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                        , name, e.getStackTrace()));
                }
            }
            statGetPerformance(secondDb,name,statKeys,LEVEL_ENGINE);
            mergeDb(secondDb,levelDb,null);
            statGetPerformance(levelDb,name,statKeys,LEVEL_ENGINE);
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
            spec.commandLine().getOut().println(String.format("Expand db %s done,expand count:%s", name, allStat));
        }
    }

    private void statGetPerformance(Object db,String name, List<byte[]> statKeys,String engine) {
        if(statKeys==null||statKeys.isEmpty()){
            return;
        }
        try {
            DB leveldb = null;
            RocksDB rocksdb = null;
            if (engine.equals(LEVEL_ENGINE)) {
                leveldb = (DB) db;
            } else {
                rocksdb = (RocksDB) db;
            }

            StringBuilder sb = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();
            for (byte[] key : statKeys) {
                long begin = System.nanoTime();
                byte[] value = leveldb == null ? rocksdb.get(key) : leveldb.get(key);
                if (value == null) {
                    sb2.append((System.nanoTime() - begin) + ",");
                } else {
                    sb.append((System.nanoTime() - begin) + ",");
                }
            }
            spec.commandLine().getOut().println(name + " statGet :" + sb.toString());
            spec.commandLine().getOut().println(name + " statGet notFound:" + sb2.toString());
        }catch (Exception e){
            spec.commandLine().getErr().println(String.format("Stat db %s error,exception:%s", name,e.toString()));
        }
    }

    private void mergeDbReverse(DB originDb, DB targetDb) {
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = originDb.iterator(
            new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToLast();

            while (levelIterator.hasPrev()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.prev();
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

    private void mergeDb(DB originDb, DB targetDb,DB except) {
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = originDb.iterator(
            new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                if(except!=null&&except.get(entry.getKey())!=null){
                    continue;
                }
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
        return openLevelDb(Paths.get(database.toString(),dbPath,c.getString(NAME_CONFIG_KEY)+"_second"));
}

    private void doExpand(DB levelDb, DB secondDb, String name, int rate, int op, Path rockPath) {
        RocksDB rdb = null;
        if(rockPath!=null){
            rdb = initDB(rockPath);
        }
        long expandKeyCount = 0;
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> values = new ArrayList<>(BATCH);
        try (DBIterator levelIterator = levelDb.iterator(
                new org.iq80.leveldb.ReadOptions().fillCache(false))) {
            JniDBFactory.pushMemoryPool(2048 * 2048);
            levelIterator.seekToFirst();

            while (levelIterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = levelIterator.next();
                addShuffleKv(op,name,entry, rate, keys, values);
                expandKeyCount += rate;
                if (keys.size() >= BATCH) {
                    try {
                        doubleInsert(op==0?levelDb:secondDb,rdb, keys, values);
                    } catch (Exception e) {
                        spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                                , name, e.getStackTrace()));
                    }
                }
            }

            if (!keys.isEmpty()) {
                try {
                    doubleInsert(op==0?levelDb:secondDb,rdb, keys, values);
                } catch (Exception e) {
                    spec.commandLine().getErr().println(String.format("Batch insert shuffled kv to %s error %s."
                            , name, e.getStackTrace()));
                }
            }
            if(op==2){
                mergeDb(levelDb,secondDb,null);
                migrate(levelDb,rdb,null);
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
                if(rdb!=null){
                    rdb.close();
                }
                JniDBFactory.popMemoryPool();
            } catch (Exception e1) {
                spec.commandLine().getErr().println(String.format("Close %s error %s."
                        , name, e1.getStackTrace()));
            }
            spec.commandLine().getOut().println(String.format("Expand db %s done,expand count:%s", name, expandKeyCount));
        }
    }

    private void addShuffleKv(int op, String name, Map.Entry<byte[], byte[]> entry, int rate,
                              List<byte[]> keys, List<byte[]> values) {
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

    private void doubleInsert(DB db,RocksDB rocksDB,List<byte[]> keys,List<byte[]> values) {
        try {
            doInsertToLevelDB(db, keys, values);
            doInsertToRocksDB(rocksDB, keys, values);
            keys.clear();
            values.clear();
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    private void batchInsert(DB db, List<byte[]> keys, List<byte[]> values)
            throws Exception {
        doInsertToLevelDB(db, keys, values);
        keys.clear();
        values.clear();
    }

    private void doInsertToLevelDB(DB db, List<byte[]> keys, List<byte[]> values) throws IOException {
        if(db==null){
            return;
        }
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
    }

    private void batchInsert(RocksDB db, List<byte[]> keys, List<byte[]> values)
        throws Exception {
        doInsertToRocksDB(db, keys, values);
        keys.clear();
        values.clear();
    }

    private void doInsertToRocksDB(RocksDB db, List<byte[]> keys, List<byte[]> values) throws RocksDBException {
        if(db==null){
            return;
        }
        try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            for (int i = 0; i < keys.size(); i++) {
                byte[] k = keys.get(i);
                byte[] v = values.get(i);
                batch.put(k, v);
            }
            db.write(new org.rocksdb.WriteOptions(), batch);
        }
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
        dbOptions.cacheSize(10 * 1024 * 1024L);
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

    public RocksDB initDB(Path path) {
        RocksDB rdb = null;
        RocksDbSettings settings = RocksDbSettings.getSettings();
        try (Options options = new Options()) {
            // most of these options are suggested by https://github.com/facebook/rocksdb/wiki/Set-Up-Options
            // general options
            options.setCreateIfMissing(true);
            options.setIncreaseParallelism(1);
            options.setLevelCompactionDynamicLevelBytes(true);
            options.setMaxOpenFiles(settings.getMaxOpenFiles());

            // general options supported user config
            options.setNumLevels(settings.getLevelNumber());
            options.setMaxBytesForLevelMultiplier(settings.getMaxBytesForLevelMultiplier());
            options.setMaxBytesForLevelBase(settings.getMaxBytesForLevelBase());
            options.setMaxBackgroundCompactions(settings.getCompactThreads());
            options.setLevel0FileNumCompactionTrigger(settings.getLevel0FileNumCompactionTrigger());
            options.setTargetFileSizeMultiplier(settings.getTargetFileSizeMultiplier());
            options.setTargetFileSizeBase(settings.getTargetFileSizeBase());

            // table options
            final BlockBasedTableConfig tableCfg;
            options.setTableFormatConfig(tableCfg = new BlockBasedTableConfig());
            tableCfg.setBlockSize(settings.getBlockSize());
            tableCfg.setBlockCache(RocksDbSettings.getCache());
            tableCfg.setCacheIndexAndFilterBlocks(true);
            tableCfg.setPinL0FilterAndIndexBlocksInCache(true);
            tableCfg.setFilter(new BloomFilter(10, false));

            // read options
            ReadOptions readOpts = new ReadOptions();
            readOpts = readOpts.setPrefixSameAsStart(true)
                .setVerifyChecksums(false);

            try {
                 rdb = RocksDB.open(options,path.toString());
                 return rdb;
            } catch (RocksDBException e) {
                throw new RuntimeException("open fail", e);
            }
        }

    }


}
