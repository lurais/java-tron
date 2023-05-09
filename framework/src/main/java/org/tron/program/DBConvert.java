package org.tron.program;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.PutFlags.MDB_APPEND;
import static org.tron.keystore.Wallet.generateRandomBytes;

import com.beust.jcommander.internal.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Status;
import org.tron.common.utils.FileUtil;
import org.tron.common.utils.MarketOrderPriceComparatorForLevelDB;
import org.tron.common.utils.MarketOrderPriceComparatorForRockDB;
import org.tron.common.utils.PropUtil;

@Slf4j
public class DBConvert implements Callable<Boolean> {

  static {
    RocksDB.loadLibrary();
  }

  private final String srcDir;
  private final String dstDir;
  private final String dbName;
  private final Path srcDbPath;
  private final Path dstDbPath;

  private long srcDbKeyCount = 0L;
  private long dstDbKeyCount = 0L;
  private long srcDbKeySum = 0L;
  private long dstDbKeySum = 0L;
  private long srcDbValueSum = 0L;
  private long dstDbValueSum = 0L;
  private final long startTime;
  private static final int CPUS  = Runtime.getRuntime().availableProcessors();
  private static final int BATCH  = 256;
  private static final String CHECKPOINT_V2_DIR_NAME = "checkpoint";


  @Override
  public Boolean call() throws Exception {
    return doConvert();
  }

  public DBConvert(String src, String dst, String name) {
    this.srcDir = src;
    this.dstDir = dst;
    this.dbName = name;
    this.srcDbPath = Paths.get(this.srcDir, name);
    this.dstDbPath = Paths.get(this.dstDir, name);
    this.startTime = System.currentTimeMillis();
  }

  public static org.iq80.leveldb.Options newDefaultLevelDbOptions() {
    org.iq80.leveldb.Options dbOptions = new org.iq80.leveldb.Options();
    dbOptions.createIfMissing(true);
    dbOptions.paranoidChecks(true);
    dbOptions.verifyChecksums(true);
    dbOptions.compressionType(CompressionType.SNAPPY);
    dbOptions.blockSize(4 * 1024);
    dbOptions.writeBufferSize(10 * 1024 * 1024);
    dbOptions.cacheSize(10 * 1024 * 1024L);
    dbOptions.maxOpenFiles(1000);
    return dbOptions;
  }

  public static void main(String[] args) {
    int code = run(args);
    logger.info("exit code {}.", code);
    System.out.printf("exit code %d.\n", code);
    System.exit(code);
  }

  public static int run(String[] args) {
    String dbSrc;
    String dbDst;
    if (args.length < 2) {
      dbSrc = "output-directory/database";
      dbDst = "output-directory-dst/database";
    } else {
      dbSrc = args[0];
      dbDst = args[1];
    }
    File dbDirectory = new File(dbSrc);
    if (!dbDirectory.exists()) {
      logger.info(" {} does not exist.", dbSrc);
      return 404;
    }
    List<File> files = Arrays.stream(Objects.requireNonNull(dbDirectory.listFiles()))
        .filter(File::isDirectory)
        .filter(e -> !CHECKPOINT_V2_DIR_NAME.equals(e.getName()))
        .collect(Collectors.toList());

    // add checkpoint v2 convert
    File cpV2Dir = new File(Paths.get(dbSrc, CHECKPOINT_V2_DIR_NAME).toString());
    List<File> cpList = null;
    if (cpV2Dir.exists()) {
      cpList = Arrays.stream(Objects.requireNonNull(cpV2Dir.listFiles()))
          .filter(File::isDirectory)
          .collect(Collectors.toList());
    }

    if (files.isEmpty()) {
      logger.info("{} does not contain any database.", dbSrc);
      return 0;
    }
    final long time = System.currentTimeMillis();
    final List<Future<Boolean>> res = new ArrayList<>();

    final ThreadPoolExecutor esDb = new ThreadPoolExecutor(
        CPUS, 16 * CPUS, 1, TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(CPUS, true), Executors.defaultThreadFactory(),
        new ThreadPoolExecutor.CallerRunsPolicy());

    esDb.allowCoreThreadTimeOut(true);

    files.forEach(f -> res.add(esDb.submit(new DBConvert(dbSrc, dbDst, f.getName()))));
    // convert v2
    if (cpList != null) {
      cpList.forEach(f -> res.add(esDb.submit(
          new DBConvert(dbSrc + "/" + CHECKPOINT_V2_DIR_NAME,
              dbDst + "/" + CHECKPOINT_V2_DIR_NAME, f.getName()))));
    }

    int fails = res.size();

    for (Future<Boolean> re : res) {
      try {
        if (re.get()) {
          fails--;
        }
      } catch (InterruptedException e) {
        logger.error("{}", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        logger.error("{}", e);
      }
    }

    esDb.shutdown();
    logger.info("database convert use {} seconds total.",
        (System.currentTimeMillis() - time) / 1000);
    if (fails > 0) {
      logger.error("failed!!!!!!!!!!!!!!!!!!!!!!!! size:{}", fails);
    }
    return fails;
  }

  public DB newLevelDb(Path db) throws Exception {
    DB database;
    File file = db.toFile();
    org.iq80.leveldb.Options dbOptions = newDefaultLevelDbOptions();
    if ("market_pair_price_to_order".equalsIgnoreCase(this.dbName)) {
      dbOptions.comparator(new MarketOrderPriceComparatorForLevelDB());
    }
    database = factory.open(file, dbOptions);
    return database;
  }

  private Options newDefaultRocksDbOptions() {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setIncreaseParallelism(1);
    options.setNumLevels(7);
    options.setMaxOpenFiles(5000);
    options.setTargetFileSizeBase(64 * 1024 * 1024);
    options.setTargetFileSizeMultiplier(1);
    options.setMaxBytesForLevelBase(512 * 1024 * 1024);
    options.setMaxBackgroundCompactions(Math.max(1, Runtime.getRuntime().availableProcessors()));
    options.setLevel0FileNumCompactionTrigger(4);
    options.setLevelCompactionDynamicLevelBytes(true);
    if ("market_pair_price_to_order".equalsIgnoreCase(this.dbName)) {
      options.setComparator(new MarketOrderPriceComparatorForRockDB(new ComparatorOptions()));
    }
    final BlockBasedTableConfig tableCfg;
    options.setTableFormatConfig(tableCfg = new BlockBasedTableConfig());
    tableCfg.setBlockSize(64 * 1024);
    tableCfg.setBlockCacheSize(32 * 1024 * 1024);
    tableCfg.setCacheIndexAndFilterBlocks(true);
    tableCfg.setPinL0FilterAndIndexBlocksInCache(true);
    tableCfg.setFilter(new BloomFilter(10, false));
    //options.prepareForBulkLoad();
    return options;
  }

  public RocksDB newRocksDb(Path db) {
    RocksDB database = null;
    try (Options options = newDefaultRocksDbOptions()) {
      database = RocksDB.open(options, db.toString());
    } catch (Exception e) {
      logger.error("{}", e);
    }
    return database;
  }

  private void batchInsert(RocksDB rocks, List<byte[]> keys, List<byte[]> values)
      throws Exception {
    try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
      for (int i = 0; i < keys.size(); i++) {
        byte[] k = keys.get(i);
        byte[] v = values.get(i);
        batch.put(k, v);
      }
      write(rocks, batch);
    }
    keys.clear();
    values.clear();
  }

  private void batchInsert(DB level, List<byte[]> keys, List<byte[]> values) throws IOException {
    try (WriteBatch batch = level.createWriteBatch()) {
      for (int i = 0; i < keys.size(); i++) {
        byte[] k = keys.get(i);
        byte[] v = values.get(i);
        batch.put(k, v);
      }
      write(level, batch);
    }
    keys.clear();
    values.clear();
  }

  /**
   * https://github.com/facebook/rocksdb/issues/6625
   * @param rocks db
   * @param batch write batch
   * @throws Exception RocksDBException
   */
  private void write(RocksDB rocks, org.rocksdb.WriteBatch batch) throws Exception {
    try {
      rocks.write(new org.rocksdb.WriteOptions(), batch);
    } catch (RocksDBException e) {
      // retry
      if (maybeRetry(e)) {
        TimeUnit.MILLISECONDS.sleep(1);
        write(rocks, batch);
      } else {
        throw e;
      }
    }
  }

  private void write(DB db, WriteBatch batch) {
    try {
      db.write(batch);
    } catch (Exception e) {
      throw e;
    }
  }

  private boolean maybeRetry(RocksDBException e) {
    boolean retry = false;
    if (e.getStatus() != null) {
      retry = e.getStatus().getCode() == Status.Code.TryAgain
          || e.getStatus().getCode() == Status.Code.Busy
          || e.getStatus().getCode() == Status.Code.Incomplete;
    }
    return retry || (e.getMessage() != null && ("Write stall".equalsIgnoreCase(e.getMessage())
        || ("Incomplete").equalsIgnoreCase(e.getMessage())));
  }

  /**
   * https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ .
   *  What's the fastest way to load data into RocksDB?
   * @param level leveldb
   * @param rocks rocksdb
   * @return if ok
   */
  public boolean convertLevelToRocksBatchIterator(RocksDB rocks, DB level) {
    // convert
    List<byte[]> keys = new ArrayList<>(BATCH);
    List<byte[]> values = new ArrayList<>(BATCH);
    List<byte[]> randKeys = new ArrayList<>(100000);
    JniDBFactory.pushMemoryPool(1024 * 1024);

    //init randkeys
    Random rand = new Random(System.currentTimeMillis());
    try (org.rocksdb.ReadOptions r = new org.rocksdb.ReadOptions().setFillCache(false);
         RocksIterator rocksIterator = rocks.newIterator(r)) {
      for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
        byte[] key = rocksIterator.key();
        if (randKeys.size() < 100000 && rand.nextInt(1000) < 10) {
          if(rand.nextInt(100)<10){
            randKeys.add(generateRandomBytes(rand.nextInt(100) + 1));
          } else {
            randKeys.add(key);
          }
        }
      }
    }
    Collections.shuffle(randKeys);

    logger.info("seq write level begin.....");
    // level test
    try (org.rocksdb.ReadOptions r = new org.rocksdb.ReadOptions().setFillCache(false);
         RocksIterator rocksIterator = rocks.newIterator(r)) {
      for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
        byte[] key = rocksIterator.key();
        byte[] value = rocksIterator.value();
        srcDbKeyCount++;
        srcDbKeySum = byteArrayToIntWithOne(srcDbKeySum, key);
        srcDbValueSum = byteArrayToIntWithOne(srcDbValueSum, value);
        keys.add(key);
        values.add(value);
        if (keys.size() >= BATCH) {
          try {
            batchInsert(level, keys, values);
          } catch (Exception e) {
            logger.error("{}", e);
            return false;
          }
        }
      }
      if (!keys.isEmpty()) {
        try {
          batchInsert(level, keys, values);
        } catch (Exception e) {
          logger.error("{}", e);
          return false;
        }
      }
      logger.info("seq write level end....");
      // check
      check(level);
    }  catch (Exception e) {
      logger.error("{}", e);
      return false;
    } finally {
      try {
      //  level.close();
      } catch (Exception e1) {
        logger.error("{}", e1);
      }
    }
    boolean checkLevelOk = dstDbKeyCount == srcDbKeyCount && dstDbKeySum == srcDbKeySum
        && dstDbValueSum == srcDbValueSum;
    // random get
    logger.info("level rand get begin....");
    for(byte[] b : randKeys){
      byte[] v = level.get(b);
    }
    logger.info("level rand get end....");


    FileUtil.createDirIfNotExists("./lsq/");
    File dbDirectory = new File("./");
    Env dbEnvironment = Env.create().setMapSize(50*1024*1024*1024L).setMaxDbs(1).open(dbDirectory);
    //dbEnvironment = Env.create().setMapSize(1_824).setMaxDbs(1).open(dbDirectory);
    Dbi db = dbEnvironment.openDbi(dbName, MDB_CREATE);

    Env seqDbEnvironment = Env.create().setMapSize(50*1024*1024*1024L).setMaxDbs(1).open(new File("./lsq/"));
    //dbEnvironment = Env.create().setMapSize(1_824).setMaxDbs(1).open(dbDirectory);
    Dbi seqDb = seqDbEnvironment.openDbi(dbName+"seq", MDB_CREATE);

    //lmdb test
    logger.info("write to lmdb begin....");
    try (org.rocksdb.ReadOptions r = new org.rocksdb.ReadOptions().setFillCache(false);
         RocksIterator rocksIterator = rocks.newIterator(r);Txn<ByteBuffer> txn = dbEnvironment.txnWrite()) {
      for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
        byte[] key = rocksIterator.key();
        byte[] value = rocksIterator.value();
        srcDbKeyCount++;
        srcDbKeySum = byteArrayToIntWithOne(srcDbKeySum, key);
        srcDbValueSum = byteArrayToIntWithOne(srcDbValueSum, value);
        keys.add(key);
        values.add(value);
        if (keys.size() >= BATCH) {
          try {
            write(txn,db,keys,values);
          } catch (Exception e) {
            logger.error("{}", e);
            return false;
          }
        }
      }
      if (!keys.isEmpty()) {
        try {
          write(txn,db,keys,values);
        } catch (Exception e) {
          logger.error("{}", e);
          return false;
        }
      }
      txn.commit();
      logger.info("write lmdb end...");
      // check
      check(dbEnvironment,db);
      logger.info("seq write to lmdb begin...");
      writeToLmdb(dbEnvironment,db,seqDbEnvironment,seqDb);
      logger.info("seq write to lmdb end...");
    }  catch (Exception e) {
      logger.error("{}", e);
      return false;
    } finally {
      try {
        level.close();
        rocks.close();
        JniDBFactory.popMemoryPool();
      } catch (Exception e1) {
        logger.error("{}", e1);
      }
    }
    boolean checkLmdbOk = dstDbKeyCount == srcDbKeyCount && dstDbKeySum == srcDbKeySum
        && dstDbValueSum == srcDbValueSum;
    // random get
    logger.info("random get lmdb begin....");
    try (Txn<ByteBuffer> txn = dbEnvironment.txnWrite()) {
      for(byte[] b : randKeys){
        Object v = db.get(txn,toBuffer(b));
      }
    }finally {
      dbEnvironment.close();
      seqDbEnvironment.close();
    }
    logger.info("random get lmdb end......"+checkLmdbOk);
    return checkLmdbOk;
  }

  private void writeToLmdb(Env dbEnvironment, Dbi db, Env seqDbEnvironment, Dbi seqDb) {
    try (Txn<ByteBuffer> txn = dbEnvironment.txnWrite();Txn<ByteBuffer> txnSeq = seqDbEnvironment.txnWrite();) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        for (final CursorIterable.KeyVal<ByteBuffer> kv : ci) {
          write(txnSeq,seqDb, Lists.newArrayList(toByteArray(kv.key())),Lists.newArrayList(toByteArray(kv.val())));
        }
      }
      txn.commit();
      txnSeq.commit();
    }
  }


  void write(Txn<ByteBuffer> txn, Dbi db, List<byte[]> keys,
             List<byte[]> values) {
    int i=0;
    while(i<keys.size()){
      db.put(txn,toBuffer(keys.get(i)), toBuffer(values.get(i)));
      i++;
    }
    keys.clear();
    values.clear();
  }

  public ByteBuffer toBuffer(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }

  private void check(DB level) {
    logger.info("check database {} start", this.dbName);
    try (DBIterator levelIterator = level.iterator(
        new org.iq80.leveldb.ReadOptions().fillCache(false))) {
      JniDBFactory.pushMemoryPool(1024 * 1024);
      levelIterator.seekToFirst();
      while (levelIterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = levelIterator.next();
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        dstDbKeyCount++;
        dstDbKeySum = byteArrayToIntWithOne(dstDbKeySum, key);
        dstDbValueSum = byteArrayToIntWithOne(dstDbValueSum, value);
      }
    } catch (IOException e) {
      logger.error("check database {} error", this.dbName, e);
    }
    logger.info("check database {} end", this.dbName);
  }

  private byte[] toByteArray(ByteBuffer byteBuffer) {
    byte[] bytesArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytesArray, 0, bytesArray.length);
    return bytesArray;
  }

  private void check(Env env,Dbi db) {
    logger.info("check database {} start", this.dbName);
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        for (final CursorIterable.KeyVal<ByteBuffer> kv : ci) {
          byte[] key = toByteArray(kv.key());
          byte[] value = toByteArray(kv.val());
          dstDbKeyCount++;
          dstDbKeySum = byteArrayToIntWithOne(dstDbKeySum, key);
          dstDbValueSum = byteArrayToIntWithOne(dstDbValueSum, value);
        }
      }
    }
    logger.info("check database {} end", this.dbName);
  }

  public boolean createEngine(String dir) {
    String enginePath = dir + File.separator + "engine.properties";

    if (!FileUtil.createFileIfNotExists(enginePath)) {
      return false;
    }

    return PropUtil.writeProperty(enginePath, "ENGINE", "LEVELDB");
  }

  public boolean checkDone(String dir) {
    String enginePath = dir + File.separator + "engine.properties";
    return FileUtil.isExists(enginePath);

  }

  public boolean doConvert() throws Exception {

    if (checkDone(this.dstDbPath.toString())) {
      logger.info(" {} is done, skip it.", this.dbName);
      return true;
    }

    File rocksDbFile = srcDbPath.toFile();
    if (!rocksDbFile.exists()) {
      logger.info(" {} does not exist.", srcDbPath.toString());
      return false;
    }

    RocksDB rocks = newRocksDb(srcDbPath);

    if (this.dstDbPath.toFile().exists()) {
      logger.info(" {} begin to clear exist database directory", this.dbName);
      FileUtil.deleteDir(this.dstDbPath.toFile());
      logger.info(" {} clear exist database directory done.", this.dbName);
    }

    FileUtil.createDirIfNotExists(dstDir);
    DB level = newLevelDb(dstDbPath);

    logger.info("Convert database {} start", this.dbName);
    boolean result  = convertLevelToRocksBatchIterator(rocks,level)
        && createEngine(dstDbPath.toString());
    long etime = System.currentTimeMillis();

    if (result) {
      logger.info("Convert database {} successful end with {} key-value {} minutes",
          this.dbName, this.srcDbKeyCount, (etime - this.startTime) / 1000.0 / 60);
    } else {
      logger.info("Convert database {} failure", this.dbName);
      if (this.dstDbPath.toFile().exists()) {
        logger.info(" {} begin to clear exist database directory", this.dbName);
        FileUtil.deleteDir(this.dstDbPath.toFile());
        logger.info(" {} clear exist database directory done.", this.dbName);
      }
    }
    return result;
  }

  public long byteArrayToIntWithOne(long sum, byte[] b) {
    for (byte oneByte : b) {
      sum += oneByte;
    }
    return sum;
  }
}