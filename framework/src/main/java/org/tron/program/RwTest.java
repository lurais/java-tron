package org.tron.program;

import static java.lang.Boolean.TRUE;
import static java.lang.Integer.BYTES;
import static java.lang.System.setProperty;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.fusesource.leveldbjni.JniDBFactory.pushMemoryPool;
import static org.iq80.leveldb.CompressionType.NONE;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_INTEGERKEY;
import static org.lmdbjava.Env.DISABLE_CHECKS_PROP;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;
import static org.lmdbjava.EnvFlags.MDB_WRITEMAP;
import static org.lmdbjava.GetOp.MDB_SET_KEY;
import static org.lmdbjava.PutFlags.MDB_APPEND;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.commons.math.random.BitsStreamGenerator;
import org.apache.commons.math.random.MersenneTwister;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.junit.Assert;
import org.lmdbjava.BufferProxy;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.KeyRange;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;
import org.tron.common.utils.FileUtil;

@Slf4j
public class RwTest {

  private String dbName;
  private Env<ByteBuffer> dbEnvironment;
  //private Dbi<ByteBuffer> db;
  private long originSum = 0;
  private long finalSum = 0;
  int[] keys;
  private static final BitsStreamGenerator RND = new MersenneTwister();
  BufferProxy<ByteBuffer> bufferProxy;
  Dbi<ByteBuffer> db = null;
  Env<ByteBuffer> env;
  int num = 10000000;
  int valSize = 100;
  static final int POSIX_MODE = 664;
  boolean intKey = false;
  boolean sequential = true;
  boolean valRandom = false;
  static final int STRING_KEY_LENGTH = 16;
  int keySize = 0;
  static final byte[] RND_MB = new byte[1_048_576];

  ByteBuffer rwKey;
  ByteBuffer rwVal;
  Cursor<ByteBuffer> c;
  Txn<ByteBuffer> txn;
  List<byte[]> randLmList = new ArrayList<>(50000);

  //leveldb
  DB leveldb = null;
  MutableDirectBuffer wkb;
  MutableDirectBuffer wvb;
  List<byte[]> randLevelList = new ArrayList<>(50000);

  static {
    setProperty(DISABLE_CHECKS_PROP, TRUE.toString());
  }

  public void lmTest() throws IOException {
    String dir = "/tmp/test/" + System.currentTimeMillis();
    FileUtil.createDirIfNotExists(dir);
    initData(true, dir);
    initLevel(new File(dir));
    long writeLmStart = System.currentTimeMillis();
    writeToLm();
    long writeLmEnd  = System.currentTimeMillis();
    readLm();
    long readLmEnd = System.currentTimeMillis();
    writeToLevel(1000000);
    long writeToLevelEnd = System.currentTimeMillis();
    readLevel();
    long readLevelEnd = System.currentTimeMillis();
    logger.info("write to lm end, cost:"+ (writeLmEnd-writeLmStart)
        + " read lm cost:"+(readLmEnd -writeLmEnd)
        +", write to level cost:"+(writeToLevelEnd-readLmEnd)
        +", read level cost:"+(readLevelEnd-writeToLevelEnd));
    // writeLevel();
  }


  public void lmRandTest() throws IOException {
    String dir = "/tmp/test/" + System.currentTimeMillis();
    FileUtil.createDirIfNotExists(dir);
    initData(true, dir);
    initLevel(new File(dir));
    long writeLmStart = System.currentTimeMillis();
    writeToLmRand();
    Collections.shuffle(randLmList);
    long writeLmEnd  = System.currentTimeMillis();
    readLmRand();
    long readLmEnd = System.currentTimeMillis();
    writeToLevelRand(1000000);
    Collections.shuffle(randLevelList);
    long writeToLevelEnd = System.currentTimeMillis();
    readLevelRand();
    long readLevelEnd = System.currentTimeMillis();
    logger.info("write to lm end, cost:"+ (writeLmEnd-writeLmStart)
        + " read lm cost:"+(readLmEnd -writeLmEnd)
        +", write to level cost:"+(writeToLevelEnd-readLmEnd)
        +", read level cost:"+(readLevelEnd-writeToLevelEnd));
    // writeLevel();
  }

  private void readLevel() {
    for (final int key : keys) {
      if (intKey) {
        wkb.putInt(0, key);
      } else {
        wkb.putStringWithoutLengthUtf8(0, padKey(key));
      }
      leveldb.get(wkb.byteArray());
    }
  }

  private void readLevelRand() {
    for(byte[] key: randLevelList) {
      leveldb.get(key);
    }
  }

  private void initLevel(File tmp) throws IOException {
    wkb = new UnsafeBuffer(new byte[keySize]);
    wvb = new UnsafeBuffer(new byte[valSize]);
    pushMemoryPool(1_024 * 512);
    final Options options = new Options();
    options.createIfMissing(true);
    options.compressionType(NONE);
    leveldb = factory.open(tmp, options);
  }

  private void readLm() {
    for (final int key : keys) {
      rwKey.clear();
      if (intKey) {
        rwKey.putInt(key).flip();
      } else {
        final byte[] str = padKey(key).getBytes(US_ASCII);
        rwKey.put(str, 0, str.length).flip();
      }
      c.get(rwKey, MDB_SET_KEY);
      txn.val();
    }
  }

  private void readLmRand() {
    for (final byte[] key : randLmList) {
      rwKey.clear();
      rwKey.put(key).flip();
      c.get(rwKey, MDB_SET_KEY);
      txn.val();
    }
  }

  static final DbiFlags[] dbiFlags(final boolean intKey) {
    final DbiFlags[] flags;
    if (intKey) {
      flags = new DbiFlags[]{MDB_CREATE, MDB_INTEGERKEY};
    } else {
      flags = new DbiFlags[]{MDB_CREATE};
    }
    return flags;
  }

  private void initData(boolean sequential,String dir) {
    keys = new int[num];
    for (int i = 0; i < num; i++) {
      if (sequential) {
        keys[i] = i;
      } else {
        while (true) {
          int candidateKey = RND.nextInt();
          if (candidateKey < 0) {
            candidateKey *= -1;
          }
//          if (!set.contains(candidateKey)) {
//            set.add(candidateKey);
//            keys[i] = candidateKey;
//            break;
//          }
        }
      }
    }
    initDbLm(true,false, new File(dir));
    keySize = intKey ? BYTES : STRING_KEY_LENGTH;
    bufferProxy = PROXY_OPTIMAL;
    rwKey = allocateDirect(120).order(LITTLE_ENDIAN);
    rwVal = allocateDirect(120);
    txn = env.txnRead();
    c = db.openCursor(txn);
  }

  static final EnvFlags[] envFlags(final boolean writeMap, final boolean sync) {
    final Set<EnvFlags> envFlagSet = new HashSet<>();
    if (writeMap) {
      envFlagSet.add(MDB_WRITEMAP);
    }
    if (!sync) {
      envFlagSet.add(MDB_NOSYNC);
    }
    final EnvFlags[] envFlags = new EnvFlags[envFlagSet.size()];
    envFlagSet.toArray(envFlags);
    return envFlags;
  }

  private void initDbLm(boolean writeMap,boolean sync,File tmp) {
    final EnvFlags[] envFlags = envFlags(writeMap, sync);
    env = create(PROXY_OPTIMAL)
        .setMapSize(mapSize(num, valSize))
        .setMaxDbs(1)
        .setMaxReaders(2)
        .open(tmp, POSIX_MODE, envFlags);

    final DbiFlags[] flags = dbiFlags(intKey);
    db = env.openDbi("db", flags);
  }

  void writeToLm() {
    try (Txn<ByteBuffer> tx = env.txnWrite();) {
      try (Cursor<ByteBuffer> c = db.openCursor(tx);) {
        final PutFlags flags = sequential ? MDB_APPEND : null;
        final int rndByteMax = RND_MB.length - valSize;
        int rndByteOffset = 0;
        for (final int key : keys) {
          rwKey.clear();
          rwVal.clear();
          if (intKey) {
            rwKey.putInt(key).flip();
          } else {
            final byte[] str = padKey(key).getBytes(US_ASCII);
            rwKey.put(str, 0, str.length).flip();
          }
          if (valRandom) {
            rwVal.put(RND_MB, rndByteOffset, valSize).flip();
            rndByteOffset += valSize;
            if (rndByteOffset >= rndByteMax) {
              rndByteOffset = 0;
            }
          } else {
            rwVal.putInt(key);
            rwVal.position(valSize);
            rwVal.flip();
          }
          c.put(rwKey, rwVal, flags);
        }
      }
      tx.commit();
    }
  }

  void writeToLmRand(){
    Random r = new Random(System.currentTimeMillis());
    int count=num;
    try (Txn<ByteBuffer> tx = env.txnWrite();) {
      try (Cursor<ByteBuffer> c = db.openCursor(tx);) {
        final PutFlags flags = null;
        while(count-->0) {
          byte[] key = generateRandomBytes(r.nextInt(100) + 1);
          byte[] value = generateRandomBytes(r.nextInt(100) + 1);
          if(randLmList.size()<1000 && r.nextInt(1000)<5){
            randLmList.add(key);
          }
          rwKey.clear();
          rwVal.clear();
          rwKey.put(key).flip();
          rwVal.put(value).flip();
          c.put(rwKey, rwVal, flags);
        }
      }
      tx.commit();
    }
  }

  void writeToLevel(final int batchSize) throws IOException {
    final int rndByteMax = RND_MB.length - valSize;
    int rndByteOffset = 0;
    WriteBatch batch = leveldb.createWriteBatch();
    for (int i = 0; i < keys.length; i++) {
      final int key = keys[i];
      if (intKey) {
        wkb.putInt(0, key, LITTLE_ENDIAN);
      } else {
        wkb.putStringWithoutLengthUtf8(0, padKey(key));
      }
      if (valRandom) {
        wvb.putBytes(0, RND_MB, rndByteOffset, valSize);
        rndByteOffset += valSize;
        if (rndByteOffset >= rndByteMax) {
          rndByteOffset = 0;
        }
      } else {
        wvb.putInt(0, key);
      }
      batch.put(wkb.byteArray(), wvb.byteArray());
      if (i % batchSize == 0) {
        leveldb.write(batch);
        batch.close();
        batch = leveldb.createWriteBatch();
      }
    }
    leveldb.write(batch); // possible partial batch
    batch.close();
  }

  void writeToLevelRand(final int batchSize) throws IOException {
    final int rndByteMax = RND_MB.length - valSize;
    Random r = new Random(System.currentTimeMillis());
    int rndByteOffset = 0;
    WriteBatch batch = leveldb.createWriteBatch();
    int i = 0;
    int count = num;
    while(count-->0){
      byte[] key = generateRandomBytes(r.nextInt(100) + 1);
      byte[] value = generateRandomBytes(r.nextInt(1000) + 1);
      if(randLevelList.size()<1000 && r.nextInt(1000)<5){
        randLevelList.add(key);
      }
      batch.put(key, value);
      i++;
      if (i % batchSize == 0) {
        leveldb.write(batch);
        batch.close();
        batch = leveldb.createWriteBatch();
      }
    }
    leveldb.write(batch); // possible partial batch
    batch.close();
  }


  static final long mapSize(final int num, final int valSize) {
    return num * ((long) valSize) * 32L / 10L;
  }

  final String padKey(final int key) {
    final String skey = Integer.toString(key);
    return "0000000000000000".substring(0, 16 - skey.length()) + skey;
  }


  public void lmdbTest() throws IOException {
    String dir = "/tmp/test/" + System.currentTimeMillis();
    FileUtil.createDirIfNotExists(dir);
    Dbi db = createDb(dir, "testdb");
    DB level = initDb(dir + "/out");
    insertRandomData(db, level, 1);
    check(db, level);
    check2(db,level);
    dbEnvironment.close();
    Assert.assertEquals(originSum, finalSum);
  }

  private DB initDb(String dir) {
    try {
      return factory.open(new File(dir), newDefaultLevelDbOptions());
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  private void check(Dbi db, DB level) {
    int count = 0;
    try (Txn<ByteBuffer> txn = dbEnvironment.txnWrite()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        for (final CursorIterable.KeyVal<ByteBuffer> kv : ci) {
          byte[] keyBytes = level.get(toByteArray(kv.key()));
          byte[] valBytes = toByteArray(kv.val());
          finalSum += byteArrayToIntWithOne(valBytes);
          if (Arrays.equals(keyBytes, valBytes)) {
            count++;
            continue;
          }
          throw new RuntimeException("not equal:" + toByteArray(kv.val()));
        }
      }
    }
    System.out.println(count);
  }

  private void check2(Dbi db, DB level) throws IOException {
    int count =0;
    try (Txn<ByteBuffer> txn = dbEnvironment.txnRead()) {
      try (DBIterator levelIterator = level.iterator(
          new org.iq80.leveldb.ReadOptions().fillCache(false))) {
        levelIterator.seekToFirst();
        while (levelIterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = levelIterator.next();
          byte[] key = entry.getKey();
          if (db.get(txn, toBuffer(key)) == null) {
            throw new RuntimeException("not found key" + key);
          }
          count++;
        }
      }
    }
    System.out.println(count);
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

  private void insertRandomData(Dbi db, DB level, int count) {
    Random r = new Random(System.currentTimeMillis());
    int wc=0;
    while (count-- > 0) {
      byte[] key = generateRandomBytes(r.nextInt(100) + 1);
      byte[] value = generateRandomBytes(r.nextInt(1000) + 1);

      db.put(toBuffer(key), toBuffer(value));
      level.put(key, value);
//      if (db.get(dbEnvironment.txnWrite(), toBuffer(key)) == null) {
//        throw new RuntimeException("write fail!");
//      }
      wc++;
      originSum += byteArrayToIntWithOne(value);
    }
    System.out.println(wc);
  }

  private static byte[] getByteArrayFromByteBuffer(ByteBuffer byteBuffer) {
    byte[] bytesArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytesArray, 0, bytesArray.length);
    return bytesArray;
  }

  public byte[] toByteArray(ByteBuffer buffer) {
    return getByteArrayFromByteBuffer(buffer);
//    buffer.flip();
//    int len = buffer.limit()-buffer.position();
//    byte[] ret = new byte[len];
//    byte[] arr = new byte[buffer.remaining()];
//    for(int i=0;i<ret.length;i++){
//      ret[i] = buffer.get();
//    }
//    return buffer.array();
//    return buffer.get(arr);

    // return ret;
  }

  public ByteBuffer toBuffer(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }

  public static byte[] generateRandomBytes(int size) {
    byte[] bytes = new byte[size];
    new SecureRandom().nextBytes(bytes);
    return bytes;
  }

  public Dbi createDb(String directoryName, String dbName) {
    dbName = dbName;
    File dbDirectory = new File(directoryName);
    dbEnvironment = Env.create().setMapSize((long)1e9).setMaxDbs(1).open(dbDirectory);
    //dbEnvironment = Env.create().setMapSize(1_824).setMaxDbs(1).open(dbDirectory);

    db = dbEnvironment.openDbi(dbName, MDB_CREATE);
    return db;
  }

  public long byteArrayToIntWithOne(byte[] b) {
    long currentSum = 0;
    for (byte oneByte : b) {
      currentSum += oneByte;
    }
    return currentSum;
  }
}
