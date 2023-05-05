package org.tron.program;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.lmdbjava.DbiFlags.MDB_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.junit.Assert;
import org.junit.Test;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;
import org.tron.common.utils.FileUtil;

@Slf4j
public class LmdbTest {

  private String dbName;
  private Env<ByteBuffer> dbEnvironment;
  private Dbi<ByteBuffer> db;
  private long originSum = 0;
  private long finalSum = 0;


  @Test
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
