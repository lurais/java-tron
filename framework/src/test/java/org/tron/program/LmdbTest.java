package org.tron.program;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

@Slf4j
public class LmdbTest {

  private String dbName;
  private Env<ByteBuffer> dbEnvironment;
  private Dbi<ByteBuffer> db;
  private long originSum = 0;
  private long finalSum = 0;


  @Test
  public void lmdbTest(){
    Dbi db = createDb("/tmp","testdb");
    insertRandomData(db, 10);
    check(db);
    dbEnvironment.close();
    Assert.assertEquals(originSum,finalSum);
  }

  private void check(Dbi db) {
    try (Txn<ByteBuffer> txn = dbEnvironment.txnWrite()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        for (final CursorIterable.KeyVal<ByteBuffer> kv : ci) {
          finalSum += byteArrayToIntWithOne(toByteArray(kv.key()));
          finalSum += byteArrayToIntWithOne(toByteArray(kv.val()));
        }
      }
    }
  }

  private void insertRandomData(Dbi db, int count) {
    Random r = new Random(System.currentTimeMillis());
    while(count-->0){
      byte[] key = generateRandomBytes(r.nextInt(100)+1);
      byte[] value = generateRandomBytes(r.nextInt(1000)+1);
      originSum += byteArrayToIntWithOne(key);
      originSum += byteArrayToIntWithOne(value);
      db.put(toBuffer(key),toBuffer(value));
    }
  }

  private static byte[] getByteArrayFromByteBuffer(ByteBuffer byteBuffer) {
    byte[] bytesArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytesArray, 0, bytesArray.length);
    return bytesArray;
  }

  public byte[] toByteArray(ByteBuffer buffer){
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

  public ByteBuffer toBuffer(byte[] bytes){
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
    dbName= dbName;
    File dbDirectory = new File(directoryName);
    dbEnvironment = Env.create().setMapSize(2_133_741_824).setMaxDbs(1).open(dbDirectory);
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
