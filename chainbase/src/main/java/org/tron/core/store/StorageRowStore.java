package org.tron.core.store;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.db.TronStoreWithRevoking;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j(topic = "DB")
@Component
public class StorageRowStore extends TronStoreWithRevoking<StorageRowCapsule> {

  @Autowired
  private StorageRowStore(@Value("storage-row") String dbName) {
    super(dbName);
  }

  public static AtomicLong timer = new AtomicLong(0);
  public static LinkedList<Long> times = new LinkedList<>();
  public static LinkedList<Long> notFoundtimes = new LinkedList<>();
  public static LinkedList<byte[]> keys = new LinkedList<>();



  @Override
  public StorageRowCapsule get(byte[] key) {
    synchronized (AccountStore.class) {
  long start = System.nanoTime();
  long time = 0;
  boolean find = Boolean.TRUE;
  try {
    StorageRowCapsule row = getUnchecked(key);
    time = System.nanoTime()-start;
    if(row==null) find=Boolean.FALSE;
    row.setRowKey(key);
    return row;
  }finally {
    times.add(time);
    if (!find) {
      notFoundtimes.add(time);
    }
  }
    }
  }
}
