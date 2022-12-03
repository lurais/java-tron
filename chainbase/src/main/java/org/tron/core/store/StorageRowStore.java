package org.tron.core.store;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.core.capsule.StorageRowCapsule;
import org.tron.core.db.TronStoreWithRevoking;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j(topic = "DB")
@Component
public class StorageRowStore extends TronStoreWithRevoking<StorageRowCapsule> {

  @Autowired
  private StorageRowStore(@Value("storage-row") String dbName) {
    super(dbName);
  }

  public static AtomicLong timer = new AtomicLong(0);


  @Override
  public StorageRowCapsule get(byte[] key) {
    long start = System.nanoTime();
    try {
      StorageRowCapsule row = getUnchecked(key);
      row.setRowKey(key);
      return row;
    }finally {
      long time = System.nanoTime() - start;
      if (time > 0) {
        timer.addAndGet(time);
      }
    }
  }
}
