package org.tron.core.store;

import com.google.common.collect.Streams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.core.capsule.CodeCapsule;
import org.tron.core.db.TronStoreWithRevoking;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j(topic = "DB")
@Component
public class CodeStore extends TronStoreWithRevoking<CodeCapsule> {

  @Autowired
  private CodeStore(@Value("code") String dbName) {
    super(dbName);
  }

  public static AtomicLong timer = new AtomicLong(0);
  public static LinkedList<Long> times = new LinkedList<>();


  @Override
  public CodeCapsule get(byte[] key) {
    long start = System.nanoTime();
    try{
      return getUnchecked(key);
    }finally {
      long time = System.nanoTime()-start;
      if(time > 0) {
        timer.addAndGet(time);
        times.add(time);
      }
    }
  }

  public long getTotalCodes() {
    return Streams.stream(revokingDB.iterator()).count();
  }

  public byte[] findCodeByHash(byte[] hash) {
    return revokingDB.getUnchecked(hash);
  }
}
