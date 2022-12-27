package org.tron.core.store;

import com.google.common.collect.Streams;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.tron.core.capsule.AbiCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.db.TronStoreWithRevoking;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j(topic = "DB")
@Component
public class ContractStore extends TronStoreWithRevoking<ContractCapsule> {

  @Autowired
  private ContractStore(@Value("contract") String dbName) {
    super(dbName);
  }

  public static AtomicLong timer = new AtomicLong(0);
  public static LinkedList<Long> times = new LinkedList<>();


  @Override
  public ContractCapsule get(byte[] key) {
    long start = System.nanoTime();
    try {
      return getUnchecked(key);
    }finally {
      long time = System.nanoTime()-start;
      if(time > 0) {
          timer.addAndGet(time);
          times.add(time);
      }
    }
  }

  @Override
  public void put(byte[] key, ContractCapsule item) {
    if (Objects.isNull(key) || Objects.isNull(item)) {
      return;
    }

    if (item.getInstance().hasAbi()) {
      item = new ContractCapsule(item.getInstance().toBuilder().clearAbi().build());
    }
    revokingDB.put(key, item.getData());
  }

  /**
   * get total transaction.
   */
  public long getTotalContracts() {
    return Streams.stream(revokingDB.iterator()).count();
  }

  /**
   * find a transaction  by it's id.
   */
  public byte[] findContractByHash(byte[] trxHash) {
    return revokingDB.getUnchecked(trxHash);
  }

}
