package org.tron.common.storage.prune;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.db2.common.Flusher;
import org.tron.core.db2.common.Value;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.SnapshotRoot;

@Slf4j(topic = "db")
@Component
public class ChainDataPruner {

  private static final String BLOCK_INDEX_STORE_NAME = "block-index";
  private static final String BLOCK_STORE_NAME = "block";
  private static final String TRANSACTION_STORE_NAME = "trans";
  private static final String TRANSACTION_RET_STORE_NAME = "transactionRetStore";
  private long blocksToRetain = CommonParameter.getInstance().getStorage().
      getDbAutoPruneRetain();
  private static Map<String, SnapshotRoot> snapshotRootMap = new ConcurrentHashMap<>();

  private ScheduledExecutorService pruneExecutor = null;

  @Autowired
  private ChainBaseManager chainBaseManager;

  @PostConstruct
  public void init() {
    if ((!chainBaseManager.isLiteNode()) ||
        (!CommonParameter.getInstance().getStorage().isDbAutoPrune())) {
      return;
    }
    pruneExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("db-prune-thread-%d").build());
    pruneExecutor.scheduleWithFixedDelay(() -> {
          try {
            prune();
          } catch (InterruptedException e) {
            logger.warn("Prune chain data thread interrupted!");
            Thread.currentThread().interrupt();
          }
        }, 10,
        CommonParameter.getInstance().getStorage().getDbAutoPruneFrequency(), TimeUnit.MINUTES);
  }

  public static void register(String dbName, SnapshotRoot snapshotRoot) {
    if (!CommonParameter.getInstance().getStorage().isDbAutoPrune()) {
      return;
    }
    snapshotRootMap.put(dbName, snapshotRoot);
  }

  public void prune() throws InterruptedException {
    long lowestBlockNumber = chainBaseManager.getBlockStore().getLimitNumber(1, 1).
        stream().map(BlockCapsule::getNum).findFirst().orElse(0L);
    long latestBlockNumber = chainBaseManager.getDynamicPropertiesStore().
        getLatestBlockHeaderNumberFromDB();
    if (lowestBlockNumber <= 0 ||latestBlockNumber < 0) {
      logger.warn("Prune block, query lowest or latest block failed! lowestBlockNumber={}," +
          "latestBlockNumber={}", lowestBlockNumber, latestBlockNumber);
      return;
    }
    while (lowestBlockNumber <= latestBlockNumber - blocksToRetain) {
      long toFetchCount = (latestBlockNumber - lowestBlockNumber + 1 - blocksToRetain) > 10 ?
          10 : (latestBlockNumber - lowestBlockNumber + 1 - blocksToRetain);
      List<BlockCapsule> blockCapsuleList = chainBaseManager.getBlockStore()
          .getLimitNumber(lowestBlockNumber, toFetchCount);
      Map<WrappedByteArray, WrappedByteArray> blockIdBatch = new HashMap<>();
      Map<WrappedByteArray, WrappedByteArray> blockNumBatch = new HashMap<>();
      Map<WrappedByteArray, WrappedByteArray> transIdBatch = new HashMap<>();
      prepareWriteBatch(blockIdBatch, blockNumBatch, transIdBatch, blockCapsuleList);
      flushDb(blockIdBatch, blockNumBatch, transIdBatch);
      lowestBlockNumber += toFetchCount;
      Thread.sleep(100);
    }
  }

  private void flushDb(Map<WrappedByteArray, WrappedByteArray> blockIdBatch,
                       Map<WrappedByteArray, WrappedByteArray> blockNumBatch,
                       Map<WrappedByteArray, WrappedByteArray> transIdBatch) {
    SnapshotRoot transactionRoot = snapshotRootMap.get(TRANSACTION_STORE_NAME);
    ((Flusher)transactionRoot.getDb()).flush(transIdBatch);
    SnapshotRoot transactionRetRoot = snapshotRootMap.get(TRANSACTION_RET_STORE_NAME);
    ((Flusher)transactionRetRoot.getDb()).flush(blockNumBatch);
    SnapshotRoot blockIndexRoot = snapshotRootMap.get(BLOCK_INDEX_STORE_NAME);
    ((Flusher)blockIndexRoot.getDb()).flush(blockNumBatch);
    SnapshotRoot blockRoot = snapshotRootMap.get(BLOCK_STORE_NAME);
    ((Flusher)blockRoot.getDb()).flush(blockIdBatch);
  }

  private void prepareWriteBatch (
      Map<WrappedByteArray, WrappedByteArray> blockIdBatch,
      Map<WrappedByteArray, WrappedByteArray> blockNumBatch,
      Map<WrappedByteArray, WrappedByteArray> transIdBatch,
      List<BlockCapsule> blockCapsuleList) {
    for (BlockCapsule blockCapsule: blockCapsuleList) {
      blockIdBatch.put(WrappedByteArray.of(blockCapsule.getBlockId().getBytes()),
          WrappedByteArray.of(Value.of(Value.Operator.DELETE, null).getBytes()));
      blockNumBatch.put(WrappedByteArray.of(Longs.toByteArray(blockCapsule.getNum())),
          WrappedByteArray.of(Value.of(Value.Operator.DELETE, null).getBytes()));
      blockCapsule.getTransactions().stream().forEach(tx-> transIdBatch.put(WrappedByteArray.of(tx.
              getTransactionId().getBytes()), WrappedByteArray.of(Value.of(Value.Operator.DELETE,
          null).getBytes())));
    }
  }

  public void shutdown() {
    if (Objects.nonNull(pruneExecutor)) {
      try {
        pruneExecutor.shutdown();
      } catch (Exception e) {
        logger.error("Chain pruner shutdown error: {}", e.getMessage());
      }
    }
  }

}
