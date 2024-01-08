package org.tron.plugins;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.rocksdb.RocksDBException;
import org.tron.plugins.utils.db.DBInterface;
import org.tron.plugins.utils.db.DBIterator;
import org.tron.plugins.utils.db.DbTool;
import picocli.CommandLine;

@Slf4j(topic = "ins")
@CommandLine.Command(name = "ins",
    description = "Inspect data for java-tron.",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0:Successful",
        "1:Internal error: exception occurred,please check toolkit.log"})
public class DbInspect implements Callable<Integer> {

  private static final long START_TIME = System.currentTimeMillis() / 1000;

  public static final String DELEGATION_DB = "delegation";
  private static long minCycle = Long.MAX_VALUE;
  private static long maxCycle = Long.MIN_VALUE;


  enum Operate {split, merge}

  @CommandLine.Spec
  CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(
      names = {"--operate", "-o"},
      defaultValue = "split",
      description = "operate: [ ${COMPLETION-CANDIDATES} ]. Default: ${DEFAULT-VALUE}",
      order = 1)
  private Operate operate;

  @CommandLine.Option(
      names = {"--fn-data-path", "-fn"},
      required = true,
      description = "the database path to be split or merged.",
      order = 3)
  private String fnDataPath;

  @CommandLine.Option(
      names = {"--help", "-h"},
      order = 5)
  private boolean help;


  @Override
  public Integer call() {
    if (help) {
      spec.commandLine().usage(System.out);
      return 0;
    }
    try {
      inspectSnapshot(fnDataPath);
      return 0;
    } catch (Exception e) {
      logger.error("{}", e);
      spec.commandLine().getErr().println(spec.commandLine().getColorScheme()
          .errorText(e.getMessage()));
      spec.commandLine().usage(System.out);
      return 1;
    } finally {
      DbTool.close();
    }
  }

  /**
   * Create the snapshot dataset.
   *
   * @param sourceDir   the original fullnode database dir,
   *                    same with {storage.db.directory} in conf file.
   */
  public void inspectSnapshot(String sourceDir) {
    logger.info("Start inspect snapshot.");
    spec.commandLine().getOut().println("Start inspect snapshot.");
    long start = System.currentTimeMillis();
    try {
      DBInterface delegationDb = DbTool.getDB(sourceDir, DELEGATION_DB);
      inspectDeltaReward(delegationDb);
      findDiffReward(delegationDb);
    } catch (IOException | RocksDBException e) {
      logger.error("Inspect snapshot failed, {}.", e.getMessage());
      spec.commandLine().getErr().println(spec.commandLine().getColorScheme()
          .stackTraceText(e));
      return;
    }
    long during = (System.currentTimeMillis() - start) / 1000;
    logger.info("Inspect snapshot finished, take {} s.", during);
    spec.commandLine().getOut().format("Inspect snapshot finished, take %d s.", during).println();
  }

  private void findDiffReward(DBInterface delegationDb) {
    for (long curr = minCycle; curr <= maxCycle; curr++) {
      byte[] currentAddrMap = delegationDb.get(String.valueOf(curr).getBytes());
      HashMap<byte[], BigInteger> addrToReward = currentAddrMap == null ? new HashMap<>() :
          SerializationUtils.deserialize(currentAddrMap);
      if (addrToReward.values().stream().distinct().count() > 1) {
         logDiff(curr, addrToReward);
      }
    }
  }

  private void logDiff(long cycle, HashMap<byte[], BigInteger> addrToReward) {
    StringBuilder sb = new StringBuilder();
    sb.append("different reward cycle found, cycle:" + cycle + "\n");
    addrToReward.entrySet().stream().forEach(bigIntegerEntry -> sb.append(
        "sr:" + new String(bigIntegerEntry.getKey()) + ",reward:" + bigIntegerEntry.getValue() +
            "\n"));
    logger.info(sb.toString());
  }

  private void inspectDeltaReward(DBInterface db)
      throws IOException, RocksDBException {
    //遍历delegation
    try (DBIterator iterator = db.iterator()) {
      for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
        byte[] key = iterator.getKey();
        byte[] value = iterator.getValue();
        String keyStr = new String(key);
        if (!isRewardKey(keyStr)) {
          continue;
        }
        storeDelta(db, keyStr, value);
      }
    }
  }

  private void storeDelta(DBInterface delegationDb, String keyStr, byte[] value) {
    String[] keySplit = keyStr.split("-");
    String cycle = keySplit[0];
    String addr = keySplit[1];
    updateCycleThreshold(cycle);
    BigInteger currReward = ArrayUtils.isEmpty(value) ? BigInteger.ZERO : new BigInteger(value);
    BigInteger currDelta = computeDelta(delegationDb, cycle, addr, currReward);

    byte[] currentAddrMap = delegationDb.get(cycle.getBytes());
    HashMap<byte[], BigInteger> addrToReward = currentAddrMap == null ? new HashMap<>() :
        SerializationUtils.deserialize(delegationDb.get(cycle.getBytes()));
    addrToReward.put(addr.getBytes(), currDelta);
    delegationDb.put(cycle.getBytes(), SerializationUtils.serialize(addrToReward));
  }

  private void updateCycleThreshold(String cycle) {
    long current = Long.parseLong(cycle);
    minCycle = Math.min(current, minCycle);
    maxCycle = Math.max(current, maxCycle);
  }

  private BigInteger computeDelta(DBInterface delegationDb, String cycle, String addr,
                                  BigInteger currReward) {
    byte[] value = delegationDb.get(buildViKey(Long.parseLong(cycle) - 1, addr));
    BigInteger preVi = ArrayUtils.isEmpty(value) ? BigInteger.ZERO : new BigInteger(value);
    return currReward.subtract(preVi);
  }

  private byte[] buildViKey(long cycle, String hexAddress) {
    return (cycle + "-" + hexAddress + "-vi").getBytes();
  }

  private boolean isRewardKey(String keyStr) {
    if (keyStr.startsWith("end-")) {
      return Boolean.FALSE;
    }
    if (keyStr.endsWith("-vi")) {
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }

}



