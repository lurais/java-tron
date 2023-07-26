package org.tron.common.application;

import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.tron.common.logsfilter.EventPluginLoader;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.ByteArray;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.capsule.VotesCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.config.args.DynamicArgs;
import org.tron.core.consensus.ConsensusService;
import org.tron.core.db.Manager;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.metrics.MetricsUtil;
import org.tron.core.net.TronNetService;
import org.tron.program.FullNode;
import org.tron.program.SolidityNode;

@Slf4j(topic = "app")
@Component
public class ApplicationImpl implements Application, ApplicationListener<ContextRefreshedEvent> {

  private ServiceContainer services;

  @Autowired
  private TronNetService tronNetService;

  @Autowired
  private Manager dbManager;

  @Autowired
  private ChainBaseManager chainBaseManager;

  @Autowired
  private ConsensusService consensusService;

  @Autowired
  private DynamicArgs dynamicArgs;

  @Override
  public void setOptions(Args args) {
    // not used
  }

  @Override
  @Autowired
  public void init(CommonParameter parameter) {
    services = new ServiceContainer();
  }

  @Override
  public void addService(Service service) {
    services.add(service);
  }

  @Override
  public void initServices(CommonParameter parameter) {
    services.init(parameter);
  }

  /**
   * start up the app.
   */
  public void startup() {
    if ((!Args.getInstance().isSolidityNode()) && (!Args.getInstance().isP2pDisable())) {
      tronNetService.start();
    }
    consensusService.start();
    MetricsUtil.init();
    dynamicArgs.init();
  }

  @Override
  public void shutdown() {
    logger.info("******** start to shutdown ********");
    if (!Args.getInstance().isSolidityNode() && (!Args.getInstance().p2pDisable)) {
      tronNetService.close();
    }
    consensusService.stop();
    synchronized (dbManager.getRevokingStore()) {
      dbManager.getSession().reset();
      closeRevokingStore();
      closeAllStore();
    }
    dbManager.stopRePushThread();
    dbManager.stopRePushTriggerThread();
    EventPluginLoader.getInstance().stopPlugin();
    dbManager.stopFilterProcessThread();
    dynamicArgs.close();
    logger.info("******** end to shutdown ********");
    FullNode.shutDownSign = true;
  }

  @Override
  public void startServices() {
    services.start();
  }

  @Override
  public void shutdownServices() {
    services.stop();
  }

  @Override
  public Manager getDbManager() {
    return dbManager;
  }

  @Override
  public ChainBaseManager getChainBaseManager() {
    return chainBaseManager;
  }

  private void closeRevokingStore() {
    logger.info("******** start to closeRevokingStore ********");
    dbManager.getRevokingStore().shutdown();
  }

  private void closeAllStore() {
    dbManager.closeAllStore();
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent event) {
    //遍历account库
    statAccountStake();
  }

  private void statAccountStake() {
    //仅统计老质押模型的质押情况
    logger.info("statAccountStake begin,iterCount=" + 0);
    long iterCount = 0;
    long newAlgorithmCycle = chainBaseManager.getDynamicPropertiesStore().getNewRewardAlgorithmEffectiveCycle();
    logger.info("statAccountStake begin,newAlgorithmCycle="+newAlgorithmCycle);
    byte[]  key =ByteArray.fromHexString("413ae9741dd749698b88ae28ace1d91dfb0dfaed2c");
    long beginCycle = chainBaseManager.getDelegationStore().getBeginCycle(key);
    long endCycle = chainBaseManager.getDelegationStore().getEndCycle(key);

    logger.info("beginCycle:"+beginCycle+",newAlgoCycle="+newAlgorithmCycle+"endCycle:"+endCycle+",accountVoteSize:"+chainBaseManager.getAccountStore().get(key).getVotesList());

    for (Map.Entry<byte[], AccountCapsule> entry : chainBaseManager.getAccountStore()) {
      iterCount++;
       beginCycle = chainBaseManager.getDelegationStore().getBeginCycle(entry.getKey());
       endCycle = chainBaseManager.getDelegationStore().getEndCycle(entry.getKey());
      if(entry.getValue().createReadableString().equals("413ae9741dd749698b88ae28ace1d91dfb0dfaed2c")){
        logger.info("beginCycle:"+beginCycle+",newAlgoCycle="+newAlgorithmCycle+"endCycle:"+endCycle+",accountVoteSize:"+entry.getValue().getVotesList().size());
      }
      if (beginCycle >= 0 && beginCycle < newAlgorithmCycle && fitOld(beginCycle,endCycle,entry.getKey(),entry.getValue())) {
        logStakeOld(entry.getValue(),  beginCycle,endCycle);
      }
    }
    logger.info("statAccountStake end,iterCount=" + iterCount);
  }

  private boolean fitOld(long beginCycle, long endCycle, byte[] address, AccountCapsule accountCapsule) {
    if(beginCycle+1==endCycle && Objects.nonNull(chainBaseManager.getDelegationStore().getAccountVote(beginCycle, address))){
      return true;
    }
    if(!CollectionUtils.isEmpty(accountCapsule.getVotesList())){
      return true;
    }
    return false;
  }

  private void logStakeOld(AccountCapsule accountCapsule, long beginCycle, long endCycle) {
    String sb = "statAccountStake account:" + accountCapsule.createReadableString() +
        " beginCycle:" + parseTime(beginCycle) +
//        " endCycle:" + endCycle +
//        " voteListSize:" + accountCapsule.getVotesList().size() +
        " frozenBalance:" + accountCapsule.getFrozenBalance();
    logger.info(sb);
  }

  private String parseTime(long beginCycle) {
    Date origin = null;
    try {
      origin = DateUtils.parseDate("2019-10-31 20:00:00","yyyy-MM-dd HH:mm:ss");
    } catch (ParseException e) {
      e.printStackTrace();
    }
    Date date = DateUtils.addHours(origin,Integer.parseInt(beginCycle+"")*6);
    return DateFormatUtils.format(date,"yyyy-MM-dd HH:mm:ss");
  }


}
