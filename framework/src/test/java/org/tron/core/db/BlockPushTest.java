package org.tron.core.db;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.tron.common.utils.Sha256Hash;
import org.tron.consensus.Consensus;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.store.DynamicPropertiesStore;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Manager.class})
@Slf4j
public class BlockPushTest {

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Test
  public void testPushBlockExit()
      throws Exception {
    exit.expectSystemExitWithStatus(1);
    BlockCapsule blockCapsule = new BlockCapsule(
        1,  Sha256Hash.ZERO_HASH, 0, ByteString.copyFrom(Hex.decode(
        "a0559ccf55fadffdf814a42aff331de9688c132612")));
    blockCapsule.setMerkleRoot();
    Manager manager = PowerMockito.spy(new Manager());
    PowerMockito.doNothing().when(manager, "applyBlock", any(), any());
    MemberModifier.field(Manager.class, "rePushTransactions").set(manager,
        new LinkedBlockingQueue<>());
    MemberModifier.field(Manager.class, "pendingTransactions").set(manager,
        new LinkedBlockingQueue<>());

    Consensus consensus = PowerMockito.spy(new Consensus());
    doNothing().when(consensus).receiveBlock(any());
    KhaosDatabase khaosDatabase = PowerMockito.mock(KhaosDatabase.class);
    doReturn(blockCapsule).when(khaosDatabase).push(any());
    RevokingDatabase revokingDatabase = PowerMockito.mock(RevokingDatabase.class);
    doReturn(PowerMockito.mock(SnapshotManager.Session.class)).when(revokingDatabase)
        .buildSession();
    MemberModifier.field(Manager.class, "revokingStore").set(manager, revokingDatabase);
    MemberModifier.field(Manager.class, "consensus").set(manager, consensus);
    MemberModifier.field(Manager.class, "khaosDb").set(manager, khaosDatabase);

    DynamicPropertiesStore dynamicPropertiesStore = PowerMockito.mock(DynamicPropertiesStore.class);
    doReturn(dynamicPropertiesStore).when(manager).getDynamicPropertiesStore();
    doReturn(new ArrayList<>()).when(manager).getVerifyTxs(any());
    doReturn(0L).when(dynamicPropertiesStore).getLatestBlockHeaderNumber();
    doReturn(Sha256Hash.ZERO_HASH).when(dynamicPropertiesStore).getLatestBlockHeaderHash();
    doReturn(0L).when(dynamicPropertiesStore).getLatestSolidifiedBlockNum();
    manager.pushBlock(blockCapsule);
  }
}
