package org.tron.core.services.http.solidity;

import static org.junit.Assert.fail;
import static org.tron.common.utils.client.utils.HttpMethed.createRequest;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.ByteString;
import java.io.UnsupportedEncodingException;
import javax.annotation.Resource;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.tron.common.BaseTest;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionInfoCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.db.BlockStore;
import org.tron.core.db.TransactionStore;
import org.tron.core.store.TransactionRetStore;
import org.tron.protos.Protocol;
import org.tron.protos.contract.BalanceContract;

public class GetTransactionInfoByIdSolidityServletTest extends BaseTest {

  @Resource
  private GetTransactionInfoByIdSolidityServlet getTransactionInfoByIdSolidityServlet;

  @Resource
  private GetTransactionByIdSolidityServlet getTransactionByIdSolidityServlet;

  @Resource
  private TransactionRetStore transactionRetStore;

  @Resource
  private TransactionStore transactionStore;

  @Resource
  private BlockStore blockStore;

  @BeforeClass
  public static void init() {
    Args.setParam(new String[]{"-d", dbPath()}, Constant.TEST_CONF);
  }

  @Before
  public void initStore() {
    TransactionInfoCapsule transactionInfoCapsule = new TransactionInfoCapsule();
    transactionInfoCapsule.setId(ByteArray.fromHexString("dd0fb91c1479dcd5fd5633" +
        "b2c053c526a2c1bb7bbb1d1877c4996abce15e8814"));
    transactionInfoCapsule.setFee(1L);
    transactionInfoCapsule.setBlockNumber(100L);
    transactionInfoCapsule.setBlockTimeStamp(200L);

    TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule();
    transactionRetCapsule.addTransactionInfo(transactionInfoCapsule.getInstance());

    BlockCapsule blockCapsule = new BlockCapsule(100, new Sha256Hash(100,
        Sha256Hash.ZERO_HASH), System.currentTimeMillis(), ByteString.EMPTY);
    BalanceContract.TransferContract tc =
        BalanceContract.TransferContract.newBuilder()
            .setAmount(10)
            .setOwnerAddress(ByteString.copyFromUtf8("aaa"))
            .setToAddress(ByteString.copyFromUtf8("bbb"))
            .build();
    TransactionCapsule trx = new TransactionCapsule(tc,
        Protocol.Transaction.Contract.ContractType.TransferContract);
    blockCapsule.addTransaction(trx);
    trx.setBlockNum(blockCapsule.getNum());

    blockStore.put(blockCapsule.getBlockId().getBytes(), blockCapsule);
    transactionRetStore.put(ByteArray.fromLong(100L), transactionRetCapsule);
    transactionStore.put(trx.getTransactionId().getBytes(), trx);
  }

  @Test
  public void testDoGetTransactionInfo() {
    MockHttpServletRequest request = createRequest(HttpGet.METHOD_NAME);
    MockHttpServletResponse response = new MockHttpServletResponse();
    request.setParameter("value",
        "dd0fb91c1479dcd5fd5633b2c053c526a2c1bb7bbb1d1877c4996abce15e8814");
    getTransactionInfoByIdSolidityServlet.doGet(request, response);
    try {
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      Assert.assertEquals(result.getString("id"), "dd0fb91c1479dcd5fd5" +
          "633b2c053c526a2c1bb7bbb1d1877c4996abce15e8814");
      Assert.assertEquals(result.getLongValue("fee"), 1);
      Assert.assertEquals(result.getIntValue("blockNumber"), 100);
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDoPostTransactionInfo() {
    MockHttpServletRequest request = createRequest(HttpPost.METHOD_NAME);
    String jsonParam = "{\"value\": \"" +
        "dd0fb91c1479dcd5fd5633b2c053c526a2c1bb7bbb1d1877c4996abce15e8814\"}";
    request.setContent(jsonParam.getBytes());
    try {
      MockHttpServletResponse response = new MockHttpServletResponse();
      getTransactionInfoByIdSolidityServlet.doPost(request, response);
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      Assert.assertEquals(result.getString("id"), "dd0fb91c1479dcd5fd5" +
          "633b2c053c526a2c1bb7bbb1d1877c4996abce15e8814");
      Assert.assertEquals(result.getLongValue("fee"), 1);
      Assert.assertEquals(result.getIntValue("blockTimeStamp"), 200);
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDoGetTransaction() {
    MockHttpServletRequest request = createRequest(HttpGet.METHOD_NAME);
    MockHttpServletResponse response = new MockHttpServletResponse();
    request.setParameter("value",
        "dd0fb91c1479dcd5fd5633b2c053c526a2c1bb7bbb1d1877c4996abce15e8814");
    getTransactionByIdSolidityServlet.doGet(request, response);
    try {
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      Assert.assertEquals(result.getString("txID"), "dd0fb91c1479dcd5fd5633b2c" +
          "053c526a2c1bb7bbb1d1877c4996abce15e8814");
      Assert.assertEquals(result.getString("raw_data_hex"), "5a410801123d0a2d7" +
          "47970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e" +
          "7472616374120c0a036161611203626262180a");
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDoPostTransaction() {
    MockHttpServletRequest request = createRequest(HttpPost.METHOD_NAME);
    String jsonParam = "{\"value\": \"" +
        "dd0fb91c1479dcd5fd5633b2c053c526a2c1bb7bbb1d1877c4996abce15e8814\"}";
    request.setContent(jsonParam.getBytes());
    try {
      MockHttpServletResponse response = new MockHttpServletResponse();
      getTransactionByIdSolidityServlet.doPost(request, response);
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      Assert.assertEquals(result.getString("txID"), "dd0fb91c1479dcd5fd5633b2c" +
          "053c526a2c1bb7bbb1d1877c4996abce15e8814");
      Assert.assertEquals(result.getString("raw_data_hex"), "5a410801123d0a2d7" +
          "47970652e676f6f676c65617069732e636f6d2f70726f746f636f6c2e5472616e73666572436f6e" +
          "7472616374120c0a036161611203626262180a");
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }
}