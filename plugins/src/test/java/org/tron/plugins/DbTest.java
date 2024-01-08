package org.tron.plugins;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.UUID;
import org.bouncycastle.util.encoders.Hex;
import org.iq80.leveldb.DB;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.tron.plugins.utils.ByteArray;
import org.tron.plugins.utils.DBUtils;
import org.tron.plugins.utils.MarketUtils;
import picocli.CommandLine;

public class DbTest {

  String INPUT_DIRECTORY;
  private static final String ACCOUNT = "account";
  private static final String MARKET = DBUtils.MARKET_PAIR_PRICE_TO_ORDER;
  CommandLine cli = new CommandLine(new Toolkit());
  String tmpDir = System.getProperty("java.io.tmpdir");

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();


  @Before
  public void init() throws IOException {
    INPUT_DIRECTORY = temporaryFolder.newFolder().toString();
    initDB(new File(INPUT_DIRECTORY, ACCOUNT));
    initDB(new File(INPUT_DIRECTORY, MARKET));
    initDB(new File(INPUT_DIRECTORY, DBUtils.CHECKPOINT_DB_V2));
    initDB(new File(INPUT_DIRECTORY, DbInspect.DELEGATION_DB));
  }

  private static void initDB(File file) throws IOException {
    if (DBUtils.CHECKPOINT_DB_V2.equalsIgnoreCase(file.getName())) {
      File dbFile = new File(file, DBUtils.CHECKPOINT_DB_V2);
      if (dbFile.mkdirs()) {
        for (int i = 0; i < 3; i++) {
          try (DB db = DBUtils.newLevelDb(Paths.get(dbFile.getPath(),
              System.currentTimeMillis() + ""))) {
            for (int j = 0; j < 100; j++) {
              byte[] bytes = UUID.randomUUID().toString().getBytes();
              db.put(bytes, bytes);
            }
          }
        }
      }
      return;
    }
    try (DB db = DBUtils.newLevelDb(file.toPath())) {
      if (MARKET.equalsIgnoreCase(file.getName())) {
        byte[] sellTokenID1 = ByteArray.fromString("100");
        byte[] buyTokenID1 = ByteArray.fromString("200");
        byte[] pairPriceKey1 = MarketUtils.createPairPriceKey(
            sellTokenID1,
            buyTokenID1,
            1000L,
            2001L
        );
        byte[] pairPriceKey2 = MarketUtils.createPairPriceKey(
            sellTokenID1,
            buyTokenID1,
            1000L,
            2002L
        );
        byte[] pairPriceKey3 = MarketUtils.createPairPriceKey(
            sellTokenID1,
            buyTokenID1,
            1000L,
            2003L
        );


        //Use out-of-order insertion，key in store should be 1,2,3
        db.put(pairPriceKey1, "1".getBytes(StandardCharsets.UTF_8));
        db.put(pairPriceKey2, "2".getBytes(StandardCharsets.UTF_8));
        db.put(pairPriceKey3, "3".getBytes(StandardCharsets.UTF_8));
      } else {
        for (int i = 0; i < 100; i++) {
          byte[] bytes = UUID.randomUUID().toString().getBytes();
          db.put(bytes, bytes);
        }
        db.put((2903 + "-" + "41547098d5b81f8ec49376589bb49e9c82d19ad17d" + "-vi").getBytes(),BigInteger.valueOf(100L).toByteArray());
        db.put((2903 + "-" + "4154a6de9a4afd83f14b260531bddf632ff5b64f55" + "-vi").getBytes(),BigInteger.valueOf(100L).toByteArray());
        db.put((2904 + "-" + "41547098d5b81f8ec49376589bb49e9c82d19ad17d" + "-vi").getBytes(),BigInteger.valueOf(107L).toByteArray());
        db.put((2904 + "-" + "4154a6de9a4afd83f14b260531bddf632ff5b64f55" + "-vi").getBytes(),BigInteger.valueOf(108L).toByteArray());
      }
    }
  }
}
