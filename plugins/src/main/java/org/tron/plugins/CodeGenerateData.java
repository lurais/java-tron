package org.tron.plugins;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.tron.core.capsule.AccountCapsule;
import org.tron.plugins.utils.ByteArray;
import org.tron.plugins.utils.DBUtils;
import org.tron.plugins.utils.db.LevelDBIterator;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

/**
 *
 * @author liukai
 * @since 2023/5/31.
 */
@Slf4j(topic = "code")
@CommandLine.Command(name = "code",
    description = "code",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: successful",
        "1: failed"})
public class CodeGenerateData implements Callable<Integer> {
  private static String DB_DIR = "/Users/penghuan/runtime/tron/tov/output-directory/";
  private static String GENERATE_DIR = "/Users/liukai/workspaces/tmp/generate_data/code_address.txt";
  private static String DATABASE_NAME = "account";
  private static int count = 0;
  private static DB db;

  @CommandLine.Option(
     names = {"--src", "-s"},
     required = true,
     order = 3)
  private String dbDir;

  @CommandLine.Option(
     names = {"--dest", "-ds"},
     required = true,
     order = 4)
  private String generateDir;

  @Override
  public Integer call() throws Exception {
  DB_DIR = dbDir;
  GENERATE_DIR = generateDir;
  logger.info("dbDir: " + DB_DIR);
  logger.info("generateDir: " + GENERATE_DIR);
  execute();
  return 1;
  }

  public static LevelDBIterator getLevelDBIterator(String dbName) throws IOException {
  return new LevelDBIterator(getDBIterator(dbName));
  }

  public static DBIterator getDBIterator(String dbName) throws IOException {
  Path path = Paths.get(DB_DIR, "database", dbName);
  db = DBUtils.newLevelDb(path);
  return db.iterator();
  }

  public int execute() throws IOException {
    Path path = Paths.get(DB_DIR, "database", "account");
    db = DBUtils.newLevelDb(path);

    byte[]  key = ByteArray.fromHexString("413ae9741dd749698b88ae28ace1d91dfb0dfaed2c");
    AccountCapsule accountCapsule = new AccountCapsule(db.get(key));
    System.out.println(accountCapsule);

//  BufferedWriter accountFile = createWriteFileWriter();
//  LevelDBIterator iterator = getLevelDBIterator(DATABASE_NAME);
//  iterator.seekToFirst();
//  while (count < 2_000_000) {
//   count++;
//
//   // 通用部分
//   String hexAddress = Hex.toHexString(iterator.getKey());
//
//
//   // 通用部份
//   iterator.next();
//  }
//  accountFile.flush();
//  accountFile.close();
  return 1;
  }


  private static BufferedWriter createWriteFileWriter() throws IOException {
  File indexFile = new File(GENERATE_DIR);
  return new BufferedWriter(new FileWriter(indexFile));
  }

  private static void writeToFile(BufferedWriter accountFile, String data) throws IOException {
  accountFile.write(data);
  accountFile.newLine();
  // for test
//  accountFile.flush();
  if (count % 10000 == 0) {
   accountFile.flush();
  }
  }

  public static void main(String[] args) throws IOException {
  new CodeGenerateData().execute();
  }
}