package org.tron.plugins;

import ch.qos.logback.core.util.FileUtil;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.DB;
import org.tron.plugins.utils.ByteArray;
import org.tron.plugins.utils.DBUtils;
import org.tron.plugins.utils.FileUtils;
import org.tron.plugins.utils.db.DbTool;
import picocli.CommandLine;

@Slf4j(topic = "seek")
@CommandLine.Command(name = "seek",
    description = "Seek data for java-tron.",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0:Successful",
        "1:Internal error: exception occurred,please check toolkit.log"})
public class DbSeek implements Callable<Integer> {

  private static final String TRANS_DB_NAME = "trans";

  @CommandLine.Spec
  CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(
      names = {"--keyPath", "-kp"},
      required = true,
      description = "the database path to be split or merged.",
      order = 3)
  private String keyPath;

  @CommandLine.Option(
      names = {"--help", "-h"},
      order = 5)
  private boolean help;

  String parentPath = "output-directory/database";

  @Override
  public Integer call() throws IOException {
    if (help) {
      spec.commandLine().usage(System.out);
      return 0;
    }
    List<String> list = new ArrayList<>(10000);
    readFileToList(list,keyPath);
    logger.info("read file list size:"+list.size()+", dbPath:"+Paths.get(parentPath, TRANS_DB_NAME).toString());
    DB db = null;
    try {
      db = DBUtils.newLevelDb(Paths.get(parentPath, TRANS_DB_NAME));
      seekInDb(list,db);
      return 0;
    } catch (Exception e) {
      logger.error("{}", e);
      spec.commandLine().getErr().println(spec.commandLine().getColorScheme()
          .errorText(e.getMessage()));
      spec.commandLine().usage(System.out);
      return 1;
    } finally {
      if (db != null) {
        db.close();
      }
    }
  }

  private void seekInDb(List<String> list, DB db) {
    for(String item:list){
      try {
        long beg = System.nanoTime();
        byte[] val = db.get(ByteArray.fromHexString(item));
        logger.info((val!=null)+"containsTransaction has cost:"+(System.nanoTime()-beg)*0.000001);
        Thread.sleep(beg%20);
      }catch(Exception e){
        e.printStackTrace();
      }
    }
  }

  private void readFileToList(List<String> list, String keyPath) {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(keyPath));
      String line;
      while ((line = reader.readLine()) != null) {
        list.add(line);
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}