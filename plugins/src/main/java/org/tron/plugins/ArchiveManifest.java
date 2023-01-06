package org.tron.plugins;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import ch.qos.logback.core.encoder.ByteArrayUtil;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.Filename;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.InternalUserComparator;
import org.iq80.leveldb.impl.Level;
import org.iq80.leveldb.impl.Level0;
import org.iq80.leveldb.impl.TableCache;
import org.iq80.leveldb.impl.VersionSet;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.CustomUserComparator;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.InternalTableIterator;
import org.iq80.leveldb.util.Slice;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j(topic = "archive")
/*
  a helper to rewrite leveldb manifest.
 */
public class ArchiveManifest implements Callable<Boolean> {


  private static final String KEY_ENGINE = "ENGINE";
  private static final String LEVELDB = "LEVELDB";

  private final Path srcDbPath;
  private final String name;
  private final Options options;
  private final long startTime;
  Random r = new Random();



  private static final int CPUS  = Runtime.getRuntime().availableProcessors();

  public ArchiveManifest(String src, String name, int maxManifestSize, int maxBatchSize) {
    this.name = name;
    this.srcDbPath = Paths.get(src, name);
    this.startTime = System.currentTimeMillis();
    this.options = newDefaultLevelDbOptions();
    this.options.maxManifestSize(maxManifestSize);
    this.options.maxBatchSize(maxBatchSize);
  }

  @Override
  public Boolean call() throws Exception {
    return doArchive();
  }

  public static org.iq80.leveldb.Options newDefaultLevelDbOptions() {
    org.iq80.leveldb.Options dbOptions = new org.iq80.leveldb.Options();
    dbOptions.createIfMissing(true);
    dbOptions.paranoidChecks(true);
    dbOptions.verifyChecksums(true);
    dbOptions.compressionType(CompressionType.SNAPPY);
    dbOptions.blockSize(4 * 1024);
    dbOptions.writeBufferSize(10 * 1024 * 1024);
    dbOptions.cacheSize(10 * 1024 * 1024L);
    dbOptions.maxOpenFiles(1000);
    dbOptions.maxBatchSize(64_000);
    dbOptions.maxManifestSize(128);
    return dbOptions;
  }

  public static void main(String[] args) {
    int code = run(args);
    logger.info("exit code {}.", code);
    System.out.printf("exit code %d.\n", code);
    System.exit(code);
  }

  public static int run(String[] args) {
    Args parameters = new Args();
    CommandLine commandLine = new CommandLine(parameters);
    commandLine.parseArgs(args);
    if (parameters.help) {
      commandLine.usage(System.out);
      return 0;
    }

    File dbDirectory = new File(parameters.databaseDirectory);
    if (!dbDirectory.exists()) {
      logger.info("Directory {} does not exist.", parameters.databaseDirectory);
      return 404;
    }

    List<File> files = Arrays.stream(Objects.requireNonNull(dbDirectory.listFiles()))
        .filter(File::isDirectory).collect(
            Collectors.toList());

    if (files.isEmpty()) {
      logger.info("Directory {} does not contain any database.", parameters.databaseDirectory);
      return 0;
    }
    final long time = System.currentTimeMillis();
    final List<Future<Boolean>> res = new ArrayList<>();
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(
        CPUS, 16 * CPUS, 1, TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(CPUS, true), Executors.defaultThreadFactory(),
        new ThreadPoolExecutor.CallerRunsPolicy());

    executor.allowCoreThreadTimeOut(true);
    List<String> dbName = Arrays.asList("account");
    files.stream().filter(file->(!file.getName().contains("second"))&&(!file.getName().contains("third"))&&dbName.contains(file.getName())).forEach(f -> res.add(
        executor.submit(new ArchiveManifest(parameters.databaseDirectory, f.getName(),
            parameters.maxManifestSize, parameters.maxBatchSize))));
    int fails = res.size();

    for (Future<Boolean> re : res) {
      try {
        if (Boolean.TRUE.equals(re.get())) {
          fails--;
        }
      } catch (InterruptedException e) {
        logger.error("{}", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        logger.error("{}", e);
      }
    }

    executor.shutdown();
    logger.info("DatabaseDirectory:{}, maxManifestSize:{}, maxBatchSize:{},"
            + "database reopen use {} seconds total.",
        parameters.databaseDirectory, parameters.maxManifestSize, parameters.maxBatchSize,
        (System.currentTimeMillis() - time) / 1000);
    if (fails > 0) {
      logger.error("Failed!!!!!!!!!!!!!!!!!!!!!!!! size:{}", fails);
    }
    return fails;
  }

  public void open() throws IOException {
    DB database = factory.open(this.srcDbPath.toFile(), this.options);
    try {
      Options options = this.options;

      int tableCacheSize = options.maxOpenFiles() - 10;
      InternalKeyComparator internalKeyComparator;
      //use custom comparator if set
      DBComparator comparator = options.comparator();
      UserComparator userComparator;
      if (comparator != null) {
        userComparator = new CustomUserComparator(comparator);
      } else {
        userComparator = new BytewiseComparator();
      }
      internalKeyComparator = new InternalKeyComparator(userComparator);
      TableCache tableCache = new TableCache(srcDbPath.toFile(), tableCacheSize,
          new InternalUserComparator(internalKeyComparator), options.verifyChecksums());
      VersionSet versions = new VersionSet(srcDbPath.toFile(), tableCache, options, internalKeyComparator);
      // load  (and recover) current version
      versions.recover();

      logger.info(srcDbPath.toString());
      StringBuilder sb = new StringBuilder();
      Level0 level0 = (Level0) getAttr("org.iq80.leveldb.impl.Version","level0",versions.getCurrent());
      List<Level> levels = (List<Level>) getAttr("org.iq80.leveldb.impl.Version","levels",versions.getCurrent());

      Map<Integer,Long> numMap = keyToFiles(levels,level0,versions.getTableCache());
      StringBuilder sb2 = new StringBuilder();
      logger.info("travel sst final:"+sb2.toString());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    database.close();
  }

  private Map<Integer, Long> keyToFiles(List<Level> levels, Level0 level0,
                                        TableCache tableCache)
      throws IllegalAccessException, NoSuchFieldException, ClassNotFoundException, IOException {
    Map<Integer, BufferedWriter> levelToFile =null;
    try {
      levelToFile = initLevelFile(name);
      for (FileMetaData fileMetaData : level0.getFiles()) {
        List<byte[]> keys = doFetchLevel0Keys(fileMetaData, level0, tableCache);
        writeToFile(keys, levelToFile.get(0));
      }
      for (Level level : levels) {
        for (FileMetaData fileMetaData : level.getFiles()) {
          List<byte[]> keys = doFetchKeys(fileMetaData, level, tableCache);
          keys = randomPick(keys, level.getLevelNumber());
          writeToFile(keys, levelToFile.get(level.getLevelNumber()));
        }
      }
      return null;
    }catch (Exception e){
      logger.info("key write error",e);
    }finally {
      if(levelToFile!=null){
        for(Map.Entry entry:levelToFile.entrySet()){
          BufferedWriter bufferedWriter = (BufferedWriter) entry.getValue();
          if(bufferedWriter!=null){
            bufferedWriter.close();
          }
        }
      }
    }
    return null;
  }

  private List<byte[]> randomPick(List<byte[]> keys, int levelNumber) {
    List<byte[]> res = new LinkedList<>();
    for(byte[] key :keys){
      if (r.nextInt(1000) < 10) {
        res.add(key);
      }
    }
    return res;
  }

  private void writeToFile(List<byte[]> keys, BufferedWriter bufferedWriter) throws IOException {
    if(keys.size()<1){
      return;
    }
    StringBuilder sb = new StringBuilder();
    for(byte[] key:keys){
      sb.append(ByteArrayUtil.toHexString(key)+"\r\n");
    }
    bufferedWriter.write(sb.toString());
  }

  private Map<Integer, BufferedWriter> initLevelFile(String name) {
    try {
      Map<Integer,BufferedWriter> fileWriterMap = new HashMap<>();
      for(int i=0;i<=5;i++){
        File file = new File(name+"level"+i);
        fileWriterMap.put(i,new BufferedWriter(new FileWriter(name+"level"+i)));
      }
      return fileWriterMap;
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  public static Object getAttr(String className,String fieldName,Object target) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {

    //获取Class对象
    // Class c=Class.forName("org.iq80.leveldb.impl.DbImpl");
    Class c=Class.forName(className);

    //获某个特定的属性值
    // Field versions=c.getDeclaredField("versions"); //通过属性名来区分
    Field versions=c.getDeclaredField(fieldName); //通过属性名来区分
    versions.setAccessible(Boolean.TRUE);
    return versions.get(target);
  }


  public Map<Integer,Long> getLevelCount(DB db, DB third, List<Level> levels, Level0 level0, TableCache tableCache)
      throws IllegalAccessException, NoSuchFieldException, ClassNotFoundException {
    Map<Integer,Long> levelToNum = initLevelMap(levels.size());
    String preFix = name + " level"+level0.getLevelNumber()+" get stat:";
    int currentStat0 = 0;
    for(FileMetaData fileMetaData: level0.getFiles()){
      if(currentStat0 > 5){
        continue;
      }
      List<byte[]> keys = doFetchLevel0Keys(fileMetaData,level0,tableCache);
      statTime(third,keys,Boolean.TRUE,Boolean.TRUE,preFix);
      currentStat0++;
    }
    for(Level level:levels){
      int currentStat = 0;
      String preFix2 = name+" level"+level.getLevelNumber()+" get stat:";
      for(FileMetaData fileMetaData:level.getFiles()) {
        // 时间测量
        if(level.getLevelNumber()<5 && currentStat>=5){
          continue;
        }
        List<byte[]> keys = doFetchKeys(fileMetaData, level, tableCache);
        if(currentStat<5) {
          statTime(third, keys, Boolean.TRUE, Boolean.TRUE, preFix2);
          currentStat++;
        }
        if(level.getLevelNumber()>=5){
          long keysFind = statTime(db,keys,Boolean.FALSE,Boolean.FALSE,preFix2);
          levelToNum.put(level.getLevelNumber(),levelToNum.get(level.getLevelNumber())+keysFind);
        }
      }
    }
    return levelToNum;
  }

  private long statTime(DB db, List<byte[]> keys, Boolean statNotFound, Boolean printResult, String preFix) {
    long find = 0L;
    List<Double> times = new ArrayList<>();
    for(byte[] key:keys){
      long begin = System.nanoTime();
      byte[] value = db.get(key);
      double fetchTime = (System.nanoTime()-begin)/1000000.0;
      if(value!=null){
        find+=1;
      }
      if(statNotFound){
        times.add(fetchTime);
        continue;
      }
      if(value!=null){
        times.add(fetchTime);
      }
    }
    if(!printResult){
      return find;
    }
    StringBuilder sb = new StringBuilder();
    for(Double time:times){
      sb.append(time+",");
    }
    logger.info(preFix+":"+sb.toString());
    return find;
  }

  private Map<Integer, Long> initLevelMap(int size) {
    Map<Integer,Long> levelMap = new HashMap<>();
    for(int i=0;i<=size;i++){
      levelMap.put(i,0L);
    }
    return levelMap;
  }

  private List<byte[]> doFetchLevel0Keys(FileMetaData fileMetaData, Level0 level0,
                                   TableCache tableCache)
      throws IllegalAccessException, NoSuchFieldException, ClassNotFoundException {
    List<byte[]> result = new ArrayList<>();
    try {
      // open the iterator
      InternalTableIterator iterator = tableCache.newIterator(fileMetaData);
      iterator.seekToFirst();

      while (iterator.hasNext()) {
        // parse the key in the block
        Map.Entry<InternalKey, Slice> entry = iterator.next();
        InternalKey internalKey = entry.getKey();
        if (internalKey == null) {
          continue;
        }
        result.add(internalKey.getUserKey().getBytes());
      }
      return result;
    }catch (Exception e){
      return result;
    }
  }

  private List<byte[]> doFetchKeys(FileMetaData fileMetaData, Level level, TableCache tableCache)
      throws IllegalAccessException, NoSuchFieldException, ClassNotFoundException {
    List<byte[]> result = new ArrayList<>();
    try {
      // open the iterator
      InternalTableIterator iterator = tableCache.newIterator(fileMetaData);
      iterator.seekToFirst();

      while (iterator.hasNext()) {
        // parse the key in the block
        Map.Entry<InternalKey, Slice> entry = iterator.next();
        InternalKey internalKey = entry.getKey();
        if (internalKey == null) {
          continue;
        }
        result.add(internalKey.getUserKey().getBytes());
      }
      return result;
    }catch (Exception e){
      return result;
    }
  }

  public boolean checkManifest(String dir) throws IOException {
    // Read "CURRENT" file, which contains a pointer to the current manifest file
    File currentFile = new File(dir, Filename.currentFileName());
    if (!currentFile.exists()) {
      return false;
    }
    String currentName = com.google.common.io.Files.asCharSource(currentFile, UTF_8).read();
    if (currentName.isEmpty() || currentName.charAt(currentName.length() - 1) != '\n') {
      return false;
    }
    currentName = currentName.substring(0, currentName.length() - 1);
    File current = new File(dir, currentName);
    if (!current.isFile()) {
      return false;
    }
    long maxSize = options.maxManifestSize();
    if (maxSize < 0) {
      return false;
    }
    logger.info("CurrentName {}/{},size {} kb.", dir, currentName, current.length() / 1024);
    if ("market_pair_price_to_order".equalsIgnoreCase(this.name)) {
      logger.info("Db {} ignored.", this.name);
      return false;
    }
    return current.length() >= maxSize * 1024 * 1024;
  }

  public boolean doArchive() throws IOException {
    File levelDbFile = srcDbPath.toFile();
    if (!levelDbFile.exists()) {
      logger.info("File {},does not exist, ignored.", srcDbPath.toString());
      return true;
    }
    if (!checkEngine()) {
      logger.info("Db {},not leveldb, ignored.", this.name);
      return true;
    }
    if (!checkManifest(levelDbFile.toString())) {
      logger.info("Db {},no need, ignored.", levelDbFile.toString());
      return true;
    }
    open();
    logger.info("Db {} archive use {} ms.", this.name, (System.currentTimeMillis() - startTime));
    return true;
  }

  public boolean checkEngine() {
    String dir = this.srcDbPath.toString();
    String enginePath = dir + File.separator + "engine.properties";
    if (!new File(enginePath).exists() && !writeProperty(enginePath, KEY_ENGINE, LEVELDB)) {
      return false;
    }
    String engine = readProperty(enginePath, KEY_ENGINE);
    return LEVELDB.equals(engine);
  }

  public static String readProperty(String file, String key) {
    try (FileInputStream fileInputStream = new FileInputStream(file);
         InputStream inputStream = new BufferedInputStream(fileInputStream)) {
      Properties prop = new Properties();
      prop.load(inputStream);
      return new String(prop.getProperty(key, "").getBytes(StandardCharsets.ISO_8859_1),
          UTF_8);
    } catch (Exception e) {
      logger.error("{}", e);
      return "";
    }
  }

  public static boolean writeProperty(String file, String key, String value) {
    try (OutputStream out = new FileOutputStream(file);
         FileInputStream fis = new FileInputStream(file);
         BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, UTF_8))) {
      BufferedReader bf = new BufferedReader(new InputStreamReader(fis, UTF_8));
      Properties properties = new Properties();
      properties.load(bf);
      properties.setProperty(key, value);
      properties.store(bw, "Generated by the application.  PLEASE DO NOT EDIT! ");
    } catch (Exception e) {
      logger.warn("{}", e);
      return false;
    }
    return true;
  }

  public static class Args {

    @Option(names = {"-d", "--database-directory"},
        defaultValue = "output-directory/database",
        description = "java-tron database directory. Default: ${DEFAULT-VALUE}")
    private  String databaseDirectory;

    @Option(names = {"-b", "--batch-size" },
        defaultValue = "80000",
        description = "deal manifest batch size. Default: ${DEFAULT-VALUE}")
    private  int maxBatchSize;

    @Option(names = {"-m", "--manifest-size" },
        defaultValue = "0",
        description = "manifest  min size(M) to archive. Default: ${DEFAULT-VALUE}")
    private  int maxManifestSize;

    @Option(names = {"-h", "--help"}, help = true)
    private  boolean help;
  }
}