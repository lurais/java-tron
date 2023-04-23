/*
 * Copyright (c) [2016] [ <ether.camp> ] This file is part of the ethereumJ library.
 *
 * The ethereumJ library is free software: you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * The ethereumJ library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with the ethereumJ
 * library. If not, see <http://www.gnu.org/licenses/>.
 */

package org.tron.common.storage.lmdb;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.primitives.Bytes;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.slf4j.LoggerFactory;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.storage.WriteOptionsWrapper;
import org.tron.common.storage.metric.DbStat;
import org.tron.common.utils.FileUtil;
import org.tron.common.utils.StorageUtils;
import org.tron.core.db.common.DbSourceInter;
import org.tron.core.db.common.iterator.StoreIterator;
import org.tron.core.db2.common.Instance;
import org.tron.core.db2.common.WrappedByteArray;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

import java.nio.ByteBuffer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;


@Slf4j(topic = "DB")
@NoArgsConstructor
public class LmdbDataSourceImpl extends DbStat implements DbSourceInter<byte[]>,
    Iterable<Entry<byte[], byte[]>>, Instance<LmdbDataSourceImpl>  {

  private String dataBaseName;
  Env<ByteBuffer> env;
  private Dbi<ByteBuffer> database;
  private volatile boolean alive;
  private String parentPath;
  private Options options;
  private WriteOptions writeOptions;
  private ReadWriteLock resetDbLock = new ReentrantReadWriteLock();
  private static final String LMDB = "LMDB";
  private static final org.slf4j.Logger innerLogger = LoggerFactory.getLogger(LMDB);
  private Logger leveldbLogger = new Logger() {
    @Override
    public void log(String message) {
      innerLogger.info("{} {}", dataBaseName, message);
    }
  };

  /**
   * constructor.
   */
  public LmdbDataSourceImpl(String parentPath, String dataBaseName, Options options,
                            WriteOptions writeOptions) {
    this.parentPath = Paths.get(
        parentPath,
        CommonParameter.getInstance().getStorage().getDbDirectory()
    ).toString();
    this.dataBaseName = dataBaseName;
    this.options = options.logger(leveldbLogger);
    this.writeOptions = writeOptions;
    initDB();
  }

  @Override
  public void initDB() {
    resetDbLock.writeLock().lock();
    try {
      logger.debug("Init DB: {}.", dataBaseName);

      if (isAlive()) {
        return;
      }

      if (dataBaseName == null) {
        throw new IllegalArgumentException("No name set to the dbStore");
      }

      try {
        openDatabase();
        alive = true;
      } catch (IOException ioe) {
        throw new RuntimeException(String.format("Can't initialize database, %s", dataBaseName),
            ioe);
      }
      logger.debug("Init DB {} done.", dataBaseName);
    } finally {
      resetDbLock.writeLock().unlock();
    }
  }

  private void openDatabase() throws IOException {
    final Path dbPath = getDbPath();
    if (dbPath == null || dbPath.getParent() == null) {
      return;
    }
    if (!Files.isSymbolicLink(dbPath.getParent())) {
      Files.createDirectories(dbPath.getParent());
    }
    this.env = create()
        // LMDB also needs to know how large our DB might be. Over-estimating is OK.
        .setMapSize(10_485_760)
        // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
        .setMaxDbs(1)
        // Now let's open the Env. The same path can be concurrently opened and
        // used in different processes, but do not open the same path twice in
        // the same process at the same time.
        .open(getDbPath().toFile());
    try {
      database = env.openDbi(dataBaseName, MDB_CREATE);
      if (!this.getDBName().startsWith("checkpoint")) {
        logger.info("DB {} open success with stat {}.",
            this.getDBName(), env.stat());
      }
    } catch (Exception e) {
      throw e;
    }
  }

  public Path getDbPath() {
    return Paths.get(parentPath, dataBaseName);
  }

  /**
   * reset database.
   */
  public void resetDb() {
    resetDbLock.writeLock().lock();
    try {
      closeDB();
      FileUtil.recursiveDelete(getDbPath().toString());
      initDB();
    } finally {
      resetDbLock.writeLock().unlock();
    }
  }

  @Override
  public boolean isAlive() {
    return alive;
  }

  @Override
  public String getDBName() {
    return dataBaseName;
  }

  @Override
  public void setDBName(String name) {
    this.dataBaseName = name;
  }

  @Override
  public byte[] getData(byte[] key) {
    resetDbLock.readLock().lock();
    try {
      return database.get(key);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public void putData(byte[] key, byte[] value) {
    resetDbLock.readLock().lock();
    try {
      database.put(key, value, writeOptions);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public void deleteData(byte[] key) {
    resetDbLock.readLock().lock();
    try {
      database.delete(key, writeOptions);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Deprecated
  @Override
  public Set<byte[]> allKeys() {
    resetDbLock.readLock().lock();
    try (DBIterator iterator = getDBIterator()) {
      Set<byte[]> result = Sets.newHashSet();
      for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
        result.add(iterator.peekNext().getKey());
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Deprecated
  @Override
  public Set<byte[]> allValues() {
    resetDbLock.readLock().lock();
    try (DBIterator iterator = getDBIterator()) {
      Set<byte[]> result = Sets.newHashSet();
      for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
        result.add(iterator.peekNext().getValue());
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  public Set<byte[]> getlatestValues(long limit) {
    if (limit <= 0) {
      return Sets.newHashSet();
    }
    resetDbLock.readLock().lock();
    try (DBIterator iterator = getDBIterator()) {
      Set<byte[]> result = Sets.newHashSet();
      long i = 0;
      iterator.seekToLast();
      if (iterator.hasNext()) {
        result.add(iterator.peekNext().getValue());
        i++;
      }
      for (; iterator.hasPrev() && i++ < limit; iterator.prev()) {
        result.add(iterator.peekPrev().getValue());
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  public Set<byte[]> getValuesNext(byte[] key, long limit) {
    if (limit <= 0) {
      return Sets.newHashSet();
    }
    resetDbLock.readLock().lock();
    try (DBIterator iterator = getDBIterator()) {
      Set<byte[]> result = Sets.newHashSet();
      long i = 0;
      for (iterator.seek(key); iterator.hasNext() && i++ < limit; iterator.next()) {
        result.add(iterator.peekNext().getValue());
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  public List<byte[]> getKeysNext(byte[] key, long limit) {
    if (limit <= 0) {
      return new ArrayList<>();
    }
    resetDbLock.readLock().lock();
    try (DBIterator iterator = getDBIterator()) {
      List<byte[]> result = new ArrayList<>();
      long i = 0;
      for (iterator.seek(key); iterator.hasNext() && i++ < limit; iterator.next()) {
        result.add(iterator.peekNext().getKey());
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  public Map<byte[], byte[]> getNext(byte[] key, long limit) {
    if (limit <= 0) {
      return Collections.emptyMap();
    }
    resetDbLock.readLock().lock();
    try (DBIterator iterator = getDBIterator()) {
      Map<byte[], byte[]> result = new HashMap<>();
      long i = 0;
      for (iterator.seek(key); iterator.hasNext() && i++ < limit; iterator.next()) {
        Entry<byte[], byte[]> entry = iterator.peekNext();
        result.put(entry.getKey(), entry.getValue());
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public Map<WrappedByteArray, byte[]> prefixQuery(byte[] key) {
    resetDbLock.readLock().lock();
    try (DBIterator iterator = getDBIterator()) {
      Map<WrappedByteArray, byte[]> result = new HashMap<>();
      for (iterator.seek(key); iterator.hasNext(); iterator.next()) {
        Entry<byte[], byte[]> entry = iterator.peekNext();
        if (Bytes.indexOf(entry.getKey(), key) == 0) {
          result.put(WrappedByteArray.of(entry.getKey()), entry.getValue());
        } else {
          return result;
        }
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public long getTotal() throws RuntimeException {
    resetDbLock.readLock().lock();
    try (DBIterator iterator = getDBIterator()) {
      long total = 0;
      for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
        total++;
      }
      return total;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  private void updateByBatchInner(Map<byte[], byte[]> rows) throws Exception {
    try (WriteBatch batch = database.createWriteBatch()) {
      innerBatchUpdate(rows,batch);
      database.write(batch, writeOptions);
    }
  }

  private void updateByBatchInner(Map<byte[], byte[]> rows, WriteOptions options) throws Exception {
    try (WriteBatch batch = database.createWriteBatch()) {
      innerBatchUpdate(rows,batch);
      database.write(batch, options);
    }
  }

  private void innerBatchUpdate(Map<byte[], byte[]> rows, WriteBatch batch) {
    rows.forEach((key, value) -> {
      if (value == null) {
        batch.delete(key);
      } else {
        batch.put(key, value);
      }
    });
  }

  @Override
  public void updateByBatch(Map<byte[], byte[]> rows, WriteOptionsWrapper options) {
    resetDbLock.readLock().lock();
    try {
      updateByBatchInner(rows, options.level);
    } catch (Exception e) {
      try {
        updateByBatchInner(rows, options.level);
      } catch (Exception e1) {
        throw new RuntimeException(e);
      }
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public void updateByBatch(Map<byte[], byte[]> rows) {
    resetDbLock.readLock().lock();
    try {
      updateByBatchInner(rows);
    } catch (Exception e) {
      try {
        updateByBatchInner(rows);
      } catch (Exception e1) {
        throw new RuntimeException(e);
      }
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public boolean flush() {
    return false;
  }

  @Override
  public void closeDB() {
    resetDbLock.writeLock().lock();
    try {
      if (!isAlive()) {
        return;
      }
      database.close();
      alive = false;
    } catch (Exception e) {
      logger.error("Failed to find the dbStore file on the closeDB: {}.", dataBaseName, e);
    } finally {
      resetDbLock.writeLock().unlock();
    }
  }

  @Override
  public org.tron.core.db.common.iterator.DBIterator iterator() {
    return new StoreIterator(getDBIterator());
  }

  public Stream<Entry<byte[], byte[]>> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  @Override
  public LmdbDataSourceImpl newInstance() {
    return new LmdbDataSourceImpl(StorageUtils.getOutputDirectoryByDbName(dataBaseName),
        dataBaseName, options, writeOptions);
  }

  private DBIterator getDBIterator() {
    ReadOptions readOptions = new ReadOptions().fillCache(false);
    return  database.iterate();
  }


  /**
   *                                Compactions
   * Level  Files Size(MB) Time(sec) Read(MB) Write(MB)
   * --------------------------------------------------
   *   1        2        2         0        0         2
   *   2        1        1         0        0         1
   */
  @Override
  public List<String> getStats() throws Exception {
    resetDbLock.readLock().lock();
    try {
      if (!isAlive()) {
        return Collections.emptyList();
      }
      String stat = database.stat();
      String[] stats = stat.split("\n");
      return Arrays.stream(stats).skip(3).collect(Collectors.toList());
    } finally {
      resetDbLock.readLock().unlock();
    }
  }

  @Override
  public String getEngine() {
    return LMDB;
  }

  @Override
  public String getName() {
    return this.dataBaseName;
  }

  @Override public void stat() {
    this.statProperty();
  }

}
