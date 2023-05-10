//package org.tron.common.storage.lmdb;
//
//import java.util.Iterator;
//import java.nio.ByteBuffer;
//import java.util.AbstractMap;
//import java.util.Map;
//import java.util.NoSuchElementException;
//import org.fusesource.leveldbjni.internal.NativeDB;
//import org.fusesource.leveldbjni.internal.NativeIterator;
//import org.lmdbjava.CursorIterable;
//import org.lmdbjava.Dbi;
//import org.lmdbjava.Env;
//import org.lmdbjava.KeyRange;
//import org.lmdbjava.Txn;
//import org.tron.common.utils.ByteBufferUtil;
//import org.tron.core.db.common.iterator.DBIterator;
//
//public class LmDBIterator implements DBIterator {
//
//  private  Dbi<ByteBuffer> db = null;
//  private  CursorIterable<ByteBuffer> ci = null;
//  private Txn<ByteBuffer> txn = null;
//
//  LmDBIterator(CursorIterable cursorIterable, Dbi dbi, Env env) {
//    cursorIterable = cursorIterable;
//    db = dbi;
//    txn = env
//  }
//
//  public void close() {
//    ci.close();
//  }
//
//  public void remove() {
//    throw new UnsupportedOperationException();
//  }
//
//  public void seek(byte[] key) {
//    try {
//      final KeyRange<ByteBuffer> range = KeyRange.atLeast(ByteBufferUtil.toBuffer(key));
//      ci = db.iterate(null,range);
//    } catch (NativeDB.DBException e) {
//      if( e.isNotFound() ) {
//        throw new NoSuchElementException();
//      } else {
//        throw new RuntimeException(e);
//      }
//    }
//  }
//
//  public void seekToFirst() {
//    cursorIterable.();
//  }
//
//  public void seekToLast() {
//    iterator.seekToLast();
//  }
//
//
//  public Map.Entry<byte[], byte[]> peekNext() {
//    if(!iterator.isValid()) {
//      throw new NoSuchElementException();
//    }
//    try {
//      return new AbstractMap.SimpleImmutableEntry<byte[],byte[]>(iterator.key(), iterator.value());
//    } catch (NativeDB.DBException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  public boolean hasNext() {
//    return iterator.isValid();
//  }
//
//  public Map.Entry<byte[], byte[]> next() {
//    Map.Entry<byte[], byte[]> rc = peekNext();
//    try {
//      iterator.next();
//    } catch (NativeDB.DBException e) {
//      throw new RuntimeException(e);
//    }
//    return rc;
//  }
//
//  public boolean hasPrev() {
//    if( !iterator.isValid() )
//      return false;
//    try {
//      iterator.prev();
//      try {
//        return iterator.isValid();
//      } finally {
//        if (iterator.isValid()) {
//          iterator.next();
//        } else {
//          iterator.seekToFirst();
//        }
//      }
//    } catch (NativeDB.DBException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  public Map.Entry<byte[], byte[]> peekPrev() {
//    try {
//      iterator.prev();
//      try {
//        return peekNext();
//      } finally {
//        if (iterator.isValid()) {
//          iterator.next();
//        } else {
//          iterator.seekToFirst();
//        }
//      }
//    } catch (NativeDB.DBException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  public Map.Entry<byte[], byte[]> prev() {
//    Map.Entry<byte[], byte[]> rc = peekPrev();
//    try {
//      iterator.prev();
//    } catch (NativeDB.DBException e) {
//      throw new RuntimeException(e);
//    }
//    return rc;
//  }
//}
