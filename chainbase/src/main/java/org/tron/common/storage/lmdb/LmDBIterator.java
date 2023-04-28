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
//import org.tron.core.db.common.iterator.DBIterator;
//
//public class LmDBIterator implements DBIterator {
//
//  private final Iterator<CursorIterable.KeyVal<?>> iterator;
//  private final CursorIterable cursorIterable;
//
//  LmDBIterator(CursorIterable cursorIterable) {
//    cursorIterable = cursorIterable;
//    iterator = cursorIterable.iterator();
//  }
//
//  public void close() {
//    cursorIterable.close();
//  }
//
//  public void remove() {
//    throw new UnsupportedOperationException();
//  }
//
//  public void seek(byte[] key) {
//    try {
//      cursorIterable=cursorIterable..;
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
