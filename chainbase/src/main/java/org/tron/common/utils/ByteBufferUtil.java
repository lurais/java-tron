package org.tron.common.utils;

import java.nio.ByteBuffer;

public class ByteBufferUtil {

  private static byte[] toByteArray(ByteBuffer byteBuffer) {
    byte[] bytesArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytesArray, 0, bytesArray.length);
    return bytesArray;
  }

  public static ByteBuffer toBuffer(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }
}
