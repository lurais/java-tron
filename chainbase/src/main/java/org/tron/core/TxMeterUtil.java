package org.tron.core;

public class TxMeterUtil {

  public static long calcLengthSum(byte[]... bytes) {
    long sum = 0L;
    for (byte[] bt : bytes) {
      if (bt != null) {
        sum += bt.length;
      }
    }
    return sum;
  }

}
