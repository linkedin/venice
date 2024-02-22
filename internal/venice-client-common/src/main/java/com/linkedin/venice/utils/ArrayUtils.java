package com.linkedin.venice.utils;

public final class ArrayUtils {
  private ArrayUtils() {
  }

  /**
   * This is mostly a copy of Arrays.compareUnsigned(T[], T[]) from Java 9.
   */
  public static int compareUnsigned(byte[] a, byte[] b) {
    if (a == b) {
      return 0;
    }

    // A null array is less than a non-null array
    if (a == null || b == null) {
      return a == null ? -1 : 1;
    }

    int length = Math.min(a.length, b.length);
    for (int i = 0; i < length; i++) {
      byte oa = a[i];
      byte ob = b[i];
      if (oa != ob) {
        return Byte.toUnsignedInt(oa) - Byte.toUnsignedInt(ob);
      }
    }

    return a.length - b.length;
  }
}
