package com.linkedin.venice.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang.Validate;


public class ByteArray implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final ByteArray EMPTY = new ByteArray();

  private final byte[] underlying;

  public ByteArray(byte... underlying) {
    Validate.notNull(underlying);
    this.underlying = underlying;
  }

  public byte[] get() {
    return underlying;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(underlying);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof ByteArray))
      return false;
    ByteArray other = (ByteArray) obj;
    return Arrays.equals(underlying, other.underlying);
  }

  @Override
  public String toString() {
    return ByteUtils.toHexString(underlying);
  }

  /**
   * Translate the each ByteArray in an iterable into a hexidecimal string
   *
   * @param arrays The array of bytes to translate
   * @return An iterable of converted strings
   */
  public static Iterable<String> toHexStrings(Iterable<ByteArray> arrays) {
    ArrayList<String> ret = new ArrayList<String>();
    for (ByteArray array: arrays)
      ret.add(ByteUtils.toHexString(array.get()));
    return ret;
  }

  public int length() {
    return underlying.length;
  }

  public boolean startsWith(byte[] prefixBytes) {
    if (underlying == null || prefixBytes == null || underlying.length < prefixBytes.length) {
      return false;
    }
    for (int i = 0; i < prefixBytes.length; i++) {
      if (underlying[i] != prefixBytes[i]) {
        return false;
      }
    }
    return true;
  }
}
