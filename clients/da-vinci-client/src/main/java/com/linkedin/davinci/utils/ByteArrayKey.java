package com.linkedin.davinci.utils;

import java.util.Arrays;


/**
 * A low overhead immutable container of byte[] suitable for use as a map key.
 */
public class ByteArrayKey {
  private final byte[] content;
  private final int hashCode;

  public ByteArrayKey(byte[] content) {
    this.content = content;
    int tmpHashCode = 1;
    for (int i = 0; i < content.length; i++) {
      tmpHashCode = 31 * tmpHashCode + content[i];
    }
    this.hashCode = tmpHashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ByteArrayKey that = (ByteArrayKey) o;
    return Arrays.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  public static ByteArrayKey wrap(byte[] content) {
    return new ByteArrayKey(content);
  }
}
