package com.linkedin.venice.router.api;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.EncodingUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


/**
 * {@code RouterKey} encapsulates the required information for a key in a router request.
 */
public class RouterKey implements Comparable<RouterKey> {
  private static final int UNKNOWN_PARTITION_ID = -1;

  private final ByteBuffer keyBuffer;
  /**
   * Initializing hashCode during construction will speed up the following {@link #hashCode()} invocations.
   */
  private final int hashCode;
  /**
   * Maintaining the partition id here is to avoid multiple lookups in
   * {@link com.linkedin.venice.router.api.path.VeniceMultiGetPath},
   * {@link VeniceDelegateMode}
   */
  private int partitionId = UNKNOWN_PARTITION_ID;

  private int keySize;

  public RouterKey(byte[] key) {
    this(ByteBuffer.wrap(key));
  }

  public RouterKey(ByteBuffer key) {
    this.keyBuffer = key;
    this.hashCode = this.keyBuffer.hashCode();
    this.keySize = key.remaining();
  }

  public static RouterKey fromString(String s) {
    return new RouterKey(s.getBytes(StandardCharsets.UTF_8));
  }

  public static RouterKey fromBase64(String s) {
    return new RouterKey(EncodingUtils.base64DecodeFromString(s));
  }

  public String base64Encoded() {
    return EncodingUtils.base64EncodeToString(keyBuffer);
  }

  public ByteBuffer getKeyBuffer() {
    return keyBuffer;
  }

  public int getKeySize() {
    return keySize;
  }

  @Override
  public int compareTo(RouterKey other) {
    return keyBuffer.compareTo(other.keyBuffer);
  }

  @Override
  public String toString() {
    return EncodingUtils.base64EncodeToString(keyBuffer);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RouterKey other = (RouterKey) o;
    return this.keyBuffer.equals(other.keyBuffer);
  }

  public void setPartitionId(int partitionId) {
    if (UNKNOWN_PARTITION_ID != this.partitionId) {
      throw new VeniceException("Partition id has been assigned: " + this.partitionId + ", and it is immutable after");
    }
    this.partitionId = partitionId;
  }

  public int getPartitionId() {
    if (UNKNOWN_PARTITION_ID == partitionId) {
      throw new VeniceException("Partition id hasn't been setup yet");
    }
    return this.partitionId;
  }
}
