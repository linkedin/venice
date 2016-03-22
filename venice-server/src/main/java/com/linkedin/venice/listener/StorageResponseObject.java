package com.linkedin.venice.listener;

/**
 * Created by mwise on 3/22/16.
 */
public class StorageResponseObject {
  private final byte[] value;
  // The latest offset the storage node has seen for the requested partition
  private final long offset;

  public StorageResponseObject(byte[] value, long offset){
    this.value = value;
    this.offset = offset;
  }

  public byte[] getValue() {
    return value;
  }

  public long getOffset() {
    return offset;
  }
}
