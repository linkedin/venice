package com.linkedin.venice.listener.response;

import com.linkedin.venice.store.record.ValueRecord;

public class StorageResponseObject extends ReadResponse {
  // The latest offset the storage node has seen for the requested partition
  private final long offset;
  // Value record storing both schema id and the real data
  private final ValueRecord valueRecord;

  public StorageResponseObject(byte[] value, long offset){
    this.valueRecord = ValueRecord.parseAndCreate(value);
    this.offset = offset;
  }

  public StorageResponseObject(long offset) {
    this.offset = offset;
    this.valueRecord = null;
  }

  public ValueRecord getValueRecord() {
    return valueRecord;
  }

  public boolean isFound() {
    return this.valueRecord != null;
  }

  public long getOffset() {
    return offset;
  }
}
