package com.linkedin.venice.listener;

import com.linkedin.venice.store.record.ValueRecord;

/**
 * Created by mwise on 3/22/16.
 */
public class StorageResponseObject {
  // The latest offset the storage node has seen for the requested partition
  private final long offset;
  // Value record storing both schema id and the real data
  private final ValueRecord valueRecord;

  public StorageResponseObject(byte[] value, long offset){
    this.valueRecord = ValueRecord.parseAndCreate(value);
    this.offset = offset;
  }

  public ValueRecord getValueRecord() {
    return valueRecord;
  }

  public long getOffset() {
    return offset;
  }
}
