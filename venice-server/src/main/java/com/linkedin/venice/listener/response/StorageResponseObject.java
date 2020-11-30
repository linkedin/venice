package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.store.record.ValueRecord;
import io.netty.buffer.ByteBuf;


public class StorageResponseObject extends ReadResponse {
  // The latest offset the storage node has seen for the requested partition
  private long offset;

  // Value record storing both schema id and the real data
  private ValueRecord valueRecord;

  public StorageResponseObject() {}

  public void setValueRecord(ValueRecord valueRecord) {
    this.valueRecord = valueRecord;
  }

  public ValueRecord getValueRecord() {
    return valueRecord;
  }

  @Override
  public boolean isFound() {
    return this.valueRecord != null;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getOffset() {
    return offset;
  }

  public int getRecordCount() {
    if (isFound()) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public ByteBuf getResponseBody() {
    return getValueRecord().getData();
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return getValueRecord().getSchemaId();
  }

  @Override
  public String getResponseOffsetHeader() {
    return Long.toString(offset);
  }
}
