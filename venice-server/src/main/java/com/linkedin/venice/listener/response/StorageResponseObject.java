package com.linkedin.venice.listener.response;

import com.linkedin.venice.store.record.ValueRecord;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;

import static io.netty.handler.codec.http.HttpResponseStatus.*;


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
