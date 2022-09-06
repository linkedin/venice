package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.store.record.ValueRecord;
import io.netty.buffer.ByteBuf;


public class StorageResponseObject extends ReadResponse {
  // Value record storing both schema id and the real data
  private ValueRecord valueRecord;

  public StorageResponseObject() {
  }

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
}
