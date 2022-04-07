package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;


public abstract class MultiKeyResponseWrapper<K> extends ReadResponse {
  protected final List<K> records;

  public MultiKeyResponseWrapper(int maxKeyCount) {
    this.records = new ArrayList<>(maxKeyCount);
  }

  public void addRecord(K record) {
    records.add(record);
  }

  protected abstract byte[] serializedResponse();

  public abstract int getResponseSchemaIdHeader();

  public int getRecordCount() {
    return records.size();
  }

  @Override
  public ByteBuf getResponseBody() {
    return Unpooled.wrappedBuffer(serializedResponse());
  }

  /**
   * This function needs to be synchronized because during batch gets, there could be several
   * threads incrementing this all at once.
   */
  @Override
  public void incrementMultiChunkLargeValueCount() {
    super.incrementMultiChunkLargeValueCount();
  }
}
