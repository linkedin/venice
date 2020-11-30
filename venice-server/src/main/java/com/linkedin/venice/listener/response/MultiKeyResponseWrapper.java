package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.common.PartitionOffsetMapUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class MultiKeyResponseWrapper<K> extends ReadResponse {
  private final Map<Integer, Long> partitionOffsetMap;
  protected final List<K> records;

  public MultiKeyResponseWrapper() {
    this.partitionOffsetMap = new HashMap<>();
    this.records = new ArrayList<>();
  }

  public void addRecord(K record) {
    records.add(record);
  }

  public void addPartitionOffsetMapping(int partition, long offset) {
    partitionOffsetMap.put(partition, offset);
  }

  public String serializedPartitionOffsetMap() throws IOException {
    return PartitionOffsetMapUtils.serializedPartitionOffsetMap(partitionOffsetMap);
  }

  protected abstract byte[] serializedResponse();

  public abstract int getResponseSchemaIdHeader();

  @Override
  public String getResponseOffsetHeader() {
    try {
      return serializedPartitionOffsetMap();
    } catch (IOException e) {
      return "Got IOException during offset header serialization.";
    }
  }

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
