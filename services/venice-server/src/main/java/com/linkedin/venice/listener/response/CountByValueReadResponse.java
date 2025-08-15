package com.linkedin.venice.listener.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.response.stats.AbstractReadResponseStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * ReadResponse implementation for CountByValue aggregation results.
 * This class encapsulates the aggregated field value counts.
 */
public class CountByValueReadResponse implements ReadResponse {
  private static final Logger LOGGER = LogManager.getLogger(CountByValueReadResponse.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Map<String, Map<Object, Integer>> fieldToValueCounts;
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
  private final ReadResponseStats stats;
  private boolean isStreamingResponse = false;
  private int rcu = 0;

  public CountByValueReadResponse() {
    this.stats = new CountByValueStats();
  }

  private class CountByValueStats extends AbstractReadResponseStats {
    private int keySize = 0;
    private int valueSize = 0;

    @Override
    public void addKeySize(int size) {
      this.keySize += size;
    }

    @Override
    public void addValueSize(int size) {
      this.valueSize += size;
    }

    @Override
    protected int getRecordCount() {
      return fieldToValueCounts != null ? fieldToValueCounts.size() : 0;
    }

    @Override
    public void recordMetrics(com.linkedin.venice.stats.ServerHttpRequestStats stats) {
      super.recordMetrics(stats);
      stats.recordKeySizeInByte(this.keySize);
      if (this.valueSize > 0) {
        stats.recordValueSizeInByte(this.valueSize);
      }
    }
  }

  public Map<String, Map<Object, Integer>> getFieldToValueCounts() {
    return fieldToValueCounts;
  }

  public void setFieldToValueCounts(Map<String, Map<Object, Integer>> fieldToValueCounts) {
    this.fieldToValueCounts = fieldToValueCounts;
  }

  @Override
  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  @Override
  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  @Override
  public ReadResponseStats getStats() {
    return stats;
  }

  @Override
  public boolean isFound() {
    return fieldToValueCounts != null && !fieldToValueCounts.isEmpty();
  }

  @Override
  public void setStreamingResponse() {
    this.isStreamingResponse = true;
  }

  @Override
  public boolean isStreamingResponse() {
    return isStreamingResponse;
  }

  @Override
  public void setRCU(int rcu) {
    this.rcu = rcu;
  }

  @Override
  public int getRCU() {
    return rcu;
  }

  @Override
  public ByteBuf getResponseBody() {
    // Serialize the fieldToValueCounts map to JSON for transmission
    if (fieldToValueCounts == null || fieldToValueCounts.isEmpty()) {
      return Unpooled.EMPTY_BUFFER;
    }

    try {
      byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(fieldToValueCounts);
      return Unpooled.wrappedBuffer(jsonBytes);
    } catch (Exception e) {
      LOGGER.error("Failed to serialize CountByValue response", e);
      return Unpooled.EMPTY_BUFFER;
    }
  }

  @Override
  public int getResponseSchemaIdHeader() {
    // CountByValue responses may not have a schema ID
    return -1;
  }
}
