package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.response.stats.SingleGetResponseStats;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.protocols.ValueCount;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Map;


public class CountByValueResponseWrapper implements ReadResponse {
  private final CountByValueResponse.Builder responseBuilder;
  private final SingleGetResponseStats stats;
  private byte[] responseBytes;
  private int rcu = 0;

  public CountByValueResponseWrapper() {
    this.responseBuilder = CountByValueResponse.newBuilder();
    this.stats = new SingleGetResponseStats();
  }

  public void setFieldToValueCounts(Map<String, Map<String, Integer>> fieldToValueCounts) {
    for (Map.Entry<String, Map<String, Integer>> fieldEntry: fieldToValueCounts.entrySet()) {
      String fieldName = fieldEntry.getKey();
      Map<String, Integer> valueCounts = fieldEntry.getValue();

      ValueCount.Builder valueCountBuilder = ValueCount.newBuilder();
      for (Map.Entry<String, Integer> valueEntry: valueCounts.entrySet()) {
        valueCountBuilder.putValueToCounts(valueEntry.getKey(), valueEntry.getValue());
      }

      responseBuilder.putFieldToValueCounts(fieldName, valueCountBuilder.build());
    }
  }

  public void setError(int errorCode, String errorMessage) {
    responseBuilder.setErrorCode(errorCode);
    responseBuilder.setErrorMessage(errorMessage);
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return -1; // CountByValue uses protobuf, not Avro
  }

  @Override
  public ByteBuf getResponseBody() {
    if (responseBytes == null) {
      responseBuilder.setErrorCode(VeniceReadResponseStatus.OK);
      CountByValueResponse response = responseBuilder.build();
      responseBytes = response.toByteArray();
    }
    return Unpooled.wrappedBuffer(responseBytes);
  }

  @Override
  public ReadResponseStats getStats() {
    return stats;
  }

  @Override
  public CompressionStrategy getCompressionStrategy() {
    return CompressionStrategy.NO_OP; // CountByValue responses are typically small
  }

  @Override
  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    // CountByValue responses don't need compression
  }

  @Override
  public boolean isStreamingResponse() {
    return false; // CountByValue is not streaming
  }

  @Override
  public void setStreamingResponse() {
    // CountByValue doesn't support streaming
  }

  @Override
  public boolean isFound() {
    return true; // CountByValue always returns results (even if empty)
  }

  @Override
  public void setRCU(int rcu) {
    this.rcu = rcu;
  }

  @Override
  public int getRCU() {
    return rcu;
  }
}
