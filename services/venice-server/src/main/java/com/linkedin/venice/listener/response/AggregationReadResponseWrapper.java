package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;


public class AggregationReadResponseWrapper extends AbstractReadResponse {
  private final String result;
  private final ReadResponseStatsRecorder statsRecorder;

  public AggregationReadResponseWrapper(String result) {
    this.result = result;
    this.statsRecorder = new NoOpReadResponseStatsRecorder();
  }

  @Override
  public ByteBuf getResponseBody() {
    return Unpooled.wrappedBuffer(result.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return -1;
  }

  @Override
  public ReadResponseStatsRecorder getStatsRecorder() {
    return statsRecorder;
  }

  @Override
  public ReadResponseStats getStats() {
    return null;
  }

  private static class NoOpReadResponseStatsRecorder implements ReadResponseStatsRecorder {
    @Override
    public void recordMetrics(com.linkedin.venice.stats.ServerHttpRequestStats stats) {
      // No-op implementation for aggregation responses
    }

    @Override
    public void recordUnmergedMetrics(com.linkedin.venice.stats.ServerHttpRequestStats stats) {
      // No-op implementation for aggregation responses
    }

    @Override
    public void merge(ReadResponseStatsRecorder other) {
      // No-op implementation for aggregation responses
    }
  }
}
