package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;


public class AggregationReadResponseWrapper extends AbstractReadResponse {
  private final String result;

  public AggregationReadResponseWrapper(String result) {
    this.result = result;
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
    return null;
  }

  @Override
  public ReadResponseStats getStats() {
    return null;
  }
}
