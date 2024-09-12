package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import com.linkedin.venice.listener.response.stats.SingleGetResponseStats;
import io.netty.buffer.ByteBuf;


public class SingleGetResponseWrapper extends AbstractReadResponse {
  // Value record storing both schema id and the real data
  private ValueRecord valueRecord;
  private final SingleGetResponseStats responseStats = new SingleGetResponseStats();

  public SingleGetResponseWrapper() {
  }

  public void setValueRecord(ValueRecord valueRecord) {
    this.valueRecord = valueRecord;
  }

  public ValueRecord getValueRecord() {
    return valueRecord;
  }

  @Override
  public ReadResponseStats getStats() {
    return this.responseStats;
  }

  @Override
  public ReadResponseStatsRecorder getStatsRecorder() {
    return this.responseStats;
  }

  @Override
  public boolean isFound() {
    return this.valueRecord != null;
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
