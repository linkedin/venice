package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;


public class SingleGetResponseStats extends AbstractReadResponseStats {
  private int keySize = 0;
  private int valueSize = 0;

  @Override
  public void addKeySize(int size) {
    this.keySize += size;
  }

  @Override
  public void addValueSize(int size) {
    // N.B.: In the case of single get, this should only called once, so it is effectively a setter.
    this.valueSize = size;
  }

  @Override
  protected int getRecordCount() {
    return this.valueSize > 0 ? 1 : 0;
  }

  @Override
  public void recordMetrics(ServerHttpRequestStats stats) {
    super.recordMetrics(stats);

    ResponseStatsUtil.consumeIntIfAbove(stats::recordValueSizeInByte, this.valueSize, 0);
    stats.recordKeySizeInByte(this.keySize);
  }
}
