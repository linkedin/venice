package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;


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
  public void recordMetrics(
      ServerHttpRequestStats stats,
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    super.recordMetrics(stats, statusEnum, statusCategory, veniceCategory);

    if (this.valueSize > 0) {
      stats.recordValueSizeInByte(statusEnum, statusCategory, veniceCategory, this.valueSize);
    }
    stats.recordKeySizeInByte(this.keySize);
  }

  @Override
  public int getResponseValueSize() {
    return this.valueSize;
  }
}
