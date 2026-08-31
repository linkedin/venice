package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;


public class MultiKeyResponseStats extends AbstractReadResponseStats {
  private final IntList keySizes;
  private final IntList valueSizes;
  private int recordCount = -1;

  public MultiKeyResponseStats(int maxKeyCount) {
    this.keySizes = new IntArrayList(maxKeyCount);
    this.valueSizes = new IntArrayList(maxKeyCount);
  }

  @Override
  public void addKeySize(int size) {
    this.keySizes.add(size);
  }

  @Override
  public void addValueSize(int size) {
    this.valueSizes.add(size);
  }

  public void setRecordCount(int count) {
    this.recordCount = count;
  }

  @Override
  protected int getRecordCount() {
    return this.recordCount;
  }

  /**
   * N.B.: The per-key/value sizes are treated as non-mergeable; each chunk records its own sizes via
   * {@link #recordUnmergedMetrics}. Only the aggregate recordCount is merged.
   */
  @Override
  public void recordUnmergedMetrics(
      ServerHttpRequestStats stats,
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    super.recordUnmergedMetrics(stats, statusEnum, statusCategory, veniceCategory);
    ResponseStatsUtil
        .recordKeyValueSizes(stats, this.keySizes, this.valueSizes, statusEnum, statusCategory, veniceCategory);
  }

  @Override
  public void merge(ReadResponseStatsRecorder other) {
    super.merge(other);
    // Merges only the mergeable field this subclass introduces: recordCount.
    // ParallelMultiKeyResponseWrapper creates all chunks with the same type,
    // so 'other' will always be a MultiKeyResponseStats here.
    if (other instanceof MultiKeyResponseStats) {
      MultiKeyResponseStats otherStats = (MultiKeyResponseStats) other;
      this.recordCount += otherStats.recordCount;
    } else {
      throw new IllegalArgumentException(
          "Expected MultiKeyResponseStats but got " + other.getClass().getSimpleName() + "; recordCount not merged");
    }
  }
}
