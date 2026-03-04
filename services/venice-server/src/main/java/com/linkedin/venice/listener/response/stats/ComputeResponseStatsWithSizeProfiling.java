package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;


public class ComputeResponseStatsWithSizeProfiling extends ComputeResponseStats {
  private final IntList keySizes;
  private final IntList valueSizes;

  public ComputeResponseStatsWithSizeProfiling(int maxKeyCount) {
    this.keySizes = new IntArrayList(maxKeyCount);
    this.valueSizes = new IntArrayList(maxKeyCount);
  }

  @Override
  public void addKeySize(int size) {
    this.keySizes.add(size);
  }

  @Override
  public void addValueSize(int size) {
    /** N.B.: {@link ComputeResponseStats} does have some logic to execute, so it is necessary to call the super. */
    super.addValueSize(size);
    this.valueSizes.add(size);
  }

  /**
   * N.B.: We prefer treating the K/V sizes as non-mergeable, even though we could technically merge these lists into a
   * bigger list, because doing so would trigger list resizes and copying, which is less efficient. Furthermore, there
   * is no benefit from the merging since we still need to do one record call per item.
   *
   * @param stats the {@link ServerHttpRequestStats} object to record stats into.
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
}
