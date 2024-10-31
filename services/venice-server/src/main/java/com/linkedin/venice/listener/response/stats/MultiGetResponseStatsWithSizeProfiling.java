package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;


public class MultiGetResponseStatsWithSizeProfiling extends MultiKeyResponseStats {
  private final IntList keySizes;
  private final IntList valueSizes;

  public MultiGetResponseStatsWithSizeProfiling(int maxKeyCount) {
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

  /**
   * N.B.: We prefer treating the K/V sizes as non-mergeable, even though we could technically merge these lists into a
   * bigger list, because doing so would trigger list resizes and copying, which is less efficient. Furthermore, there
   * is no benefit from the merging since we still need to do one record call per item.
   *
   * @param stats the {@link ServerHttpRequestStats} object to record stats into.
   */
  @Override
  public void recordUnmergedMetrics(ServerHttpRequestStats stats) {
    super.recordUnmergedMetrics(stats);
    ResponseStatsUtil.recordKeyValueSizes(stats, this.keySizes, this.valueSizes);
  }
}
