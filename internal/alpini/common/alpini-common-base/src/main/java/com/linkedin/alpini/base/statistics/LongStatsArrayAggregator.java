package com.linkedin.alpini.base.statistics;

import com.linkedin.alpini.base.concurrency.ArraySortedSet;
import java.util.Comparator;
import java.util.SortedSet;


/**
 * Extend the {@link LongQuantileEstimation} class and add computation for the mean and standard deviation of
 * the supplied values. The statistics are summarised in a {@link LongStats} object and the statistics are reset.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class LongStatsArrayAggregator extends LongStatsAggregator {
  public LongStatsArrayAggregator(double epsilon, int compactSize) {
    super(epsilon, compactSize);
  }

  @Override
  protected SortedSet<LongQuantileArrayEstimation.Sample> newSortedSet(
      Comparator<LongQuantileArrayEstimation.Sample> comparator) {
    return new ArraySortedSet<>(comparator, 2 * getCompactSize());
  }

  @Override
  protected SortedSet<LongQuantileArrayEstimation.Sample> cloneSortedSet(
      SortedSet<LongQuantileArrayEstimation.Sample> samples) {
    return ((ArraySortedSet<LongQuantileArrayEstimation.Sample>) samples).clone();
  }

  @Override
  protected LongQuantileArrayEstimation.Sample floor(
      SortedSet<LongQuantileArrayEstimation.Sample> samples,
      LongQuantileArrayEstimation.Sample v) {
    return ((ArraySortedSet<LongQuantileArrayEstimation.Sample>) samples).floor(v);
  }

}
