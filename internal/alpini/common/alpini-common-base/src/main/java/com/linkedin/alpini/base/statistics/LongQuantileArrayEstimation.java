package com.linkedin.alpini.base.statistics;

import com.linkedin.alpini.base.concurrency.ArraySortedSet;
import java.util.Comparator;
import java.util.SortedSet;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class LongQuantileArrayEstimation extends LongQuantileEstimation {
  public LongQuantileArrayEstimation(double epsilon, int compactSize) {
    super(epsilon, compactSize);
  }

  @Override
  protected SortedSet<Sample> newSortedSet(Comparator<Sample> comparator) {
    return new ArraySortedSet<>(comparator, 2 * getCompactSize());
  }

  @Override
  protected SortedSet<Sample> cloneSortedSet(SortedSet<Sample> samples) {
    return ((ArraySortedSet<Sample>) samples).clone();
  }

  @Override
  protected Sample floor(SortedSet<Sample> samples, Sample v) {
    return ((ArraySortedSet<Sample>) samples).floor(v);
  }
}
