package com.linkedin.venice.spark.datawriter.task;

import com.linkedin.venice.utils.HyperLogLogSketch;
import org.apache.spark.util.AccumulatorV2;


/**
 * A Spark {@link AccumulatorV2} that wraps {@link HyperLogLogSketch} for distributed
 * cardinality estimation across Spark tasks.
 *
 * Each task gets a fresh copy via {@link #copy()}, adds keys during processing,
 * and the driver merges all task-level sketches via {@link #merge(AccumulatorV2)}.
 */
public class HyperLogLogAccumulator extends AccumulatorV2<byte[], Long> {
  private static final long serialVersionUID = 1L;

  private HyperLogLogSketch sketch;

  public HyperLogLogAccumulator() {
    this.sketch = new HyperLogLogSketch();
  }

  @Override
  public boolean isZero() {
    return sketch.isEmpty();
  }

  @Override
  public AccumulatorV2<byte[], Long> copy() {
    HyperLogLogAccumulator newAcc = new HyperLogLogAccumulator();
    newAcc.sketch = this.sketch.copy();
    return newAcc;
  }

  @Override
  public void reset() {
    sketch.reset();
  }

  @Override
  public void add(byte[] key) {
    sketch.add(key);
  }

  @Override
  public void merge(AccumulatorV2<byte[], Long> other) {
    sketch.merge(((HyperLogLogAccumulator) other).sketch);
  }

  @Override
  public Long value() {
    return sketch.estimate();
  }
}
