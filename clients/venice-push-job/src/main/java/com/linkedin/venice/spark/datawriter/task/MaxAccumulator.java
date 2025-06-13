package com.linkedin.venice.spark.datawriter.task;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorV2;


public class MaxAccumulator<T extends Comparable<T>> extends AccumulatorV2<T, T> {
  private final AtomicReference<T> maxValue = new AtomicReference<>(null);

  MaxAccumulator(T initialValue) {
    maxValue.set(initialValue);
  }

  MaxAccumulator(T initialValue, SparkContext sparkContext, String accumulatorName) {
    this(initialValue);

    sparkContext.register(this, accumulatorName);
  }

  @Override
  public boolean isZero() {
    return maxValue.get() == null;
  }

  @Override
  public AccumulatorV2<T, T> copy() {
    return new MaxAccumulator<>(maxValue.get());
  }

  @Override
  public void reset() {
    maxValue.set(null);
  }

  @Override
  public void add(T v) {
    if (v == null) {
      return;
    }

    maxValue.updateAndGet(currentMax -> {
      if (currentMax == null || v.compareTo(currentMax) > 0) {
        return v;
      }
      return currentMax;
    });
  }

  @Override
  public void merge(AccumulatorV2<T, T> other) {
    add(other.value());
  }

  @Override
  public T value() {
    return maxValue.get();
  }
}
