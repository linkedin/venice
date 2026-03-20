package com.linkedin.venice.spark.datawriter.task;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.util.AccumulatorV2;


/**
 * A Spark accumulator that maintains per-key long counters, merging by summing values per key.
 * Used for tracking per-partition record counts during Spark-based push jobs.
 */
public class MapLongAccumulator extends AccumulatorV2<scala.Tuple2<Integer, Long>, Map<Integer, Long>> {
  private static final long serialVersionUID = 1L;

  private final ConcurrentHashMap<Integer, Long> map = new ConcurrentHashMap<>();

  @Override
  public boolean isZero() {
    return map.isEmpty();
  }

  @Override
  public AccumulatorV2<scala.Tuple2<Integer, Long>, Map<Integer, Long>> copy() {
    MapLongAccumulator newAcc = new MapLongAccumulator();
    newAcc.map.putAll(this.map);
    return newAcc;
  }

  @Override
  public void reset() {
    map.clear();
  }

  @Override
  public void add(scala.Tuple2<Integer, Long> v) {
    map.merge(v._1(), v._2(), Long::sum);
  }

  @Override
  public void merge(AccumulatorV2<scala.Tuple2<Integer, Long>, Map<Integer, Long>> other) {
    MapLongAccumulator otherAcc = (MapLongAccumulator) other;
    otherAcc.map.forEach((key, value) -> map.merge(key, value, Long::sum));
  }

  @Override
  public Map<Integer, Long> value() {
    return Collections.unmodifiableMap(map);
  }
}
