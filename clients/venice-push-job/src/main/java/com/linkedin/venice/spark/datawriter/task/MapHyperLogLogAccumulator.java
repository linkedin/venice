package com.linkedin.venice.spark.datawriter.task;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;


/**
 * A Spark accumulator that maintains per-partition HyperLogLog sketches for estimating
 * the unique key cardinality within each partition independently.
 *
 * Used during repush when source and destination partition counts match, enabling
 * per-partition verification that catches partition-level record loss which aggregate
 * HLL comparison would mask.
 *
 * Each partition gets its own HLL sketch (16KB). For 1000 partitions, total memory is ~16MB per task.
 */
public class MapHyperLogLogAccumulator extends AccumulatorV2<Tuple2<Integer, byte[]>, Map<Integer, Long>> {
  private static final long serialVersionUID = 1L;

  private final ConcurrentHashMap<Integer, HyperLogLogAccumulator> hllMap = new ConcurrentHashMap<>();

  @Override
  public boolean isZero() {
    return hllMap.isEmpty();
  }

  @Override
  public AccumulatorV2<Tuple2<Integer, byte[]>, Map<Integer, Long>> copy() {
    MapHyperLogLogAccumulator newAcc = new MapHyperLogLogAccumulator();
    hllMap.forEach((partition, hll) -> {
      newAcc.hllMap.put(partition, (HyperLogLogAccumulator) hll.copy());
    });
    return newAcc;
  }

  @Override
  public void reset() {
    hllMap.clear();
  }

  @Override
  public void add(Tuple2<Integer, byte[]> v) {
    hllMap.computeIfAbsent(v._1(), k -> new HyperLogLogAccumulator()).add(v._2());
  }

  @Override
  public void merge(AccumulatorV2<Tuple2<Integer, byte[]>, Map<Integer, Long>> other) {
    MapHyperLogLogAccumulator otherAcc = (MapHyperLogLogAccumulator) other;
    otherAcc.hllMap.forEach((partition, otherHll) -> {
      hllMap.merge(partition, otherHll, (existing, incoming) -> {
        existing.merge(incoming);
        return existing;
      });
    });
  }

  @Override
  public Map<Integer, Long> value() {
    ConcurrentHashMap<Integer, Long> result = new ConcurrentHashMap<>();
    hllMap.forEach((partition, hll) -> result.put(partition, hll.value()));
    return Collections.unmodifiableMap(result);
  }
}
