package com.linkedin.venice.spark.datawriter.task;

import java.io.Serializable;
import org.apache.spark.SparkContext;
import org.apache.spark.util.LongAccumulator;


/**
 * Holds per-stage diagnostic accumulators for tracking record counts, byte sizes, and timing
 * through the VPJ Spark pipeline. One instance per pipeline stage (e.g., ttl_filter, compaction,
 * compression_reencode, kafka_write).
 *
 * <p>Accumulators are registered with the SparkContext and aggregated on the driver after
 * {@code dataFrame.count()} triggers execution.
 *
 * <h3>Accuracy under speculative execution</h3>
 * Spark does not guarantee exactly-once accumulator semantics when speculative execution is
 * enabled ({@code spark.speculation=true}). Under speculation, two task attempts for the same
 * partition may both complete successfully. Spark discards the output of the slower attempt but
 * <em>does not roll back</em> its accumulator deltas, so both attempts' contributions are applied
 * to the driver-side value. This means all counters ({@code recordsIn}, {@code recordsOut},
 * {@code bytesIn}, {@code bytesOut}, {@code timeNs}) may be over-counted for any partition that
 * was speculated. The degree of inflation is bounded by the number of speculated partitions.
 *
 * <p>Failed task retries do <em>not</em> cause double-counting: the driver discards accumulator
 * updates from failed attempts, so only the successful retry's delta is applied.
 *
 * <p>These accumulators are intended for diagnostic purposes only (pipeline stage I/O ratios,
 * throughput estimates, timing). They should not be used for correctness checks or billing.
 */
public class StageMetrics implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String stageName;
  public final LongAccumulator recordsIn;
  public final LongAccumulator recordsOut;
  public final LongAccumulator bytesIn;
  public final LongAccumulator bytesOut;
  public final LongAccumulator timeNs;

  public StageMetrics(SparkContext sparkContext, String stageName) {
    this.stageName = stageName;
    this.recordsIn = sparkContext.longAccumulator(stageName + ".records_in");
    this.recordsOut = sparkContext.longAccumulator(stageName + ".records_out");
    this.bytesIn = sparkContext.longAccumulator(stageName + ".bytes_in");
    this.bytesOut = sparkContext.longAccumulator(stageName + ".bytes_out");
    this.timeNs = sparkContext.longAccumulator(stageName + ".time_ns");
  }

  public String getStageName() {
    return stageName;
  }
}
