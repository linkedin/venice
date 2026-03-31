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
