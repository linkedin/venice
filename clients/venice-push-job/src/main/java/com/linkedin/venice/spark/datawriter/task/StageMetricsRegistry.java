package com.linkedin.venice.spark.datawriter.task;

import com.linkedin.venice.jobs.StageMetricsSnapshot;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.SparkContext;


/**
 * Central registry for per-stage diagnostic metrics in the VPJ Spark pipeline.
 * Maintains insertion order so the report reflects the pipeline execution sequence.
 *
 * <p>Usage:
 * <pre>
 *   StageMetricsRegistry registry = new StageMetricsRegistry(sparkContext);
 *   StageMetrics ttlMetrics = registry.register("ttl_filter");
 *   // ... instrument stage with ttlMetrics.recordsIn, ttlMetrics.recordsOut, etc.
 *   // After dataFrame.count():
 *   StageMetricsSnapshot snapshot = registry.snapshot();
 *   LOGGER.info("VPJ Pipeline Diagnostics:\n{}", snapshot.getFormattedReport());
 * </pre>
 */
public class StageMetricsRegistry {
  private final LinkedHashMap<String, StageMetrics> stages = new LinkedHashMap<>();
  private final SparkContext sparkContext;

  public StageMetricsRegistry(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * Register a new pipeline stage for tracking. Stages are reported in registration order.
   *
   * @param stageName unique identifier for the stage (e.g., "ttl_filter", "compaction")
   * @return the StageMetrics instance for this stage (returns existing instance if already registered)
   */
  public StageMetrics register(String stageName) {
    return stages.computeIfAbsent(stageName, name -> new StageMetrics(sparkContext, name));
  }

  /**
   * Get a previously registered stage's metrics.
   *
   * @return the StageMetrics, or null if not registered
   */
  public StageMetrics getStage(String stageName) {
    return stages.get(stageName);
  }

  /**
   * Capture an immutable snapshot of all stage metrics. Call this after {@code dataFrame.count()}
   * triggers execution so accumulator values are finalized.
   *
   * @return an engine-agnostic snapshot with per-stage summaries and a formatted report
   */
  public StageMetricsSnapshot snapshot() {
    LinkedHashMap<String, StageMetricsSnapshot.StageSummary> summaries = new LinkedHashMap<>();
    for (Map.Entry<String, StageMetrics> entry: stages.entrySet()) {
      StageMetrics m = entry.getValue();
      summaries.put(
          entry.getKey(),
          new StageMetricsSnapshot.StageSummary(
              m.recordsIn.value(),
              m.recordsOut.value(),
              m.bytesIn.value(),
              m.bytesOut.value(),
              m.timeNs.value()));
    }
    return new StageMetricsSnapshot(summaries);
  }
}
