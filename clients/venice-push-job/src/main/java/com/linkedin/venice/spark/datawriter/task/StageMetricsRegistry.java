package com.linkedin.venice.spark.datawriter.task;

import java.io.Serializable;
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
 *   LOGGER.info("VPJ Pipeline Diagnostics:\n{}", registry.generateReport());
 * </pre>
 */
public class StageMetricsRegistry implements Serializable {
  private static final long serialVersionUID = 1L;

  private final LinkedHashMap<String, StageMetrics> stages = new LinkedHashMap<>();
  private final transient SparkContext sparkContext;

  public StageMetricsRegistry(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * Register a new pipeline stage for tracking. Stages are reported in registration order.
   *
   * @param stageName unique identifier for the stage (e.g., "ttl_filter", "compaction")
   * @return the StageMetrics instance for this stage
   * @throws IllegalArgumentException if a stage with this name is already registered
   */
  public StageMetrics register(String stageName) {
    if (stages.containsKey(stageName)) {
      throw new IllegalArgumentException("Stage already registered: " + stageName);
    }
    StageMetrics metrics = new StageMetrics(sparkContext, stageName);
    stages.put(stageName, metrics);
    return metrics;
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
   * Generate a formatted diagnostic report showing per-stage I/O counts, byte sizes, ratios, and timing.
   *
   * <p>Example output:
   * <pre>
   * Stage                  Records In   Bytes In   Records Out  Bytes Out  Ratio  Time
   * ─────────────────────  ──────────   ────────   ───────────  ─────────  ─────  ────
   * raw_kafka_input        —            —          1,234,567    456.0 MB   —      12.3s
   * ttl_filter             1,234,567    456.0 MB   1,222,222    451.0 MB   0.99   5.1s
   * </pre>
   */
  public String generateReport() {
    if (stages.isEmpty()) {
      return "No stages registered.";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format(
            "%-25s %12s %10s %12s %10s %6s %8s%n",
            "Stage",
            "Records In",
            "Bytes In",
            "Records Out",
            "Bytes Out",
            "Ratio",
            "Time"));
    sb.append(
        String.format(
            "%-25s %12s %10s %12s %10s %6s %8s%n",
            "─────────────────────────",
            "──────────",
            "────────",
            "───────────",
            "─────────",
            "─────",
            "────────"));

    for (Map.Entry<String, StageMetrics> entry: stages.entrySet()) {
      StageMetrics m = entry.getValue();
      long recIn = m.recordsIn.value();
      long recOut = m.recordsOut.value();
      long bIn = m.bytesIn.value();
      long bOut = m.bytesOut.value();
      double timeSec = m.timeNs.value() / 1_000_000_000.0;

      String recInStr = recIn > 0 ? formatNumber(recIn) : "—";
      String bytesInStr = recIn > 0 ? formatBytes(bIn) : "—";
      String recOutStr = formatNumber(recOut);
      String bytesOutStr = formatBytes(bOut);
      String ratioStr = (recIn > 0 && bIn > 0) ? String.format("%.2f", (double) bOut / bIn) : "—";
      String timeStr = String.format("%.1fs", timeSec);

      sb.append(
          String.format(
              "%-25s %12s %10s %12s %10s %6s %8s%n",
              entry.getKey(),
              recInStr,
              bytesInStr,
              recOutStr,
              bytesOutStr,
              ratioStr,
              timeStr));
    }

    return sb.toString();
  }

  static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.1f KB", bytes / 1024.0);
    } else if (bytes < 1024L * 1024 * 1024) {
      return String.format("%.1f MB", bytes / (1024.0 * 1024));
    } else {
      return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
  }

  static String formatNumber(long n) {
    return String.format("%,d", n);
  }
}
