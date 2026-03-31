package com.linkedin.venice.spark.datawriter.task;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.SparkContext;


/**
 * Central registry for per-stage diagnostic metrics in the VPJ Spark pipeline.
 * Maintains insertion order so the report reflects the pipeline execution sequence.
 *
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
   * @return a Spark-agnostic snapshot with per-stage summaries and a formatted report
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
    return new StageMetricsSnapshot(summaries, generateReport());
  }

  /**
   * Generate a formatted diagnostic report showing per-stage I/O counts, byte sizes, and timing.
   *
   * <p>Example output:
   * <pre>
   * Stage                  Records In   Bytes In   Records Out  Bytes Out     Time
   * ─────────────────────  ──────────   ────────   ───────────  ─────────  ────────
   * ttl_filter             1,234,567    456.0 MB   1,222,222    451.0 MB      5.1s
   * compaction             1,222,222    451.0 MB   1,100,000    410.0 MB         —
   * </pre>
   */
  public String generateReport() {
    if (stages.isEmpty()) {
      return "No stages registered.";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format(
            Locale.US,
            "%-25s %12s %10s %12s %10s %8s%n",
            "Stage",
            "Records In",
            "Bytes In",
            "Records Out",
            "Bytes Out",
            "Time"));
    sb.append(
        String.format(
            Locale.US,
            "%-25s %12s %10s %12s %10s %8s%n",
            "─────────────────────────",
            "──────────",
            "────────",
            "───────────",
            "─────────",
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
      String timeStr = m.timeNs.value() > 0 ? String.format(Locale.US, "%.1fs", timeSec) : "—";

      sb.append(
          String.format(
              Locale.US,
              "%-25s %12s %10s %12s %10s %8s%n",
              entry.getKey(),
              recInStr,
              bytesInStr,
              recOutStr,
              bytesOutStr,
              timeStr));
    }

    return sb.toString();
  }

  static String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    } else if (bytes < 1024 * 1024) {
      return String.format(Locale.US, "%.1f KB", bytes / 1024.0);
    } else if (bytes < 1024L * 1024 * 1024) {
      return String.format(Locale.US, "%.1f MB", bytes / (1024.0 * 1024));
    } else {
      return String.format(Locale.US, "%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
  }

  static String formatNumber(long n) {
    return String.format(Locale.US, "%,d", n);
  }
}
