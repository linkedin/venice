package com.linkedin.venice.spark.datawriter.task;

import java.util.LinkedHashMap;
import java.util.Locale;
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
 *   StageMetricsRegistry.Snapshot snapshot = registry.snapshot();
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
   * @return a Spark-agnostic snapshot with per-stage summaries and a formatted report
   */
  public Snapshot snapshot() {
    LinkedHashMap<String, Snapshot.StageSummary> summaries = new LinkedHashMap<>();
    for (Map.Entry<String, StageMetrics> entry: stages.entrySet()) {
      StageMetrics m = entry.getValue();
      summaries.put(
          entry.getKey(),
          new Snapshot.StageSummary(
              m.recordsIn.value(),
              m.recordsOut.value(),
              m.bytesIn.value(),
              m.bytesOut.value(),
              m.timeNs.value()));
    }
    return new Snapshot(summaries);
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

  /**
   * Immutable, Spark-agnostic snapshot of stage metrics captured after a VPJ Spark job completes.
   * Use {@link StageMetricsRegistry#snapshot()} to create an instance after
   * {@code dataFrame.count()} triggers execution.
   */
  public static class Snapshot {
    private final LinkedHashMap<String, StageSummary> stages;
    private String formattedReport;

    Snapshot(LinkedHashMap<String, StageSummary> stages) {
      this.stages = stages;
    }

    /**
     * Get the summary for a specific stage, or null if not registered.
     */
    public StageSummary getStage(String stageName) {
      return stages.get(stageName);
    }

    /**
     * Get the formatted diagnostic report. Generated lazily on first call.
     *
     * <p>Example output:
     * <pre>
     * Stage                  Records In   Bytes In   Records Out  Bytes Out  Task Time
     * ─────────────────────  ──────────   ────────   ───────────  ─────────  ────────
     * ttl_filter             1,234,567    456.0 MB   1,222,222    451.0 MB      5.1s
     * compaction             1,222,222    451.0 MB   1,100,000    410.0 MB         —
     * </pre>
     */
    public String getFormattedReport() {
      if (formattedReport == null) {
        formattedReport = generateReport();
      }
      return formattedReport;
    }

    @Override
    public String toString() {
      return getFormattedReport();
    }

    private String generateReport() {
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
              "Task Time"));
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

      for (Map.Entry<String, StageSummary> entry: stages.entrySet()) {
        StageSummary m = entry.getValue();
        double timeSec = m.timeNs / 1_000_000_000.0;

        String recInStr = m.recordsIn > 0 ? formatNumber(m.recordsIn) : "—";
        String bytesInStr = m.recordsIn > 0 ? formatBytes(m.bytesIn) : "—";
        String recOutStr = formatNumber(m.recordsOut);
        String bytesOutStr = formatBytes(m.bytesOut);
        String timeStr = m.timeNs > 0 ? String.format(Locale.US, "%.1fs", timeSec) : "—";

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

    /**
     * Immutable metrics for a single pipeline stage.
     */
    public static class StageSummary {
      private final long recordsIn;
      private final long recordsOut;
      private final long bytesIn;
      private final long bytesOut;
      private final long timeNs;

      public StageSummary(long recordsIn, long recordsOut, long bytesIn, long bytesOut, long timeNs) {
        this.recordsIn = recordsIn;
        this.recordsOut = recordsOut;
        this.bytesIn = bytesIn;
        this.bytesOut = bytesOut;
        this.timeNs = timeNs;
      }

      public long getRecordsIn() {
        return recordsIn;
      }

      public long getRecordsOut() {
        return recordsOut;
      }

      public long getBytesIn() {
        return bytesIn;
      }

      public long getBytesOut() {
        return bytesOut;
      }

      public long getTimeNs() {
        return timeNs;
      }
    }
  }
}
