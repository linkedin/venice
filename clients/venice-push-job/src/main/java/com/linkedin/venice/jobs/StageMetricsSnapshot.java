package com.linkedin.venice.jobs;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;


/**
 * Immutable, engine-agnostic snapshot of stage metrics captured after a VPJ job completes.
 * Use {@link com.linkedin.venice.spark.datawriter.task.StageMetricsRegistry#snapshot()} to create
 * an instance after {@code dataFrame.count()} triggers execution.
 */
public class StageMetricsSnapshot {
  private final LinkedHashMap<String, StageSummary> stages;
  private String formattedReport;

  public StageMetricsSnapshot(LinkedHashMap<String, StageSummary> stages) {
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
   *
   * <p><b>Task Time semantics:</b> For mapPartitions stages (ttl_filter, compression_reencode, kafka_write),
   * this is wall-clock time per partition summed across partitions. For flatMapGroups stages (compaction,
   * chunk_assembly), this is per-group elapsed time summed across all groups. Both represent total task
   * time, not wall-clock duration.
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
