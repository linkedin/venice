package com.linkedin.venice.spark.datawriter.task;

import java.util.LinkedHashMap;


/**
 * Immutable snapshot of stage metrics captured after a VPJ Spark job completes.
 * This is a Spark-agnostic representation — no Spark types are exposed.
 *
 * <p>Use {@link StageMetricsRegistry#snapshot()} to create an instance after
 * {@code dataFrame.count()} triggers execution.
 */
public class StageMetricsSnapshot {
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

  private final LinkedHashMap<String, StageSummary> stages;
  private final String formattedReport;

  StageMetricsSnapshot(LinkedHashMap<String, StageSummary> stages, String formattedReport) {
    this.stages = stages;
    this.formattedReport = formattedReport;
  }

  /**
   * Get the summary for a specific stage, or null if not registered.
   */
  public StageSummary getStage(String stageName) {
    return stages.get(stageName);
  }

  /**
   * Get the pre-formatted diagnostic report string.
   */
  public String getFormattedReport() {
    return formattedReport;
  }

  @Override
  public String toString() {
    return formattedReport;
  }
}
