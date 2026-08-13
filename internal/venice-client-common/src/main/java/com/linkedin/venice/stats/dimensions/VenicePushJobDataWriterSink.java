package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_DATA_WRITER_SINK;


/**
 * Dimension enum identifying which sink a push job's data-writer tasks were writing to when the reported
 * duration accrued. It lets a single duration metric carry both legs of a dual write instead of needing one
 * metric per leg.
 *
 * Maps to {@link VeniceMetricsDimensions#VENICE_PUSH_JOB_DATA_WRITER_SINK}.
 */
public enum VenicePushJobDataWriterSink implements VeniceDimensionInterface {
  /** Time spent invoking the Venice/Kafka writes and flushing/closing the Venice writer. */
  VENICE,

  /**
   * Time spent in the external storage write path: throttling wait, batchPut calls including
   * retries and retry backoff, external flush and external close.
   */
  EXTERNAL_STORAGE;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_PUSH_JOB_DATA_WRITER_SINK;
  }
}
