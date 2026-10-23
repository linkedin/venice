package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_SOURCE;


/**
 * Dimension enum identifying what originated a push job, which {@link VeniceMetricsDimensions#VENICE_PUSH_JOB_TYPE}
 * cannot express: system-initiated repushes reuse data already in Venice but are reported as
 * {@link com.linkedin.venice.meta.Version.PushType#BATCH}, so without this dimension a repush is
 * indistinguishable from a push that brought in fresh data from outside Venice.
 *
 * Maps to {@link VeniceMetricsDimensions#VENICE_PUSH_JOB_SOURCE}.
 */
public enum VenicePushJobSource implements VeniceDimensionInterface {
  /**
   * A push whose data originated outside Venice. This covers ordinary batch and incremental pushes as well as
   * a regular batch push issued against a TTL-repush-enabled store, which despite its push id is not a repush.
   */
  USER,

  /** Repush that re-ingests an existing version topic without applying a TTL filter. */
  REPUSH,

  /** Repush that re-ingests an existing version topic while filtering out records past their TTL. */
  TTL_REPUSH,

  /**
   * Repush sourced from the store's ETL output rather than from a version topic. Unlike the other repush
   * kinds this one carries no push id prefix, so it is identified by the push job's {@code source.etl} config.
   */
  ETL_REPUSH,

  /** System-initiated push performed to satisfy a compliance or privacy deletion. */
  COMPLIANCE;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_PUSH_JOB_SOURCE;
  }
}
