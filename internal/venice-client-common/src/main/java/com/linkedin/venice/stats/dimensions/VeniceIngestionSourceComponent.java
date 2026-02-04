package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;


/**
 * Dimension enum representing the source component in the ingestion flow.
 * Used for metrics like ingestion latency between components.
 *
 * <p>Note: Similar to {@link VeniceIngestionDestinationComponent}. These are
 * 2 different enums to provide type safety when defining and recording metrics.
 */
public enum VeniceIngestionSourceComponent implements VeniceDimensionInterface {
  PRODUCER, LOCAL_BROKER, SOURCE_BROKER, LEADER_CONSUMER, FOLLOWER_CONSUMER;

  private final String component;

  VeniceIngestionSourceComponent() {
    this.component = name().toLowerCase();
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_INGESTION_SOURCE_COMPONENT;
  }

  @Override
  public String getDimensionValue() {
    return component;
  }
}
