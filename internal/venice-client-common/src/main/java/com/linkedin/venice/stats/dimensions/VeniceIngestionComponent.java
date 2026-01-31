package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;


/**
 * Dimension to represent components in the ingestion flow.
 * Used for both source and destination component dimensions.
 */
public enum VeniceIngestionComponent implements VeniceDimensionInterface {
  PRODUCER, LOCAL_BROKER, SOURCE_BROKER, LEADER_CONSUMER, FOLLOWER_CONSUMER;

  private final String component;

  VeniceIngestionComponent() {
    this.component = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Note: This enum is used for both source and destination component dimensions.
   * The actual dimension name is determined by how it's used in the metric.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_INGESTION_SOURCE_COMPONENT;
  }

  @Override
  public String getDimensionValue() {
    return component;
  }
}
