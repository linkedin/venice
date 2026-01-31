package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the destination component in the ingestion flow.
 * Used for metrics like ingestion latency between components.
 *
 * Note: This is separate from {@link VeniceIngestionComponent} to provide type safety
 * when recording metrics that have both source and destination component dimensions.
 */
public enum VeniceDestinationIngestionComponent implements VeniceDimensionInterface {
  LOCAL_BROKER("local_broker"), SOURCE_BROKER("source_broker"), LEADER_CONSUMER("leader_consumer"),
  FOLLOWER_CONSUMER("follower_consumer");

  private final String dimensionValue;

  VeniceDestinationIngestionComponent(String dimensionValue) {
    this.dimensionValue = dimensionValue;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
  }

  @Override
  public String getDimensionValue() {
    return dimensionValue;
  }
}
