package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the destination component in the ingestion flow.
 * Used for metrics like ingestion latency between components.
 *
 * <p>Note: Similar to {@link VeniceIngestionSourceComponent}. TThese are
 * 2 different enums with same values to provide type safety when defining and
 * recording metrics.
 */
public enum VeniceIngestionDestinationComponent implements VeniceDimensionInterface {
  /** The local Kafka broker in the same region */
  LOCAL_BROKER("local_broker"),
  /** The source Kafka broker from a remote region */
  SOURCE_BROKER("source_broker"),
  /** The leader replica consumer that processes messages first */
  LEADER_CONSUMER("leader_consumer"),
  /** The follower replica consumer that replicates from leader */
  FOLLOWER_CONSUMER("follower_consumer");

  private final String dimensionValue;

  VeniceIngestionDestinationComponent(String dimensionValue) {
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
