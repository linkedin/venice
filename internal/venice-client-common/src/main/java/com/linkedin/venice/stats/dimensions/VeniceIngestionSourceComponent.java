package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;


/**
 * Dimension enum representing the source component in the ingestion flow.
 * Used for metrics like ingestion latency between components.
 *
 * <p>Note: Paired with {@link VeniceIngestionDestinationComponent}. The two enums have asymmetric
 * value sets because LEADER_CONSUMER and FOLLOWER_CONSUMER are only meaningful as destinations
 * (consumers receive data, they don't produce it), while PRODUCER is only meaningful as a source.
 */
public enum VeniceIngestionSourceComponent implements VeniceDimensionInterface {
  /** The original producer that created the message */
  PRODUCER,
  /** The local Kafka broker in the same region */
  LOCAL_BROKER,
  /** The source Kafka broker from a remote region */
  SOURCE_BROKER;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_INGESTION_SOURCE_COMPONENT;
  }
}
