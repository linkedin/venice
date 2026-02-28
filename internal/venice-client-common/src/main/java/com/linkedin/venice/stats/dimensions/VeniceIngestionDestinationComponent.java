package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;


/**
 * Dimension enum representing the destination component in the ingestion flow.
 * Used for metrics like ingestion latency between components.
 *
 * <p>Note: Paired with {@link VeniceIngestionSourceComponent}. The two enums have asymmetric
 * value sets because LEADER_CONSUMER and FOLLOWER_CONSUMER are only meaningful as destinations
 * (consumers receive data, they don't produce it), while PRODUCER is only meaningful as a source.
 */
public enum VeniceIngestionDestinationComponent implements VeniceDimensionInterface {
  /** The local Kafka broker in the same region */
  LOCAL_BROKER,
  /** The source Kafka broker from a remote region */
  SOURCE_BROKER,
  /** The leader replica consumer that processes messages first */
  LEADER_CONSUMER,
  /** The follower replica consumer that replicates from leader */
  FOLLOWER_CONSUMER;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_INGESTION_DESTINATION_COMPONENT;
  }
}
