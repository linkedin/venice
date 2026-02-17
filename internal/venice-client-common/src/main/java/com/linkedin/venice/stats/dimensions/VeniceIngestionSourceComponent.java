package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;


/**
 * Dimension enum representing the source component in the ingestion flow.
 * Used for metrics like ingestion latency between components.
 *
 * <p>Note: Similar to {@link VeniceIngestionDestinationComponent}. These are
 * 2 different enums with same values to provide type safety when defining and
 * recording metrics.
 */
public enum VeniceIngestionSourceComponent implements VeniceDimensionInterface {
  /** The original producer that created the message */
  PRODUCER,
  /** The local Kafka broker in the same region */
  LOCAL_BROKER,
  /** The source Kafka broker from a remote region */
  SOURCE_BROKER,
  /** The leader replica consumer that processes messages first */
  LEADER_CONSUMER,
  /** The follower replica consumer that replicates from leader */
  FOLLOWER_CONSUMER;

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
