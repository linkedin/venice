package com.linkedin.venice.pubsub;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;


/**
 * Categories of PubSub health tracked independently by the health monitor.
 * A broker can be healthy while the metadata service is unhealthy, or vice versa.
 */
public enum PubSubHealthCategory implements VeniceDimensionInterface {
  /** PubSub broker health — affects produce and consume operations */
  BROKER,

  /** PubSub metadata service health — affects topic creation, deletion, and metadata queries */
  METADATA_SERVICE;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_PUBSUB_HEALTH_CATEGORY;
  }
}
