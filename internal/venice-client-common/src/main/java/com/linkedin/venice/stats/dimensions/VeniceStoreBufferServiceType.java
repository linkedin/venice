package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum for store buffer service type: sorted vs unsorted drainer queues.
 */
public enum VeniceStoreBufferServiceType implements VeniceDimensionInterface {
  SORTED, UNSORTED;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_STORE_BUFFER_SERVICE_TYPE;
  }
}
