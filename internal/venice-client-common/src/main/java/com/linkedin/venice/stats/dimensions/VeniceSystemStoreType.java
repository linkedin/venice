package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the type of Venice system store. Used to differentiate
 * OTel metrics by system store type (e.g., health checks, ingestion, replication).
 *
 * Maps to {@link VeniceMetricsDimensions#VENICE_SYSTEM_STORE_TYPE}.
 */
public enum VeniceSystemStoreType implements VeniceDimensionInterface {
  /**
   * The meta system store, which stores metadata about Venice user stores (schemas, configs,
   * replica statuses). Used by DaVinci clients and thin clients for metadata bootstrapping.
   */
  META_STORE("meta_store"),

  /**
   * The DaVinci push status system store, which tracks push job completion status as reported
   * by DaVinci client instances. Used by the controller to determine when all DaVinci clients
   * have finished ingesting a new store version.
   */
  DAVINCI_PUSH_STATUS_STORE("davinci_push_status_store");

  private final String dimensionValue;

  VeniceSystemStoreType(String dimensionValue) {
    this.dimensionValue = dimensionValue;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_SYSTEM_STORE_TYPE;
  }

  @Override
  public String getDimensionValue() {
    return dimensionValue;
  }
}
