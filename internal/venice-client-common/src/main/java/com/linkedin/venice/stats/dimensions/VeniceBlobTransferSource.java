package com.linkedin.venice.stats.dimensions;

public enum VeniceBlobTransferSource implements VeniceDimensionInterface {
  DAVINCI_PEER, VENICE_SERVER;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_SOURCE;
  }
}
