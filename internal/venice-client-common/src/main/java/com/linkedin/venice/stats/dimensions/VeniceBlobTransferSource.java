package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_SOURCE;


public enum VeniceBlobTransferSource implements VeniceDimensionInterface {
  DAVINCI_PEER, VENICE_SERVER;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_BLOB_TRANSFER_SOURCE;
  }
}
