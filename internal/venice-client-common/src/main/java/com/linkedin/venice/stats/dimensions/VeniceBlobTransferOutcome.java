package com.linkedin.venice.stats.dimensions;

public enum VeniceBlobTransferOutcome implements VeniceDimensionInterface {
  SUCCESS, NO_CANDIDATES, NOT_FOUND, THROTTLED, CONNECT_FAILURE, SSL_FAILURE, OUT_OF_MEMORY, ACL_DENIED,
  VERSION_MISMATCH, MIXED, OTHER;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_OUTCOME;
  }
}
