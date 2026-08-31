package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_FALLBACK_REASON;


public enum VeniceBlobTransferFallbackReason implements VeniceDimensionInterface {
  /**
   * Peer discovery returned no blob candidates for the replica.
   */
  NO_CANDIDATES,
  /**
   * Every discovered blob host was attempted and none served the blob.
   */
  ALL_HOSTS_FAILED;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_BLOB_TRANSFER_FALLBACK_REASON;
  }
}
