package com.linkedin.venice.stats.dimensions;

/**
 * Dimension to represent the type of read-compute operation performed on a value.
 *
 * @see com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType
 */
public enum VeniceComputeOperationType implements VeniceDimensionInterface {
  /** Computes the dot product between two vectors. */
  DOT_PRODUCT,
  /** Computes the cosine similarity between two vectors. */
  COSINE_SIMILARITY,
  /** Computes the element-wise (Hadamard) product of two vectors. */
  HADAMARD_PRODUCT,
  /** Counts the number of elements in a collection field. */
  COUNT;

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_COMPUTE_OPERATION_TYPE;
  }
}
