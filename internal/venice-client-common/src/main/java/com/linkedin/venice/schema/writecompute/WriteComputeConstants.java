package com.linkedin.venice.schema.writecompute;

/**
 * This utility class contains all String literal constants used in write compute.
 *
 * TODO: make this class package private
 * Ideally this class should be package private because write-compute String literal constants are write-compute
 * internal implementation details. That means if other packages access these constants, they depend on implementation
 * details of write compute (which is not a good practice) instead of depending on write-compute APIs.
 *
 */
public class WriteComputeConstants {
  private WriteComputeConstants() {
    // Utility class
  }

  public static final String WRITE_COMPUTE_RECORD_SCHEMA_SUFFIX = "WriteOpRecord";

  // List operations
  public static final String LIST_OPS_NAME = "ListOps";
  public static final String SET_UNION = "setUnion";
  public static final String SET_DIFF = "setDiff";

  // Map operations
  public static final String MAP_OPS_NAME = "MapOps";
  public static final String MAP_UNION = "mapUnion";
  public static final String MAP_DIFF = "mapDiff";
}
