package com.linkedin.venice;

public class VeniceConstants {
  /** The following field is used to construct result schema for compute API. */
  public static final String VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME = "__veniceComputationError__";

  /** Used as the owner property of system stores which are internal to Venice. */
  public static final String SYSTEM_STORE_OWNER = "venice-internal";

  /**
   * legacy compute request V1 would expect "double" as result while
   * any other version of compute request would expect ["float", "null"]
   */
  public static int COMPUTE_REQUEST_VERSION_V1 = 1;

  /**
   * Compute request version 2.
   */
  public static int COMPUTE_REQUEST_VERSION_V2 = 2;
}
