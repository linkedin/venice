package com.linkedin.venice;

import javax.servlet.http.HttpServletRequest;

public class VeniceConstants {
  /** The following field is used to construct result schema for compute API. */
  public static final String VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME = "__veniceComputationError__";

  /** Used as the owner property of system stores which are internal to Venice. */
  public static final String SYSTEM_STORE_OWNER = "venice-internal";

  /**
   * Default per router max read quota; notice that this value is used in controller;
   * the actual per router max read quota is defined as a router config "max.read.capacity".
   *
   * TODO: Support common configs among different components, so that we can define the config value once
   *       and used everywhere.
   */
  public static final int DEFAULT_PER_ROUTER_READ_QUOTA = 20_000_000;

  /**
   * DEPRECATED!
   * legacy compute request V1 would expect "double" as result while
   * any other version of compute request would expect ["float", "null"]
   */
  public static int COMPUTE_REQUEST_VERSION_V1 = 1;

  /**
   * Compute request version 2.
   */
  public static int COMPUTE_REQUEST_VERSION_V2 = 2;

  /**
   * V3 contains all V2 operator + new Count operator
   */
  public static int COMPUTE_REQUEST_VERSION_V3 = 3;

  /**
   * The default SSL factory class name; this class is mostly used in test cases; products that uses Venice lib
   * should override the SSL factory class.
   */
  public static String DEFAULT_SSL_FACTORY_CLASS_NAME = "com.linkedin.venice.security.DefaultSSLFactory";

  /**
   * In a {@link HttpServletRequest}, we can get the client certificate by retrieving the following attribute.
   */
  public static String CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME = "javax.servlet.request.X509Certificate";

  /**
   * Beginning of router request path; it would used by router and client modules
   */

  // URI: /push_status/storeName
  public static final String TYPE_PUSH_STATUS = "push_status";

  // URI: /hybrid_store_quota/storeName
  public static final String TYPE_HYBRID_STORE_QUOTA = "hybrid_store_quota";
  // End of router request path
}
