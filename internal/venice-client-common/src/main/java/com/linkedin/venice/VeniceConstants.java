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
   * Compute request version 2.
   */
  public static final int COMPUTE_REQUEST_VERSION_V2 = 2;

  /**
   * V3 contains all V2 operator + new Count operator
   */
  public static final int COMPUTE_REQUEST_VERSION_V3 = 3;

  /**
   * V4 contains all V3 operators + executeWithFilter
   */
  public static final int COMPUTE_REQUEST_VERSION_V4 = 4;

  /**
   * The default SSL factory class name; this class is mostly used in test cases; products that uses Venice lib
   * should override the SSL factory class.
   */
  public static final String DEFAULT_SSL_FACTORY_CLASS_NAME = "com.linkedin.venice.security.DefaultSSLFactory";

  /**
   * In a {@link HttpServletRequest}, we can get the client certificate by retrieving the following attribute.
   */
  public static final String CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME = "javax.servlet.request.X509Certificate";

  /**
   * Beginning of router request path; it would used by router and client modules
   */

  // URI: /push_status/storeName
  public static final String TYPE_PUSH_STATUS = "push_status";

  // URI: /stream_hybrid_store_quota/storeName
  public static final String TYPE_STREAM_HYBRID_STORE_QUOTA = "stream_hybrid_store_quota";

  // URI: /stream_reprocessing_hybrid_store_quota/storeName
  public static final String TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA = "stream_reprocessing_hybrid_store_quota";

  // URI: /store_state/storeName
  public static final String TYPE_STORE_STATE = "store_state";
  // End of router request path

  public static final String NATIVE_REPLICATION_DEFAULT_SOURCE_FABRIC = "prod-lva1";

  public static final String ENVIRONMENT_CONFIG_KEY_FOR_REGION_NAME = "FABRIC";

  public static final String SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION = "com.linkedin.app.env";

  // public static final String TIMESTAMP_FIELD_NAME = "timestamp"; //
  //
  // public static final String REPLICATION_CHECKPOINT_VECTOR_FIELD = "replication_checkpoint_vector";

  /**
   * This is a sentinel value to be used in TopicSwitch message rewindStartTimestamp field between controller and server.
   * When controller specifies this, Leader server nodes will calculate the rewind start time itself.
   */
  public static final Long REWIND_TIME_DECIDED_BY_SERVER = -2L;
}
