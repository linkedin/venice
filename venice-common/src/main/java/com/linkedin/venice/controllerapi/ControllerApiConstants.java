package com.linkedin.venice.controllerapi;

public class ControllerApiConstants {

  public static final String HOSTNAME = "hostname";
  public static final String CLUSTER = "cluster_name";

  public static final String NAME = "store_name";
  public static final String OWNER = "owner";
  public static final String PARTITION_COUNT = "partition_count";
  public static final String STORE_SIZE = "store_size";
  public static final String VERSION = "version";
  public static final String STATUS = "status";
  public static final String FROZEN = "frozen";
  public static final String ERROR = "error";
  public static final String STORAGE_NODE_ID = "storage_node_id"; /* host_port */
  public static final String KEY_SCHEMA = "key_schema";
  public static final String VALUE_SCHEMA = "value_schema";
  public static final String SCHEMA_ID = "schema_id";
  public static final String TOPIC = "topic"; // TODO remove this, everyone should use store and version instead
  public static final String OFFSET = "offset";
  public static final String OPERATION = "operation";
  public static final String READ_OPERATION = "read";
  public static final String WRITE_OPERATION = "write";
  public static final String READ_WRITE_OPERATION = READ_OPERATION + WRITE_OPERATION;
  public static final String EXECUTION_ID = "execution_id";

  private ControllerApiConstants(){}
}
