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
  public static final String ENABLE_READS = "enable_reads";
  public static final String ENABLE_WRITES = "enable_writes";
  public static final String STORAGE_QUOTA_IN_BYTE = "storage_quota_in_byte";
  public static final String READ_QUOTA_IN_CU = "read_quota_in_cu";
  public static final String REWIND_TIME_IN_SECONDS = "rewind_time_seconds";
  public static final String OFFSET_LAG_TO_GO_ONLINE = "offset_lag_to_go_online";
  public static final String COMPRESSION_STRATEGY = "compression_strategy";

  public static final String PUSH_TYPE = "push_type";
  public static final String PUSH_JOB_ID = "push_job_id";

  public static final String EXPECTED_ROUTER_COUNT = "expected_router_count";

  public static final String VOLDEMORT_STORE_NAME = "voldemort_store_name";
  public static final String PUSH_STRATEGY = "push_strategy";

  public static final String ACCESS_CONTROLLED = "access_controlled";

  private ControllerApiConstants(){}

  /**
   * Producer type for pushing data
   */
  public enum PushType {
    BATCH, //This is a batch push that will create a new version
    STREAM //This is a stream job that writes into a buffer topic (or possibly the current version topic depending on store-level configs)
  }
}
