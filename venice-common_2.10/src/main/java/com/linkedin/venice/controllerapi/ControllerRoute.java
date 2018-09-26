package com.linkedin.venice.controllerapi;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.ListUtils;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;


public enum ControllerRoute {

  CREATE_VERSION("/create_version", Arrays.asList(NAME, STORE_SIZE)),
  REQUEST_TOPIC("/request_topic", Arrays.asList(NAME, STORE_SIZE, PUSH_TYPE, PUSH_JOB_ID)), // topic that writer should produce to
  EMPTY_PUSH("/empty_push", Arrays.asList(NAME, STORE_SIZE, PUSH_JOB_ID)), // do an empty push into a new version for this store
  END_OF_PUSH("/end_of_push", Arrays.asList(NAME, VERSION)), // write an END OF PUSH message into the topic
  STORE("/store", Arrays.asList(NAME)), // get all information about that store
  NEW_STORE("/new_store", Arrays.asList(NAME, KEY_SCHEMA, VALUE_SCHEMA), OWNER),
  MIGRATE_STORE("/migrate_store", Arrays.asList(NAME, CLUSTER, CLUSTER_SRC)),
  DELETE_STORE("/delete_store", Arrays.asList(NAME)),
  // Beside store name, others are all optional parameters for flexibility and compatibility.
  UPDATE_STORE("/update_store", Arrays.asList(NAME),OWNER, VERSION, LARGEST_USED_VERSION_NUMBER, PARTITION_COUNT,
      ENABLE_READS, ENABLE_WRITES, STORAGE_QUOTA_IN_BYTE, READ_QUOTA_IN_CU, REWIND_TIME_IN_SECONDS,
      OFFSET_LAG_TO_GO_ONLINE, ACCESS_CONTROLLED, COMPRESSION_STRATEGY, CHUNKING_ENABLED, SINGLE_GET_ROUTER_CACHE_ENABLED,
      BATCH_GET_ROUTER_CACHE_ENABLED, BATCH_GET_LIMIT, NUM_VERSIONS_TO_PRESERVE),
  SET_VERSION("/set_version", Arrays.asList(NAME, VERSION)),
  ENABLE_STORE("/enable_store", Arrays.asList(NAME, OPERATION, STATUS)), // status "true" or "false", operation "read" or "write" or "readwrite".
  DELETE_ALL_VERSIONS("/delete_all_versions", Arrays.asList(NAME)),
  DELETE_OLD_VERSION("/delete_old_version", Arrays.asList(NAME, VERSION)),

  JOB("/job", Arrays.asList(NAME, VERSION)),
  KILL_OFFLINE_PUSH_JOB("/kill_offline_push_job", Arrays.asList(TOPIC)),
  LIST_STORES("/list_stores", Arrays.asList()),
  LIST_NODES("/list_instances", Arrays.asList()),
  CLUSTER_HELATH_STORES("/cluster_health_stores", Arrays.asList()),
  ClUSTER_HEALTH_INSTANCES("/cluster_health_instances", Arrays.asList()),
  LIST_REPLICAS("/list_replicas", Arrays.asList(NAME, VERSION)),
  NODE_REPLICAS("/storage_node_replicas", Arrays.asList(STORAGE_NODE_ID)),
  NODE_REMOVABLE("/node_removable", Arrays.asList(STORAGE_NODE_ID), INSTANCE_VIEW),
  WHITE_LIST_ADD_NODE("/white_list_add_node", Arrays.asList(STORAGE_NODE_ID)),
  WHITE_LIST_REMOVE_NODE("/white_list_remove_node", Arrays.asList(STORAGE_NODE_ID)),
  REMOVE_NODE("/remove_node", Arrays.asList(STORAGE_NODE_ID)),
  SKIP_ADMIN("/skip_admin_message", Arrays.asList(OFFSET)),

  GET_KEY_SCHEMA("/get_key_schema", Arrays.asList(NAME)),
  ADD_VALUE_SCHEMA("/add_value_schema", Arrays.asList(NAME, VALUE_SCHEMA)),
  SET_OWNER("/set_owner", Arrays.asList(NAME, OWNER)),
  SET_PARTITION_COUNT("/set_partition_count", Arrays.asList(NAME, PARTITION_COUNT)),
  GET_ALL_VALUE_SCHEMA("/get_all_value_schema", Arrays.asList(NAME)),
  GET_VALUE_SCHEMA("/get_value_schema", Arrays.asList(NAME, SCHEMA_ID)),
  GET_VALUE_SCHEMA_ID("/get_value_schema_id", Arrays.asList(NAME, VALUE_SCHEMA)),
  MASTER_CONTROLLER("/master_controller", Arrays.asList()),

  EXECUTION("/execution", Arrays.asList(EXECUTION_ID)),
  LAST_SUCCEED_EXECUTION_ID("/last_succeed_execution_id", Arrays.asList()),

  STORAGE_ENGINE_OVERHEAD_RATIO("/storage_engine_overhead_ratio", Arrays.asList(NAME)),

  ENABLE_THROTTLING("/enable_throttling", Arrays.asList(STATUS)),
  ENABLE_MAX_CAPACITY_PROTECTION("/enable_max_capacity_protection", Arrays.asList(STATUS)),
  ENABLE_QUOTA_REBALANCED("/enable_quota_rebalanced", Arrays.asList(STATUS, EXPECTED_ROUTER_COUNT)),
  GET_ROUTERS_CLUSTER_CONFIG("/get_routers_cluster_config", Arrays.asList()),

  // TODO: those operations don't require param: cluster.
  // This could be resolved in multi-cluster support project.
  GET_ALL_MIGRATION_PUSH_STRATEGIES("/get_all_push_strategies", Arrays.asList()),
  SET_MIGRATION_PUSH_STRATEGY("/set_push_strategy", Arrays.asList(VOLDEMORT_STORE_NAME, PUSH_STRATEGY)),

  CLUSTER_DISCOVERY("/discover_cluster", Arrays.asList(NAME)),
  LIST_BOOTSTRAPPING_VERSIONS("/list_bootstrapping_versions", Arrays.asList()),

  OFFLINE_PUSH_INFO("/offline_push_info", Arrays.asList(NAME, VERSION)),

  UPLOAD_PUSH_JOB_STATUS("/upload_push_job_status", Arrays.asList(CLUSTER, NAME, VERSION, PUSH_JOB_STATUS,
      PUSH_JOB_DURATION, PUSH_JOB_ID));

  private final String path;
  private final List<String> params;
  private final List<String> optionalParams;

  ControllerRoute(String path, List<String> params, String... optionalParams){
    this.path = path;
    this.params = ListUtils.union(params, getCommonParams());
    this.optionalParams = Arrays.asList(optionalParams);
  }

  private static List<String> getCommonParams(){
    return Arrays.asList(HOSTNAME, CLUSTER);
  }

  public String getPath(){
    return path;
  }

  public List<String> getParams(){
    return params;
  }

  public List<String> getOptionalParams() {
    return optionalParams;
  }

}
