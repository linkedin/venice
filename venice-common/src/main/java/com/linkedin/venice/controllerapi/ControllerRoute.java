package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpMethod;

import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import org.apache.commons.collections.ListUtils;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

public enum ControllerRoute {
  REQUEST_TOPIC("/request_topic", HttpMethod.POST, Arrays.asList(NAME, STORE_SIZE, PUSH_TYPE, PUSH_JOB_ID), PUSH_IN_SORTED_ORDER), // topic that writer should produce to
  EMPTY_PUSH("/empty_push", HttpMethod.POST, Arrays.asList(NAME, STORE_SIZE, PUSH_JOB_ID)), // do an empty push into a new version for this store
  END_OF_PUSH("/end_of_push", HttpMethod.POST, Arrays.asList(NAME, VERSION)), // write an END OF PUSH message into the topic
  STORE("/store", HttpMethod.GET, Collections.singletonList(NAME)), // get all information about that store
  NEW_STORE("/new_store", HttpMethod.POST, Arrays.asList(NAME, KEY_SCHEMA, VALUE_SCHEMA), OWNER, IS_SYSTEM_STORE, ACCESS_PERMISSION),
  CHECK_RESOURCE_CLEANUP_FOR_STORE_CREATION("/check_resource_cleanup_for_store_creation", HttpMethod.GET, Arrays.asList(CLUSTER, NAME)),
  MIGRATE_STORE("/migrate_store", HttpMethod.POST, Arrays.asList(NAME, CLUSTER, CLUSTER_DEST)),
  COMPLETE_MIGRATION("/complete_migration", HttpMethod.POST, Arrays.asList(NAME, CLUSTER, CLUSTER_DEST)),
  ABORT_MIGRATION("/abort_migration", HttpMethod.POST, Arrays.asList(NAME, CLUSTER, CLUSTER_DEST)),
  DELETE_STORE("/delete_store", HttpMethod.POST, Collections.singletonList(NAME)),
  // Beside store name, others are all optional parameters for flexibility and compatibility.
  UPDATE_STORE("/update_store", HttpMethod.POST, Collections.singletonList(NAME), OWNER, VERSION, LARGEST_USED_VERSION_NUMBER,
      PARTITION_COUNT, PARTITIONER_CLASS, PARTITIONER_PARAMS, AMPLIFICATION_FACTOR,
      ENABLE_READS, ENABLE_WRITES, STORAGE_QUOTA_IN_BYTE, HYBRID_STORE_OVERHEAD_BYPASS, READ_QUOTA_IN_CU, REWIND_TIME_IN_SECONDS,
      OFFSET_LAG_TO_GO_ONLINE, ACCESS_CONTROLLED, COMPRESSION_STRATEGY, CLIENT_DECOMPRESSION_ENABLED, CHUNKING_ENABLED,
      SINGLE_GET_ROUTER_CACHE_ENABLED, BATCH_GET_ROUTER_CACHE_ENABLED, BATCH_GET_LIMIT, NUM_VERSIONS_TO_PRESERVE,
      WRITE_COMPUTATION_ENABLED, READ_COMPUTATION_ENABLED, LEADER_FOLLOWER_MODEL_ENABLED, BACKUP_STRATEGY, AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED,
      INCREMENTAL_PUSH_ENABLED, BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS, HYBRID_STORE_DISK_QUOTA_ENABLED,
      REGULAR_VERSION_ETL_ENABLED, FUTURE_VERSION_ETL_ENABLED, ETLED_PROXY_USER_ACCOUNT, INCREMENTAL_PUSH_POLICY),
  SET_VERSION("/set_version", HttpMethod.POST, Arrays.asList(NAME, VERSION)),
  ENABLE_STORE("/enable_store", HttpMethod.POST, Arrays.asList(NAME, OPERATION, STATUS)), // status "true" or "false", operation "read" or "write" or "readwrite".
  DELETE_ALL_VERSIONS("/delete_all_versions", HttpMethod.POST, Collections.singletonList(NAME)),
  DELETE_OLD_VERSION("/delete_old_version", HttpMethod.POST, Arrays.asList(NAME, VERSION)),
  UPDATE_CLUSTER_CONFIG("/update_cluster_config", HttpMethod.POST, Collections.singletonList(CLUSTER), SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND),

  JOB("/job", HttpMethod.GET, Arrays.asList(NAME, VERSION)),
  KILL_OFFLINE_PUSH_JOB("/kill_offline_push_job", HttpMethod.POST, Collections.singletonList(TOPIC)),
  LIST_STORES("/list_stores", HttpMethod.GET, Collections.emptyList(), INCLUDE_SYSTEM_STORES),
  LIST_CHILD_CLUSTERS("/list_child_clusters", HttpMethod.GET, Collections.emptyList()),
  LIST_NODES("/list_instances", HttpMethod.GET, Collections.emptyList()),
  CLUSTER_HEALTH_STORES("/cluster_health_stores", HttpMethod.GET, Collections.emptyList()),
  ClUSTER_HEALTH_INSTANCES("/cluster_health_instances", HttpMethod.GET, Collections.emptyList()),
  LIST_REPLICAS("/list_replicas", HttpMethod.GET, Arrays.asList(NAME, VERSION)),
  NODE_REPLICAS("/storage_node_replicas", HttpMethod.GET, Collections.singletonList(STORAGE_NODE_ID)),
  NODE_REMOVABLE("/node_removable", HttpMethod.GET, Collections.singletonList(STORAGE_NODE_ID), INSTANCE_VIEW),
  WHITE_LIST_ADD_NODE("/white_list_add_node", HttpMethod.POST, Collections.singletonList(STORAGE_NODE_ID)),
  WHITE_LIST_REMOVE_NODE("/white_list_remove_node", HttpMethod.POST, Collections.singletonList(STORAGE_NODE_ID)),
  REMOVE_NODE("/remove_node", HttpMethod.POST, Collections.singletonList(STORAGE_NODE_ID)),
  SKIP_ADMIN("/skip_admin_message", HttpMethod.POST, Collections.singletonList(OFFSET)),

  GET_KEY_SCHEMA("/get_key_schema", HttpMethod.GET, Collections.singletonList(NAME)),
  ADD_VALUE_SCHEMA("/add_value_schema", HttpMethod.POST,  Arrays.asList(NAME, VALUE_SCHEMA)),
  ADD_DERIVED_SCHEMA("/add_derived_schema", HttpMethod.POST, Arrays.asList(NAME, SCHEMA_ID, DERIVED_SCHEMA)),
  SET_OWNER("/set_owner", HttpMethod.POST, Arrays.asList(NAME, OWNER)),
  SET_PARTITION_COUNT("/set_partition_count", HttpMethod.POST, Arrays.asList(NAME, PARTITION_COUNT)),
  GET_ALL_VALUE_SCHEMA("/get_all_value_schema", HttpMethod.GET, Collections.singletonList(NAME)),
  GET_ALL_VALUE_AND_DERIVED_SCHEMA("/get_all_value_and_derived_schema", HttpMethod.GET, Collections.singletonList(NAME)),
  GET_VALUE_SCHEMA("/get_value_schema", HttpMethod.GET, Arrays.asList(NAME, SCHEMA_ID)),
  GET_VALUE_SCHEMA_ID("/get_value_schema_id", HttpMethod.POST, Arrays.asList(NAME, VALUE_SCHEMA)),
  GET_VALUE_OR_DERIVED_SCHEMA_ID("/get_value_or_derived_schema_id", HttpMethod.POST, Arrays.asList(NAME, DERIVED_SCHEMA)),
  REMOVE_DERIVED_SCHEMA("/remove_derived_schema", HttpMethod.POST, Arrays.asList(NAME, SCHEMA_ID, DERIVED_SCHEMA_ID)),
  MASTER_CONTROLLER("/master_controller", HttpMethod.GET, Collections.emptyList()),

  EXECUTION("/execution", HttpMethod.GET, Collections.singletonList(EXECUTION_ID)),
  LAST_SUCCEED_EXECUTION_ID("/last_succeed_execution_id", HttpMethod.GET, Collections.emptyList()),

  STORAGE_ENGINE_OVERHEAD_RATIO("/storage_engine_overhead_ratio", HttpMethod.GET, Collections.singletonList(NAME)),

  ENABLE_THROTTLING("/enable_throttling", HttpMethod.POST, Collections.singletonList(STATUS)),
  ENABLE_MAX_CAPACITY_PROTECTION("/enable_max_capacity_protection", HttpMethod.POST, Collections.singletonList(STATUS)),
  ENABLE_QUOTA_REBALANCED("/enable_quota_rebalanced", HttpMethod.POST, Arrays.asList(STATUS, EXPECTED_ROUTER_COUNT)),
  GET_ROUTERS_CLUSTER_CONFIG("/get_routers_cluster_config", HttpMethod.GET, Collections.emptyList()),

  // TODO: those operations don't require param: cluster.
  // This could be resolved in multi-cluster support project.
  GET_ALL_MIGRATION_PUSH_STRATEGIES("/get_all_push_strategies", HttpMethod.GET, Collections.emptyList()),
  SET_MIGRATION_PUSH_STRATEGY("/set_push_strategy", HttpMethod.GET, Arrays.asList(VOLDEMORT_STORE_NAME, PUSH_STRATEGY)),

  CLUSTER_DISCOVERY("/discover_cluster", HttpMethod.GET, Collections.singletonList(NAME)),
  LIST_BOOTSTRAPPING_VERSIONS("/list_bootstrapping_versions", HttpMethod.GET, Collections.emptyList()),

  OFFLINE_PUSH_INFO("/offline_push_info", HttpMethod.POST, Arrays.asList(NAME, VERSION)),

  UPLOAD_PUSH_JOB_STATUS("/upload_push_job_status", HttpMethod.POST, Arrays.asList(CLUSTER, NAME, VERSION, PUSH_JOB_STATUS,
      PUSH_JOB_DURATION, PUSH_JOB_ID)),

  SEND_PUSH_JOB_DETAILS("/send_push_job_details", HttpMethod.POST, Arrays.asList(CLUSTER, NAME, VERSION), PUSH_JOB_DETAILS),

  ADD_VERSION("/add_version", HttpMethod.POST, Arrays.asList(NAME, PUSH_JOB_ID, VERSION, PARTITION_COUNT)),

  LIST_LF_STORES("/list_lf_stores", HttpMethod.GET, Collections.emptyList()),

  ENABLE_LF_MODEL("/enable_lf_model", HttpMethod.POST, Arrays.asList(STORE_TYPE, STATUS)),

  DEMATERIALIZE_METADATA_STORE_VERSION("/dematerialize_metadata_store_version", HttpMethod.POST, Arrays.asList(CLUSTER, NAME, VERSION)),
  FUTURE_VERSION("/list_future_versions", HttpMethod.GET, Arrays.asList(CLUSTER, NAME)),
  SET_TOPIC_COMPACTION("/set_topic_compaction", HttpMethod.POST, Arrays.asList(TOPIC, TOPIC_COMPACTION_POLICY)),
  UPDATE_ACL("/update_acl", HttpMethod.POST, Arrays.asList(CLUSTER, NAME, ACCESS_PERMISSION)),
  GET_ACL("/get_acl", HttpMethod.GET, Arrays.asList(CLUSTER, NAME)),
  DELETE_ACL("/delete_acl", HttpMethod.GET, Arrays.asList(CLUSTER, NAME)),
  CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER("/configure_native_replication_for_cluster", HttpMethod.POST, Arrays.asList(CLUSTER, STORE_TYPE, STATUS)),
  CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER("/configure_active_active_replication_for_cluster", HttpMethod.POST, Arrays.asList(CLUSTER, STORE_TYPE, STATUS)),
  GET_DELETABLE_STORE_TOPICS("/get_deletable_store_topics", HttpMethod.GET, Collections.emptyList()),
  GET_ALL_REPLICATION_METADATA_SCHEMAS("/get_all_replication_metadata_schemas", HttpMethod.GET,
      Collections.singletonList(NAME)),
  CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER("/configure_incremental_push_for_cluster", HttpMethod.POST, Arrays.asList(CLUSTER, INCREMENTAL_PUSH_POLICY)),
  GET_ONGOING_INCREMENTAL_PUSH_VERSIONS("/get_ongoing_incremental_push_versions", HttpMethod.GET,
      Collections.singletonList(TOPIC));


  private final String path;
  private final HttpMethod httpMethod;
  private final List<String> params;
  private final List<String> optionalParams;

  ControllerRoute(String path, HttpMethod httpMethod, List<String> params, String... optionalParams) {
    this.path = path;
    this.httpMethod = httpMethod;
    this.params = ListUtils.union(params, getCommonParams());
    this.optionalParams = Arrays.asList(optionalParams);
  }

  private static List<String> getCommonParams() {
    // This will work together with AdminSparkServer#validateParams
    return Arrays.asList(HOSTNAME);
  }

  public String getPath(){
    return path;
  }

  public static ControllerRoute valueOfPath(String path) {
    for (ControllerRoute route : values()) {
      if (route.pathEquals(path)) {
        return route;
      }
    }
    return null;
  }

  public boolean pathEquals(String uri) {
    // strips slashes from beginning and end of passed in uri and does a string comparison
    if(uri == null) {
      return false;
    }
    return path.replaceAll("/", "").equals(uri.replaceAll("/", ""));
  }

  public HttpMethod getHttpMethod() {
    return this.httpMethod;
  }

  public List<String> getParams(){
    return params;
  }

  public List<String> getOptionalParams() {
    return optionalParams;
  }
}
