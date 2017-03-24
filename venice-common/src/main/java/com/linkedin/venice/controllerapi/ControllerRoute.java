package com.linkedin.venice.controllerapi;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.ListUtils;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;


public enum ControllerRoute {

  CREATE_VERSION("/create_version", Arrays.asList(NAME, STORE_SIZE)),
  STORE("/store", Arrays.asList(NAME)), // get all information about that store
  NEW_STORE("/new_store", Arrays.asList(NAME, OWNER, KEY_SCHEMA, VALUE_SCHEMA)),
  SET_VERSION("/set_version", Arrays.asList(NAME, VERSION)),
  CURRENT_VERSION("/current_version", Arrays.asList(NAME)),
  ACTIVE_VERSIONS("/active_versions", Arrays.asList(NAME)),
  ENABLE_STORE("/enable_store", Arrays.asList(NAME, OPERATION, STATUS)), // status "true" or "false", operation "read" or "write" or "readwrite".
  DELETE_ALL_VERSIONS("/delete_all_versions", Arrays.asList(NAME)),

  JOB("/job", Arrays.asList(NAME, VERSION)),
  KILL_OFFLINE_PUSH_JOB("/kill_offline_push_job", Arrays.asList(TOPIC)),
  LIST_STORES("/list_stores", Arrays.asList()),
  LIST_NODES("/list_instances", Arrays.asList()),
  CLUSTER_HELATH_STORES("/cluster_health_stores", Arrays.asList()),
  ClUSTER_HEALTH_INSTANCES("/cluster_health_instances", Arrays.asList()),
  LIST_REPLICAS("/list_replicas", Arrays.asList(NAME, VERSION)),
  NODE_REPLICAS("/storage_node_replicas", Arrays.asList(STORAGE_NODE_ID)),
  NODE_REMOVABLE("/node_removable", Arrays.asList(STORAGE_NODE_ID)),
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
  LAST_SUCCEED_EXECUTION_ID("/last_succeed_execution_id", Arrays.asList());

  private final String path;
  private final List<String> params;

  ControllerRoute(String path, List<String> params){
    this.path = path;
    this.params = ListUtils.union(params, getCommonParams());
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

}
