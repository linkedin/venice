package com.linkedin.venice.controllerapi;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.ListUtils;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;


public enum ControllerRoute {

  CREATE("/create", Arrays.asList(NAME, STORE_SIZE, OWNER, KEY_SCHEMA, VALUE_SCHEMA)), // Only for tests

  STORE("/store", Arrays.asList(NAME)), // get all information about that store
  NEWSTORE("/new_store", Arrays.asList(NAME, OWNER)),
  SETVERSION("/set_version", Arrays.asList(NAME, VERSION)),
  RESERVE_VERSION("/reserve_version", Arrays.asList(NAME, VERSION)),
  NEXTVERSION("/next_version", Arrays.asList(NAME)),
  CURRENT_VERSION("/current_version", Arrays.asList(NAME)),
  ACTIVE_VERSIONS("/active_versions", Arrays.asList(NAME)),
  PAUSE_STORE("/pause_store", Arrays.asList(NAME, STATUS)), // status "true" or "false"

  JOB("/job", Arrays.asList(NAME, VERSION)),
  LIST_STORES("/list_stores", Arrays.asList()),
  LIST_NODES("/list_instances", Arrays.asList()),
  LIST_REPLICAS("/list_replicas", Arrays.asList(NAME, VERSION)),
  NODE_REPLICAS("/storage_node_replicas", Arrays.asList(STORAGE_NODE_ID)),
  NODE_REMOVABLE("/node_removable", Arrays.asList(STORAGE_NODE_ID)),

  INIT_KEY_SCHEMA("/init_key_schema", Arrays.asList(NAME, KEY_SCHEMA)),
  GET_KEY_SCHEMA("/get_key_schema", Arrays.asList(NAME)),
  ADD_VALUE_SCHEMA("/add_value_schema", Arrays.asList(NAME, VALUE_SCHEMA)),
  GET_ALL_VALUE_SCHEMA("/get_all_value_schema", Arrays.asList(NAME)),
  GET_VALUE_SCHEMA("/get_value_schema", Arrays.asList(NAME, SCHEMA_ID)),
  GET_VALUE_SCHEMA_ID("/get_value_schema_id", Arrays.asList(NAME, VALUE_SCHEMA)),

  GET_MASTER_CONTROLLER("/get_master_controller", Arrays.asList());

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
