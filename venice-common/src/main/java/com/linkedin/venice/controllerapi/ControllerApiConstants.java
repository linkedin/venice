package com.linkedin.venice.controllerapi;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.ListUtils;


public class ControllerApiConstants {

  public static final String HOSTNAME = "hostname";
  public static final String CLUSTER = "cluster_name";

  public static final String NAME = "store_name";
  public static final String OWNER = "owner";
  public static final String STORE_SIZE = "store_size";
  public static final String VERSION = "version";
  public static final String STATUS = "status";
  public static final String ERROR = "error";
  public static final String KEY_SCHEMA = "key_schema";
  public static final String VALUE_SCHEMA = "value_schema";
  public static final String SCHEMA_ID = "schema_id";

  public static final List<String> COMMON_PARAMS = Arrays.asList(HOSTNAME, CLUSTER);

  public static final String CREATE_PATH = "/create";
  public static final List<String> CREATE_PARAMS = ListUtils.union(Arrays.asList(NAME, STORE_SIZE, OWNER, KEY_SCHEMA, VALUE_SCHEMA),COMMON_PARAMS);


  public static final String NEWSTORE_PATH = "/new_store";
  public static final List<String> NEWSTORE_PARAMS = ListUtils.union(Arrays.asList(NAME, OWNER), COMMON_PARAMS);

  public static final String SETVERSION_PATH = "/set_version";
  public static final List<String> SETVERSION_PARAMS = ListUtils.union(Arrays.asList(NAME, VERSION), COMMON_PARAMS);

  public static final String RESERVE_VERSION_PATH = "/reserve_version";
  public static final List<String> RESERVE_VERSION_PARAMS = ListUtils.union(Arrays.asList(NAME, VERSION), COMMON_PARAMS);

  public static final String NEXTVERSION_PATH = "/next_version";
  public static final List<String> NEXTVERSION_PARAMS = ListUtils.union(Arrays.asList(NAME), COMMON_PARAMS);

  public static final String CURRENT_VERSION_PATH = "/current_version";
  public static  List<String> CURRENT_VERSION_PARAMS = ListUtils.union(Arrays.asList(NAME), COMMON_PARAMS);

  public static final String ACTIVE_VERSIONS_PATH = "/active_versions";
  public static final List<String> ACTIVE_VERSIONS_PARAMS = ListUtils.union(Arrays.asList(NAME), COMMON_PARAMS);

  public static final String JOB_PATH = "/job";
  public static final List<String> JOB_PARMAS = ListUtils.union(Arrays.asList(NAME, VERSION), COMMON_PARAMS);

  public static final String LIST_STORES_PATH = "/list_stores";
  public static final List<String> LIST_STORES_PARAMS = ListUtils.union(Arrays.asList(), COMMON_PARAMS);

  public static final String INIT_KEY_SCHEMA_PATH = "/init_key_schema";
  public static final List<String> INIT_KEY_SCHEMA_PARAMS = ListUtils.union(Arrays.asList(NAME, KEY_SCHEMA), COMMON_PARAMS);

  public static final String GET_KEY_SCHEMA_PATH = "/get_key_schema";
  public static final List<String> GET_KEY_SCHEMA_PARAMS = ListUtils.union(Arrays.asList(NAME), COMMON_PARAMS);

  public static final String ADD_VALUE_SCHEMA_PATH = "/add_value_schema";
  public static final List<String> ADD_VALUE_SCHEMA_PARAMS = ListUtils.union(Arrays.asList(NAME, VALUE_SCHEMA), COMMON_PARAMS);

  public static final String GET_ALL_VALUE_SCHEMA_PATH = "/get_all_value_schema";
  public static final List<String> GET_ALL_VALUE_SCHEMA_PARAMS = ListUtils.union(Arrays.asList(NAME), COMMON_PARAMS);

  public static final String GET_VALUE_SCHEMA_PATH = "/get_value_schema";
  public static  List<String> GET_VALUE_SCHEMA_PARAMS = ListUtils.union(Arrays.asList(NAME, SCHEMA_ID), COMMON_PARAMS);

  public static final String GET_VALUE_SCHEMA_ID_PATH = "/get_value_schema_id";
  public static final List<String> GET_VALUE_SCHEMA_ID_PARAMS = ListUtils.union(Arrays.asList(NAME, VALUE_SCHEMA), COMMON_PARAMS);

  public static final String GET_MASTER_CONTROLLER_PATH = "/get_master_controller";
  public static final List<String> GET_MASTER_CONTROLLER_PARAMS = ListUtils.union(Arrays.asList(), COMMON_PARAMS);

  private ControllerApiConstants(){}
}
