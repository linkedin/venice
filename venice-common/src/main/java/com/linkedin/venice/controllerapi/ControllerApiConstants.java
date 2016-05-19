package com.linkedin.venice.controllerapi;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by mwise on 3/17/16.
 */
public class ControllerApiConstants {

  public static final String CLUSTER = "cluster_name";
  public static final String NAME = "store_name";
  public static final String OWNER = "owner";
  public static final String STORE_SIZE = "store_size";
  public static final String VERSION = "version";
  public static final String STATUS = "status";
  public static final String ERROR = "error";
  public static final String KEY_SCHEMA = "key_schema";
  public static final String VALUE_SCHEMA = "value_schema";

  public static final List<String> VERSION_PARAMS = new ArrayList<>();
  static {
    VERSION_PARAMS.add(CLUSTER);
    VERSION_PARAMS.add(NAME);
    VERSION_PARAMS.add(VERSION);
  }

  public static final String CREATE_PATH = "/create";
  public static final List<String> CREATE_PARAMS = new ArrayList<>();
  static {
    CREATE_PARAMS.add(CLUSTER);
    CREATE_PARAMS.add(NAME);
    CREATE_PARAMS.add(STORE_SIZE);
    CREATE_PARAMS.add(OWNER);
  }

  public static final String NEWSTORE_PATH = "/newstore";
  public static final List<String> NEWSTORE_PARAMS = new ArrayList<>();
  static {
    NEWSTORE_PARAMS.add(CLUSTER);
    NEWSTORE_PARAMS.add(NAME);
    NEWSTORE_PARAMS.add(OWNER);
  }

  public static final String SETVERSION_PATH = "/setversion";
  public static final List<String> SETVERSION_PARAMS = VERSION_PARAMS;

  public static final String RESERVE_VERSION_PATH = "/reserveversion";
  public static final List<String> RESERVE_VERSION_PARAMS = VERSION_PARAMS;

  public static final String NEXTVERSION_PATH = "/nextversion";
  public static final List<String> NEXTVERSION_PARAMS = new ArrayList<>();
  static{
    NEXTVERSION_PARAMS.add(CLUSTER);
    NEXTVERSION_PARAMS.add(NAME);
  }

  public static final String CURRENT_VERSION_PATH = "/currentversion";
  public static final List<String> CURRENT_VERSION_PARAMS = NEXTVERSION_PARAMS; /* cluster and storename */

  public static final String ACTIVE_VERSIONS_PATH = "/activeversions";
  public static final List<String> ACTIVE_VERSIONS_PARAMS = NEXTVERSION_PARAMS; /* cluster and storename */

  public static final String JOB_PATH = "/job";
  public static final List<String> JOB_PARMAS = VERSION_PARAMS;

  private ControllerApiConstants(){}
}
