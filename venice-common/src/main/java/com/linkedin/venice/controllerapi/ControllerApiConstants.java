package com.linkedin.venice.controllerapi;

import java.util.ArrayList;
import java.util.Arrays;
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

  public static final String CREATE_PATH = "/create";
  public static final List<String> CREATE_PARAMS = Arrays.asList(CLUSTER, NAME, STORE_SIZE, OWNER);

  public static final String NEWSTORE_PATH = "/newstore";
  public static final List<String> NEWSTORE_PARAMS = Arrays.asList(CLUSTER, NAME, OWNER);

  public static final String SETVERSION_PATH = "/setversion";
  public static final List<String> SETVERSION_PARAMS = Arrays.asList(CLUSTER, NAME, VERSION);;

  public static final String RESERVE_VERSION_PATH = "/reserveversion";
  public static final List<String> RESERVE_VERSION_PARAMS = Arrays.asList(CLUSTER, NAME, VERSION);;

  public static final String NEXTVERSION_PATH = "/nextversion";
  public static final List<String> NEXTVERSION_PARAMS = Arrays.asList(CLUSTER, NAME);

  public static final String CURRENT_VERSION_PATH = "/currentversion";
  public static final List<String> CURRENT_VERSION_PARAMS = Arrays.asList(CLUSTER, NAME);

  public static final String ACTIVE_VERSIONS_PATH = "/activeversions";
  public static final List<String> ACTIVE_VERSIONS_PARAMS = Arrays.asList(CLUSTER, NAME);

  public static final String JOB_PATH = "/job";
  public static final List<String> JOB_PARMAS = Arrays.asList(CLUSTER, NAME, VERSION);;

  public static final String LIST_STORES_PATH = "/liststores";
  public static final List<String> LIST_STORES_PARAMS = Arrays.asList(CLUSTER);


  private ControllerApiConstants(){}
}
