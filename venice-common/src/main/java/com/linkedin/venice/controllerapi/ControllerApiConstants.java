package com.linkedin.venice.controllerapi;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by mwise on 3/17/16.
 */
public class ControllerApiConstants {

  public static final String NAME = "storename";
  public static final String OWNER = "owner";
  public static final String STORE_SIZE = "store_size";
  public static final String VERSION = "version";
  public static final String STATUS = "status";
  public static final String ERROR = "error";

  public static final String CREATE_PATH = "/create";
  public static final List<String> CREATE_PARAMS = new ArrayList<>();
  static {
    CREATE_PARAMS.add(NAME);
    CREATE_PARAMS.add(STORE_SIZE);
    CREATE_PARAMS.add(OWNER);
  }

  public static final String SETVERSION_PATH = "/setversion";
  public static final List<String> SETVERSION_PARAMS = new ArrayList<>();
  static{
    SETVERSION_PARAMS.add(NAME);
    SETVERSION_PARAMS.add(VERSION);
  }


  public static final String JOB_PATH = "/job";
  public static final List<String> JOB_PARMAS = new ArrayList<>();
  static {
    JOB_PARMAS.add(NAME);
    JOB_PARMAS.add(VERSION);
  }

  private ControllerApiConstants(){}
}
