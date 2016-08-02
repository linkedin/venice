package com.linkedin.venice;

/**
 * Created by mwise on 5/23/16.
 */
public enum Arg {

  ROUTER("router", "r"),
  CLUSTER("cluster", "c"),
  STORE("store", "s"),
  VERSION("version", "v"),
  HELP("help", "h"),
  KEY_SCHEMA("key-schema-file", "ks"),
  VALUE_SCHEMA("value-schema-file", "vs"),
  OWNER("owner", "o");

  private final String argName;
  private final String first;
  Arg(String argName, String first){
    this.argName = argName;
    this.first = first;
  }

  @Override
  public String toString(){
    return argName;
  }

  public String first(){
    return first;
  }
}
