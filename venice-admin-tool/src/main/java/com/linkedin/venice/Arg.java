package com.linkedin.venice;

/**
 * Created by mwise on 5/23/16.
 */
public enum Arg {
  ROUTER("router", "r"),
  CLUSTER("cluster", "c"),
  STORE("store", "s"),
  VERSION("version", "v"),
  QUERY("query", "q"),
  HELP("help", "h"),
  NEW("new", "n"),
  KEY_SCHEMA("key-schema-file", "k"),
  VALUE_SCHEMA("value-schema-file", "l"),
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
