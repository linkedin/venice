package com.linkedin.venice;

/**
 * Created by mwise on 5/23/16.
 */
public enum Arg {
  ROUTER("router"),
  CLUSTER("cluster"),
  STORE("store"),
  VERSION("version"),
  QUERY("query"),
  HELP("help");

  private final String argName;
  Arg(String argName){
    this.argName = argName;
  }

  @Override
  public String toString(){
    return argName;
  }

  public String first(){
    return argName.substring(0,1);
  }
}
