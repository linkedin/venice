package com.linkedin.venice;

public enum Arg {

  ROUTER("router", "r", true, "Venice router url, eg. http://localhost:1689"),
  CLUSTER("cluster", "c", true, "Name of Venice cluster"),
  STORE("store", "s", true, "Name of Venice store"),
  VERSION("version", "v", true, "Venice store version number"),
  KEY_SCHEMA("key-schema-file", "ks", true, "Path to text file with key schema"),
  VALUE_SCHEMA("value-schema-file", "vs", true, "Path to text file with value schema"),
  OWNER("owner", "o", true, "Owner email for new store creation"),
  STORAGE_NODE("storage-node", "n", true, "Helix instance ID for a storage node, eg. lva1-app1234_1690"),
  KEY("key", "k", true, "Plain-text key for identifying a record in a store"),

  FILTER_JSON("filter-json", "f", true, "Comma-delimited list of fields to display from the json output.  Omit to display all fields"),
  FLAT_JSON("flat-json", "fj", false, "Display output as flat json, without pretty-print indentation and line breaks"),
  HELP("help", "h", false, "Show usage");


  private final String argName;
  private final String first;
  private final boolean parameterized;
  private final String helpText;

  Arg(String argName, String first, boolean parameterized, String helpText){
    this.argName = argName;
    this.first = first;
    this.parameterized = parameterized;
    this.helpText = helpText;
  }

  @Override
  public String toString(){
    return argName;
  }

  public String first(){
    return first;
  }

  public String getHelpText(){
    return helpText;
  }

  public boolean isParameterized(){
    return parameterized;
  }
}
