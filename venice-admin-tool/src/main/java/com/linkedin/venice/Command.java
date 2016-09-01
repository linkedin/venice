package com.linkedin.venice;

import java.util.StringJoiner;

public enum Command {

  LIST_STORES("list-stores", "",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER}),
  DESCRIBE_STORE("describe-store", "",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  DESCRIBE_STORES("describe-stores", "",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER}),
  PAUSE_STORE("pause-store", "Prevent a store from accepting new versions",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  RESUME_STORE("resume-store", "Allow a store to accept new versions again after being paused",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  NEXT_VERSION("next-version", "Show the next version available to be added to a store",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  CURRENT_VERSION("current-version", "Show the version currently being served",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  AVAILABLE_VERSIONS("available-versions", "Show all versions that are active",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  JOB_STATUS("job-status", "Query the ingest status of a running push job",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  NEW_STORE("new-store", "",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VERSION, Arg.KEY_SCHEMA, Arg.VALUE_SCHEMA}),
  SET_VERSION("set-version", "Set the version that will be served",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  ADD_SCHEMA("add-schema", "",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VALUE_SCHEMA}),
  LIST_STORAGE_NODES("list-storage-nodes", "",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER}),
  NODE_REMOVABLE("node-removable", "A node is removable if all replicas it is serving are available on other nodes",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORAGE_NODE}),
  REPLICAS_OF_STORE("replicas-of-store", "List the location and status of all replicas for a store",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  REPLICAS_ON_STORAGE_NODE("replicas-on-storage-node", "List the store and status of all replicas on a storage node",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORAGE_NODE}),
  QUERY("query", "Query a store that has a simple key schema",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.KEY});

  private final String commandName;
  private final String description;
  private final Arg[] requiredArgs;

  Command(String argName, String description, Arg[] requiredArgs){
    this.commandName = argName;
    this.description = description;
    this.requiredArgs = requiredArgs;
  }

  @Override
  public String toString(){
    return commandName;
  }

  public Arg[] getRequiredArgs(){
    return requiredArgs;
  }

  public String getDesc(){
    StringJoiner sj = new StringJoiner(".  ");
    if (!description.isEmpty()){
      sj.add(description);
    }

    StringJoiner arguments = new StringJoiner(", ");
    for (Arg arg : getRequiredArgs()){
      arguments.add("--" + arg.toString());
    }

    sj.add("Requires: " + arguments.toString());
    return sj.toString();
  }
}
