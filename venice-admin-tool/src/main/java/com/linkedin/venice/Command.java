package com.linkedin.venice;

import java.util.StringJoiner;

public enum Command {

  LIST_STORES("list-stores", "",
      new Arg[] {Arg.URL, Arg.CLUSTER}),
  DESCRIBE_STORE("describe-store", "",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE}),
  DESCRIBE_STORES("describe-stores", "",
      new Arg[] {Arg.URL, Arg.CLUSTER}),
  DISABLE_STORE_WRITE("disable-store-write", "Prevent a store from accepting new versions",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE}),
  ENABLE_STORE_WRITE("enable-store-write", "Allow a store to accept new versions again after being writes have been disabled",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE}),
  DISABLE_STORE_READ("disable-store-read", "Prevent a store from serving read requests",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE}),
  ENABLE_STORE_READ("enable-store-read", "Allow a store to serve read requests again after reads have been disabled",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE}),
  DISABLE_STORE("disable-store", "Disable store in both read and write path.",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE}),
  ENABLE_STORE("enable-store", "Enable a store in both read and write path",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE}),
  JOB_STATUS("job-status", "Query the ingest status of a running push job",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  KILL_JOB("kill-job", "Kill a running push job",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  SKIP_ADMIN("skip-admin", "Skip an admin message",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.OFFSET}),
  NEW_STORE("new-store", "",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.OWNER, Arg.KEY_SCHEMA, Arg.VALUE_SCHEMA}),
  SET_VERSION("set-version", "Set the version that will be served",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  ADD_SCHEMA("add-schema", "",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.VALUE_SCHEMA}),
  LIST_STORAGE_NODES("list-storage-nodes", "",
      new Arg[] {Arg.URL, Arg.CLUSTER}),
  CLUSTER_HEALTH_INSTANCES("cluster-health-instances", "List the status for every instance",
      new Arg[]{Arg.URL, Arg.CLUSTER}),
  CLUSTER_HEALTH_STORES("cluster-health-stores", "List the status for every store", new Arg[]{Arg.URL, Arg.CLUSTER}),
  NODE_REMOVABLE("node-removable", "A node is removable if all replicas it is serving are available on other nodes",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORAGE_NODE}),
  WHITE_LIST_ADD_NODE("white-list-add-node", "Add a storage node into the white list.",
      new Arg[]{Arg.URL, Arg.CLUSTER, Arg.STORAGE_NODE}),
  WHITE_LIST_REMOVE_NODE("white-list-remove-node", "Remove a storage node from the white list.",
      new Arg[]{Arg.URL, Arg.CLUSTER, Arg.STORAGE_NODE}),
  REMOVE_NODE("remove-node", "Remove a storage node from the cluster",
      new Arg[]{Arg.URL, Arg.CLUSTER, Arg.STORAGE_NODE}),
  REPLICAS_OF_STORE("replicas-of-store", "List the location and status of all replicas for a store",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  REPLICAS_ON_STORAGE_NODE("replicas-on-storage-node", "List the store and status of all replicas on a storage node",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORAGE_NODE}),
  QUERY("query", "Query a store that has a simple key schema",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.KEY}),
  DELETE_ALL_VERSIONS("delete-all-versions", "Delete all versions in given store.",
      new Arg[]{Arg.URL, Arg.CLUSTER, Arg.STORE}),
  GET_EXECUTION("get-execution", "Get the execution status for an async admin command.",
      new Arg[]{Arg.URL, Arg.CLUSTER, Arg.EXECUTION}),
  SET_OWNER("set-owner", "Update owner info of an existing store",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.OWNER}),
  SET_PARTITION_COUNT("set-partition-count", "Update the number of partitions of an existing store",
      new Arg[] {Arg.URL, Arg.CLUSTER, Arg.STORE, Arg.PARTITION_COUNT});

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
