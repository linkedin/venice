package com.linkedin.venice;

public enum Command {

  LIST_STORES("list-stores",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER}),
  DESCRIBE_STORE("describe-store",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  DESCRIBE_STORES("describe-stores",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER}),
  NEXT_VERSION("next-version",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  CURRENT_VERSION("current-version",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  AVAILABLE_VERSIONS("available-versions",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE}),
  JOB_STATUS("job-status",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  NEW_STORE("new-store",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VERSION, Arg.KEY_SCHEMA, Arg.VALUE_SCHEMA}),
  SET_VERSION("set-version",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  ADD_SCHEMA("add-schema",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VALUE_SCHEMA}),
  LIST_STORAGE_NODES("list-storage-nodes",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER}),
  REPLICAS_OF_STORE("replicas-of-store",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORE, Arg.VERSION}),
  REPLICAS_ON_STORAGE_NODE("replicas-on-storage-node",
      new Arg[] {Arg.ROUTER, Arg.CLUSTER, Arg.STORAGE_NODE});

  private final String commandName;
  private final Arg[] requiredArgs;

  Command(String argName, Arg[] requiredArgs){
    this.commandName = argName;
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
    StringBuilder sb = new StringBuilder();
    sb.append("Required arguments: ");
    for (Arg arg : requiredArgs){
      sb.append(arg.toString());
      sb.append(", ");
    }
    return sb.toString();
  }
}
