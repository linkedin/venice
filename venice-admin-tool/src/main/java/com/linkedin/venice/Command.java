package com.linkedin.venice;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;

import static com.linkedin.venice.Arg.*;

/**
 * TODO: Merge this with {@link com.linkedin.venice.controllerapi.ControllerRoute}
 */
public enum Command {

  LIST_STORES("list-stores", "",
      new Arg[] {URL, CLUSTER}, new Arg[] {INCLUDE_SYSTEM_STORES}),
  DESCRIBE_STORE("describe-store", "",
      new Arg[] {URL, CLUSTER, STORE}),
  DESCRIBE_STORES("describe-stores", "",
      new Arg[] {URL, CLUSTER}, new Arg[] {INCLUDE_SYSTEM_STORES}),
  DISABLE_STORE_WRITE("disable-store-write", "Prevent a store from accepting new versions",
      new Arg[] {URL, CLUSTER, STORE}),
  ENABLE_STORE_WRITE("enable-store-write", "Allow a store to accept new versions again after being writes have been disabled",
      new Arg[] {URL, CLUSTER, STORE}),
  DISABLE_STORE_READ("disable-store-read", "Prevent a store from serving read requests",
      new Arg[] {URL, CLUSTER, STORE}),
  ENABLE_STORE_READ("enable-store-read", "Allow a store to serve read requests again after reads have been disabled",
      new Arg[] {URL, CLUSTER, STORE}),
  DISABLE_STORE("disable-store", "Disable store in both read and write path",
      new Arg[] {URL, CLUSTER, STORE}),
  ENABLE_STORE("enable-store", "Enable a store in both read and write path",
      new Arg[] {URL, CLUSTER, STORE}),
  JOB_STATUS("job-status", "Query the ingest status of a running push job",
      new Arg[] {URL, CLUSTER, STORE, VERSION}),
  KILL_JOB("kill-job", "Kill a running push job",
      new Arg[] {URL, CLUSTER, STORE, VERSION}),
  SKIP_ADMIN("skip-admin", "Skip an admin message",
      new Arg[] {URL, CLUSTER, OFFSET},
      new Arg[] {SKIP_DIV}),
  NEW_STORE("new-store", "",
      new Arg[]{URL, CLUSTER, STORE, KEY_SCHEMA, VALUE_SCHEMA},
      new Arg[]{OWNER, VSON_STORE}),
  DELETE_STORE("delete-store", "Delete the given store including both metadata and all versions in this store",
      new Arg[]{URL, CLUSTER, STORE}),
  SET_VERSION("set-version", "Set the version that will be served",
      new Arg[] {URL, CLUSTER, STORE, VERSION}),
  ADD_SCHEMA("add-schema", "",
      new Arg[] {URL, CLUSTER, STORE, VALUE_SCHEMA}),
  ADD_DERIVED_SCHEMA("add-derived-schema", "",
      new Arg[] {URL, CLUSTER, STORE, VALUE_SCHEMA_ID, DERIVED_SCHEMA}),
  REMOVE_DERIVED_SCHEMA("remove-derived-schema", "remove derived schema for a given store by the value and derived schema Ids",
      new Arg[] {URL, CLUSTER, STORE, VALUE_SCHEMA_ID, DERIVED_SCHEMA_ID}),
  LIST_STORAGE_NODES("list-storage-nodes", "",
      new Arg[] {URL, CLUSTER}),
  CLUSTER_HEALTH_INSTANCES("cluster-health-instances", "List the status for every instance",
      new Arg[]{URL, CLUSTER}),
  CLUSTER_HEALTH_STORES("cluster-health-stores", "List the status for every store",
      new Arg[]{URL, CLUSTER}),
  NODE_REMOVABLE("node-removable", "A node is removable if all replicas it is serving are available on other nodes",
      new Arg[] {URL, CLUSTER, STORAGE_NODE}),
  WHITE_LIST_ADD_NODE("white-list-add-node", "Add a storage node into the white list",
      new Arg[]{URL, CLUSTER, STORAGE_NODE}),
  WHITE_LIST_REMOVE_NODE("white-list-remove-node", "Remove a storage node from the white list",
      new Arg[]{URL, CLUSTER, STORAGE_NODE}),
  REMOVE_NODE("remove-node", "Remove a storage node from the cluster",
      new Arg[]{URL, CLUSTER, STORAGE_NODE}),
  REPLICAS_OF_STORE("replicas-of-store", "List the location and status of all replicas for a store",
      new Arg[] {URL, CLUSTER, STORE, VERSION}),
  REPLICAS_ON_STORAGE_NODE("replicas-on-storage-node", "List the store and status of all replicas on a storage node",
      new Arg[] {URL, CLUSTER, STORAGE_NODE}),
  QUERY("query", "Query a store that has a simple key schema",
      new Arg[] {URL, CLUSTER, STORE, KEY},
      new Arg[]{VSON_STORE, VENICE_CLIENT_SSL_CONFIG_FILE}),
  SHOW_SCHEMAS("schemas", "Show the key and value schemas for a store",
      new Arg[] {URL, CLUSTER, STORE}),
  DELETE_ALL_VERSIONS("delete-all-versions", "Delete all versions in given store",
      new Arg[]{URL, CLUSTER, STORE}),
  DELETE_OLD_VERSION("delete-old-version", "Delete the given version(non current version) in the given store",
      new Arg[]{URL, CLUSTER, STORE, VERSION}),
  GET_EXECUTION("get-execution", "Get the execution status for an async admin command",
      new Arg[]{URL, CLUSTER, EXECUTION}),
  SET_OWNER("set-owner", "Update owner info of an existing store",
      new Arg[] {URL, CLUSTER, STORE, OWNER}),
  SET_PARTITION_COUNT("set-partition-count", "Update the number of partitions of an existing store",
      new Arg[] {URL, CLUSTER, STORE, PARTITION_COUNT}),
  UPDATE_STORE("update-store","update store metadata",
      new Arg[] {URL, CLUSTER, STORE},
      new Arg[] {OWNER, VERSION, LARGEST_USED_VERSION_NUMBER, PARTITION_COUNT, PARTITIONER_CLASS, PARTITIONER_PARAMS,
          AMPLIFICATION_FACTOR, READABILITY, WRITEABILITY, STORAGE_QUOTA, HYBRID_STORE_OVERHEAD_BYPASS,
          READ_QUOTA, HYBRID_REWIND_SECONDS, HYBRID_OFFSET_LAG, HYBRID_TIME_LAG, HYBRID_DATA_REPLICATION_POLICY,
          HYBRID_BUFFER_REPLAY_POLICY, ACCESS_CONTROL, COMPRESSION_STRATEGY, CLIENT_DECOMPRESSION_ENABLED,
          CHUNKING_ENABLED, BATCH_GET_LIMIT, NUM_VERSIONS_TO_PRESERVE, WRITE_COMPUTATION_ENABLED,
          READ_COMPUTATION_ENABLED, LEADER_FOLLOWER_MODEL_ENABLED, BACKUP_STRATEGY, AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED,
          INCREMENTAL_PUSH_ENABLED, BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOUR, HYBRID_STORE_DISK_QUOTA_ENABLED,
          REGULAR_VERSION_ETL_ENABLED, FUTURE_VERSION_ETL_ENABLED, ETLED_PROXY_USER_ACCOUNT, NATIVE_REPLICATION_ENABLED, PUSH_STREAM_SOURCE_ADDRESS,
          INCREMENTAL_PUSH_POLICY, BACKUP_VERSION_RETENTION_DAY, REPLICATION_FACTOR, NATIVE_REPLICATION_SOURCE_FABRIC, REPLICATE_ALL_CONFIGS,
          ACTIVE_ACTIVE_REPLICATION_ENABLED, REGIONS_FILTER, APPLY_TARGET_VERSION_FILTER_FOR_INC_PUSH}),
  EMPTY_PUSH("empty-push", "Do an empty push into an existing store",
      new Arg[]{URL, CLUSTER, STORE, PUSH_ID, STORE_SIZE}),
  ENABLE_THROTTLING("enable-throttling", "Enable the feature that throttling read request on all routers",
      new Arg[]{URL, CLUSTER}),
  DISABLE_THROTTLING("disable-throttling", "Disable the feature that throttling read request on all routers",
      new Arg[]{URL, CLUSTER}),
  ENABLE_MAX_CAPACITY_PROTECTION("enable-max-capacity-protection",
      "Enable the feature that prevent read request usage exceeding the max capacity on all routers",
      new Arg[]{URL, CLUSTER}),
  DISABLE_MAX_CAPACITY_PROTECTION("disable-max-capacity-protection",
      "Disable the feature that prevent read request usage exceeding the max capacity on all routers.",
      new Arg[]{URL, CLUSTER}),
  ENABLE_QUTOA_REBALANCE("enable-quota-rebalance",
      "Enable the feature that quota could be rebalanced once live router count is changed on all routers",
      new Arg[]{URL, CLUSTER}),
  DISABLE_QUTOA_REBALANCE("disable-quota-rebalance",
      "Disable the feature that quota could be rebalanced once live router count is changed on all routers",
      new Arg[]{URL, CLUSTER, EXPECTED_ROUTER_COUNT}),
  GET_ROUTERS_CLUSTER_CONFIG("get-routers-cluster-config", "Get cluster level router's config",
      new Arg[]{URL, CLUSTER}),
  CONVERT_VSON_SCHEMA("convert-vson-schema", "Convert and print out Avro schemas based on input Vson schemas",
      new Arg[] {KEY_SCHEMA, VALUE_SCHEMA}),
  GET_ALL_MIGRATION_PUSH_STRATEGIES("get-all-migration-push-strategies",
      "Get migration push strategies for all the voldemort stores",
      new Arg[] {URL, CLUSTER}),
  GET_MIGRATION_PUSH_STRATEGY("get-migration-push-strategy",
      "Get migration push strategy for the specified voldemort store",
      new Arg[] {URL, CLUSTER, VOLDEMORT_STORE}),
  SET_MIGRATION_PUSH_STRATEGY("set-migration-push-strategy",
      "Setup migration push strategy for the specified voldemort store",
      new Arg[] {URL, CLUSTER, VOLDEMORT_STORE, MIGRATION_PUSH_STRATEGY}),
  LIST_BOOTSTRAPPING_VERSIONS("list-bootstrapping-versions",
      "List all versions which have at least one bootstrapping replica",
      new Arg[]{URL, CLUSTER}),
  DELETE_KAFKA_TOPIC("delete-kafka-topic",
      "Delete a Kafka topic directly (without interaction with the Venice Controller",
      new Arg[]{KAFKA_BOOTSTRAP_SERVERS, KAFKA_ZOOKEEPER_CONNECTION_URL, KAFKA_TOPIC_NAME},
      new Arg[]{KAFKA_OPERATION_TIMEOUT, KAFKA_CONSUMER_CONFIG_FILE}),
  START_MIRROR_MAKER("start-kafka-mirror-maker",
      "Start a local Mirror Maker process, to forklift data between Kafka clusters.",
      new Arg[]{KAFKA_ZOOKEEPER_CONNECTION_URL_SOURCE, KAFKA_ZOOKEEPER_CONNECTION_URL_DESTINATION,
          KAFKA_BOOTSTRAP_SERVERS_DESTINATION, KAFKA_TOPIC_WHITELIST},
      new Arg[]{KAFKA_CONSUMER_CONFIG_FILE, KAFKA_PRODUCER_CONFIG_FILE}),
  DUMP_ADMIN_MESSAGES("dump-admin-messages",
      "Dump admin messages",
      new Arg[] {CLUSTER, KAFKA_BOOTSTRAP_SERVERS, STARTING_OFFSET, MESSAGE_COUNT, KAFKA_CONSUMER_CONFIG_FILE}),
  DUMP_CONTROL_MESSAGES("dump-control-messages", "Dump control messages in a partition",
      new Arg[] {KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_CONFIG_FILE, KAFKA_TOPIC_NAME, KAFKA_TOPIC_PARTITION, STARTING_OFFSET, MESSAGE_COUNT}),
  DUMP_KAFKA_TOPIC("dump-kafka-topic", "Dump a Kafka topic for a Venice cluster.  If start offset and message count are not specified, the entire partition will be dumped.  PLEASE REFRAIN FROM USING SERVER CERTIFICATES, IT IS A GDPR VIOLATION, GET ADDED TO THE STORE ACL'S OR GET FAST ACCESS TO THE KAFKA TOPIC!!",
      new Arg[] {KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_CONFIG_FILE, KAFKA_TOPIC_NAME, CLUSTER, URL}),
  MIGRATE_STORE("migrate-store", "Migrate store from one cluster to another within the same fabric",
      new Arg[] {URL, STORE, CLUSTER_SRC, CLUSTER_DEST}),
  MIGRATION_STATUS("migration-status", "Get store migration status",
      new Arg[] {URL, STORE, CLUSTER_SRC, CLUSTER_DEST}),
  COMPLETE_MIGRATION("complete-migration", "Update cluster discovery in a fabric",
      new Arg[] {URL, STORE, CLUSTER_SRC, CLUSTER_DEST, FABRIC}),
  ABORT_MIGRATION("abort-migration", "Kill store migration task and revert to previous state",
      new Arg[] {URL, STORE, CLUSTER_SRC, CLUSTER_DEST},
      new Arg[] {FORCE}),
  END_MIGRATION("end-migration", "Send this command to delete the original store",
      new Arg[] {URL, STORE, CLUSTER_SRC, CLUSTER_DEST}),
  SEND_END_OF_PUSH("send-end-of-push", "Send this message after Samza grandfathering job to close offline batch push",
      new Arg[] {URL, CLUSTER, STORE, VERSION}),
  LIST_LF_STORES("list-lf-stores", "list all stores that use leader/follower model",
      new Arg[] {URL, CLUSTER}),
  ENABLE_LF_MODEL("enable-lf-model", "enable leader/follower model for certain stores based on the param",
      new Arg[] {URL, CLUSTER, STORE_TYPE}),
  DISABLE_LF_MODEL("disable-lf-model", "disable leader/follower model for certain stores based on the param",
      new Arg[] {URL, CLUSTER, STORE_TYPE}),
  NEW_ZK_SHARED_STORE("new-zk-shared-store", "Create a new store object to be shared by multiple stores with the same prefix",
      new Arg[] {URL, CLUSTER, STORE},
      new Arg[] {OWNER, DEFAULT_CONFIGS}),
  NEW_ZK_SHARED_STORE_VERSION("new-zk-shared-store-version", "Create a new store version for an existing Zk shared store",
      new Arg[] {URL, CLUSTER, STORE}),
  MATERIALIZE_METADATA_STORE_VERSION("materialize-metadata-store-version", "Materialize the metadata system store for an existing Venice store",
      new Arg[] {URL, CLUSTER, STORE, VERSION}),
  DEMATERIALIZE_METADATA_STORE_VERSION("dematerialize-metadata-store-version", "Dematerialize the metadata system store for an existing Venice store",
      new Arg[] {URL, CLUSTER, STORE, VERSION}),
  CREATE_DAVINCI_PUSH_STATUS_STORE("create-davinci-push-status-store", "Create a Da Vinci push status store for an existing Venice store",
      new Arg[] {URL, CLUSTER, STORE}),
  DELETE_DAVINCI_PUSH_STATUS_STORE("delete-davinci-push-status-store", "Delete the Da Vinci push status store for an existing Venice store",
      new Arg[] {URL, CLUSTER, STORE}),
  NEW_STORE_ACL("new-store-acl", "Create a new store with ACL permissions set",
      new Arg[]{URL, CLUSTER, STORE, KEY_SCHEMA, VALUE_SCHEMA, ACL_PERMS},
      new Arg[]{OWNER, VSON_STORE}),
  UPDATE_STORE_ACL("update-store-acl", "Update ACL's for an existing store",
      new Arg[] {URL, CLUSTER, STORE, ACL_PERMS}),
  GET_STORE_ACL("get-store-acl", "Get ACL's for an existing store",
      new Arg[] {URL, CLUSTER, STORE}),
  DELETE_STORE_ACL("delete-store-acl", "Delete ACL's for an existing store",
      new Arg[] {URL, CLUSTER, STORE}),
  ADD_TO_STORE_ACL("add-to-store-acl", "Add a principal to ACL's for an existing store",
      new Arg[] {URL, CLUSTER, STORE, PRINCIPAL}, new Arg[] {READABILITY, WRITEABILITY}),
  REMOVE_FROM_STORE_ACL("remove-from-store-acl", "Remove a principal from ACL's for an existing store",
      new Arg[] {URL, CLUSTER, STORE, PRINCIPAL}, new Arg[] {READABILITY, WRITEABILITY}),
  ENABLE_NATIVE_REPLICATION_FOR_CLUSTER("enable-native-replication-for-cluster", "enable native replication for certain stores (batch-only, hybrid-only, incremental-push, hybrid-or-incremental, all) in a cluster",
      new Arg[] {URL, CLUSTER, STORE_TYPE}, new Arg[] {REGIONS_FILTER, NATIVE_REPLICATION_SOURCE_FABRIC}),
  DISABLE_NATIVE_REPLICATION_FOR_CLUSTER("disable-native-replication-for-cluster", "disable native replication for certain stores (batch-only, hybrid-only, incremental-push, hybrid-or-incremental, all) in a cluster",
      new Arg[] {URL, CLUSTER, STORE_TYPE}, new Arg[] {REGIONS_FILTER, NATIVE_REPLICATION_SOURCE_FABRIC}),
  ENABLE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER("enable-active-active-replication-for-cluster", "enable active active replication for certain stores (batch-only, hybrid-only, incremental-push, hybrid-or-incremental, all) in a cluster",
      new Arg[] {URL, CLUSTER, STORE_TYPE}, new Arg[] {REGIONS_FILTER}),
  DISABLE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER("disable-active-active-replication-for-cluster", "disable active active replication for certain stores (batch-only, hybrid-only, incremental-push, hybrid-or-incremental, all) in a cluster",
      new Arg[] {URL, CLUSTER, STORE_TYPE}, new Arg[] {REGIONS_FILTER});
  private final String commandName;
  private final String description;
  private final Arg[] requiredArgs;
  private final Arg[] optionalArgs;

  Command(String argName, String description, Arg[] requiredArgs){
    this(argName, description, requiredArgs, new Arg[] {});
  }

  Command(String argName, String description, Arg[] requiredArgs, Arg[] optionalArgs) {
    this.commandName = argName;
    this.description = description;
    this.requiredArgs = requiredArgs;
    this.optionalArgs = optionalArgs;
  }

  @Override
  public String toString(){
    return commandName;
  }

  public Arg[] getRequiredArgs(){
    return requiredArgs;
  }

  public Arg[] getOptionalArgs() {
    return optionalArgs;
  }

  public String getDesc(){
    StringJoiner sj = new StringJoiner("");
    if (!description.isEmpty()){
      sj.add(description);
      sj.add(". ");
    }

    StringJoiner requiredArgs = new StringJoiner("\n");
    for (Arg arg : getRequiredArgs()){
      requiredArgs.add("--" + arg.toString());
    }

    sj.add("Requires: \n" + requiredArgs);

    StringJoiner optionalArgs = new StringJoiner("\n");
    for (Arg arg : getOptionalArgs()) {
      optionalArgs.add("--" + arg.toString());
    }

    if (getOptionalArgs().length > 0) {
      sj.add("\nOptional args: \n" + optionalArgs.toString());
    }

    return sj.toString();
  }

  public static Comparator<Command> commandComparator = new Comparator<Command>() {
    public int compare(Command c1,Command c2) {
      return c1.commandName.compareTo(c2.commandName);
    }
  };

  public static Command getCommand(String name, CommandLine cmdLine) {
    for (Command cmd: values()) {
      if (cmd.commandName.equals(name)) {
        return cmd;
      }
    }
    if (null == name) {
      List<String> candidateCommands = Arrays.stream(Command.values())
          .filter(command -> Arrays.stream(command.getRequiredArgs())
              .allMatch(arg -> cmdLine.hasOption(arg.toString())))
          .map(commmand -> "--" + commmand.toString())
          .collect(Collectors.toList());
      if (!candidateCommands.isEmpty()) {
        throw new VeniceException("No command found, potential commands compatible with the provided parameters include: "
            + Arrays.toString(candidateCommands.toArray()));
      }
    }
    String message = name == null ? " No command found, Please specify a command, eg [--describe-store] " : "No Command found with name: " + name;
    throw new VeniceException(message);
  }
}
