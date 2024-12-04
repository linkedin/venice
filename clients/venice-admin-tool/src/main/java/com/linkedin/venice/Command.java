package com.linkedin.venice;

import static com.linkedin.venice.Arg.ACCESS_CONTROL;
import static com.linkedin.venice.Arg.ACL_PERMS;
import static com.linkedin.venice.Arg.ACTIVE_ACTIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.Arg.ALLOW_STORE_MIGRATION;
import static com.linkedin.venice.Arg.AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED;
import static com.linkedin.venice.Arg.BACKUP_FOLDER;
import static com.linkedin.venice.Arg.BACKUP_STRATEGY;
import static com.linkedin.venice.Arg.BACKUP_VERSION_RETENTION_DAY;
import static com.linkedin.venice.Arg.BASE_PATH;
import static com.linkedin.venice.Arg.BATCH_GET_LIMIT;
import static com.linkedin.venice.Arg.BLOB_TRANSFER_ENABLED;
import static com.linkedin.venice.Arg.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOUR;
import static com.linkedin.venice.Arg.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED;
import static com.linkedin.venice.Arg.CHUNKING_ENABLED;
import static com.linkedin.venice.Arg.CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.Arg.CLUSTER;
import static com.linkedin.venice.Arg.CLUSTER_DEST;
import static com.linkedin.venice.Arg.CLUSTER_LIST;
import static com.linkedin.venice.Arg.CLUSTER_SRC;
import static com.linkedin.venice.Arg.COMPRESSION_STRATEGY;
import static com.linkedin.venice.Arg.DATETIME;
import static com.linkedin.venice.Arg.DAVINCI_HEARTBEAT_REPORTED;
import static com.linkedin.venice.Arg.DEBUG;
import static com.linkedin.venice.Arg.DERIVED_SCHEMA;
import static com.linkedin.venice.Arg.DERIVED_SCHEMA_ID;
import static com.linkedin.venice.Arg.DEST_FABRIC;
import static com.linkedin.venice.Arg.DEST_ZK_SSL_CONFIG_FILE;
import static com.linkedin.venice.Arg.DEST_ZOOKEEPER_URL;
import static com.linkedin.venice.Arg.DISABLE_DAVINCI_PUSH_STATUS_STORE;
import static com.linkedin.venice.Arg.DISABLE_META_STORE;
import static com.linkedin.venice.Arg.ENABLE_DISABLED_REPLICA;
import static com.linkedin.venice.Arg.ENABLE_STORE_MIGRATION;
import static com.linkedin.venice.Arg.END_DATE;
import static com.linkedin.venice.Arg.ETLED_PROXY_USER_ACCOUNT;
import static com.linkedin.venice.Arg.EXECUTION;
import static com.linkedin.venice.Arg.EXPECTED_ROUTER_COUNT;
import static com.linkedin.venice.Arg.EXTRA_COMMAND_ARGS;
import static com.linkedin.venice.Arg.FABRIC;
import static com.linkedin.venice.Arg.FABRIC_A;
import static com.linkedin.venice.Arg.FABRIC_B;
import static com.linkedin.venice.Arg.FORCE;
import static com.linkedin.venice.Arg.FUTURE_VERSION_ETL_ENABLED;
import static com.linkedin.venice.Arg.GRAVEYARD_CLUSTERS;
import static com.linkedin.venice.Arg.HYBRID_BUFFER_REPLAY_POLICY;
import static com.linkedin.venice.Arg.HYBRID_DATA_REPLICATION_POLICY;
import static com.linkedin.venice.Arg.HYBRID_OFFSET_LAG;
import static com.linkedin.venice.Arg.HYBRID_REWIND_SECONDS;
import static com.linkedin.venice.Arg.HYBRID_STORE_DISK_QUOTA_ENABLED;
import static com.linkedin.venice.Arg.HYBRID_STORE_OVERHEAD_BYPASS;
import static com.linkedin.venice.Arg.HYBRID_TIME_LAG;
import static com.linkedin.venice.Arg.INCLUDE_SYSTEM_STORES;
import static com.linkedin.venice.Arg.INCREMENTAL_PUSH_ENABLED;
import static com.linkedin.venice.Arg.INFILE;
import static com.linkedin.venice.Arg.INSTANCES;
import static com.linkedin.venice.Arg.INTERVAL;
import static com.linkedin.venice.Arg.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.Arg.KAFKA_CONSUMER_CONFIG_FILE;
import static com.linkedin.venice.Arg.KAFKA_OPERATION_TIMEOUT;
import static com.linkedin.venice.Arg.KAFKA_TOPIC_LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.Arg.KAFKA_TOPIC_MIN_IN_SYNC_REPLICA;
import static com.linkedin.venice.Arg.KAFKA_TOPIC_NAME;
import static com.linkedin.venice.Arg.KAFKA_TOPIC_PARTITION;
import static com.linkedin.venice.Arg.KAFKA_TOPIC_RETENTION_IN_MS;
import static com.linkedin.venice.Arg.KEY;
import static com.linkedin.venice.Arg.KEY_SCHEMA;
import static com.linkedin.venice.Arg.LAG_FILTER_ENABLED;
import static com.linkedin.venice.Arg.LARGEST_USED_VERSION_NUMBER;
import static com.linkedin.venice.Arg.LATEST_SUPERSET_SCHEMA_ID;
import static com.linkedin.venice.Arg.MAX_COMPACTION_LAG_SECONDS;
import static com.linkedin.venice.Arg.MAX_NEARLINE_RECORD_SIZE_BYTES;
import static com.linkedin.venice.Arg.MAX_RECORD_SIZE_BYTES;
import static com.linkedin.venice.Arg.MESSAGE_COUNT;
import static com.linkedin.venice.Arg.MIGRATION_PUSH_STRATEGY;
import static com.linkedin.venice.Arg.MIN_COMPACTION_LAG_SECONDS;
import static com.linkedin.venice.Arg.NATIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.Arg.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.Arg.NEARLINE_PRODUCER_COMPRESSION_ENABLED;
import static com.linkedin.venice.Arg.NEARLINE_PRODUCER_COUNT_PER_WRITER;
import static com.linkedin.venice.Arg.NON_INTERACTIVE;
import static com.linkedin.venice.Arg.NUM_VERSIONS_TO_PRESERVE;
import static com.linkedin.venice.Arg.OFFSET;
import static com.linkedin.venice.Arg.OUTFILE;
import static com.linkedin.venice.Arg.OWNER;
import static com.linkedin.venice.Arg.PARTITION;
import static com.linkedin.venice.Arg.PARTITIONER_CLASS;
import static com.linkedin.venice.Arg.PARTITIONER_PARAMS;
import static com.linkedin.venice.Arg.PARTITION_COUNT;
import static com.linkedin.venice.Arg.PARTITION_DETAIL_ENABLED;
import static com.linkedin.venice.Arg.PRINCIPAL;
import static com.linkedin.venice.Arg.PROGRESS_INTERVAL;
import static com.linkedin.venice.Arg.PUSH_ID;
import static com.linkedin.venice.Arg.PUSH_STREAM_SOURCE_ADDRESS;
import static com.linkedin.venice.Arg.READABILITY;
import static com.linkedin.venice.Arg.READ_COMPUTATION_ENABLED;
import static com.linkedin.venice.Arg.READ_QUOTA;
import static com.linkedin.venice.Arg.RECOVERY_COMMAND;
import static com.linkedin.venice.Arg.RECOVER_CLUSTER;
import static com.linkedin.venice.Arg.REGIONS_FILTER;
import static com.linkedin.venice.Arg.REGULAR_VERSION_ETL_ENABLED;
import static com.linkedin.venice.Arg.REMOVE_VIEW;
import static com.linkedin.venice.Arg.REPAIR;
import static com.linkedin.venice.Arg.REPLICATE_ALL_CONFIGS;
import static com.linkedin.venice.Arg.REPLICATION_FACTOR;
import static com.linkedin.venice.Arg.RETRY;
import static com.linkedin.venice.Arg.RMD_CHUNKING_ENABLED;
import static com.linkedin.venice.Arg.SEPARATE_REALTIME_TOPIC_ENABLED;
import static com.linkedin.venice.Arg.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.Arg.SERVER_URL;
import static com.linkedin.venice.Arg.SKIP_DIV;
import static com.linkedin.venice.Arg.SKIP_LAST_STORE_CREATION;
import static com.linkedin.venice.Arg.SOURCE_FABRIC;
import static com.linkedin.venice.Arg.SRC_ZK_SSL_CONFIG_FILE;
import static com.linkedin.venice.Arg.SRC_ZOOKEEPER_URL;
import static com.linkedin.venice.Arg.STARTING_OFFSET;
import static com.linkedin.venice.Arg.START_DATE;
import static com.linkedin.venice.Arg.STORAGE_NODE;
import static com.linkedin.venice.Arg.STORAGE_NODE_READ_QUOTA_ENABLED;
import static com.linkedin.venice.Arg.STORAGE_PERSONA;
import static com.linkedin.venice.Arg.STORAGE_QUOTA;
import static com.linkedin.venice.Arg.STORE;
import static com.linkedin.venice.Arg.STORES;
import static com.linkedin.venice.Arg.STORE_SIZE;
import static com.linkedin.venice.Arg.STORE_TYPE;
import static com.linkedin.venice.Arg.STORE_VIEW_CONFIGS;
import static com.linkedin.venice.Arg.SYSTEM_STORE_TYPE;
import static com.linkedin.venice.Arg.TARGET_SWAP_REGION;
import static com.linkedin.venice.Arg.TARGET_SWAP_REGION_WAIT_TIME;
import static com.linkedin.venice.Arg.TO_BE_STOPPED_NODES;
import static com.linkedin.venice.Arg.UNUSED_SCHEMA_DELETION_ENABLED;
import static com.linkedin.venice.Arg.URL;
import static com.linkedin.venice.Arg.VALUE_SCHEMA;
import static com.linkedin.venice.Arg.VALUE_SCHEMA_ID;
import static com.linkedin.venice.Arg.VENICE_CLIENT_SSL_CONFIG_FILE;
import static com.linkedin.venice.Arg.VENICE_ZOOKEEPER_URL;
import static com.linkedin.venice.Arg.VERSION;
import static com.linkedin.venice.Arg.VIEW_CLASS;
import static com.linkedin.venice.Arg.VIEW_NAME;
import static com.linkedin.venice.Arg.VIEW_PARAMS;
import static com.linkedin.venice.Arg.VOLDEMORT_STORE;
import static com.linkedin.venice.Arg.VSON_STORE;
import static com.linkedin.venice.Arg.WRITEABILITY;
import static com.linkedin.venice.Arg.WRITE_COMPUTATION_ENABLED;
import static com.linkedin.venice.Arg.ZK_SSL_CONFIG_FILE;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;


/**
 * TODO: Merge this with {@link com.linkedin.venice.controllerapi.ControllerRoute}
 */
public enum Command {
  LIST_STORES(
      "list-stores", "List all stores present in the given cluster", new Arg[] { URL, CLUSTER },
      new Arg[] { INCLUDE_SYSTEM_STORES }
  ), DESCRIBE_STORE("describe-store", "Get store details", new Arg[] { URL, STORE }, new Arg[] { CLUSTER }),
  DESCRIBE_STORES("describe-stores", "", new Arg[] { URL, CLUSTER }, new Arg[] { INCLUDE_SYSTEM_STORES }),
  DISABLE_STORE_WRITE(
      "disable-store-write", "Prevent a store from accepting new versions", new Arg[] { URL, STORE },
      new Arg[] { CLUSTER }
  ),
  ENABLE_STORE_WRITE(
      "enable-store-write", "Allow a store to accept new versions again after being writes have been disabled",
      new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  DISABLE_STORE_READ(
      "disable-store-read", "Prevent a store from serving read requests", new Arg[] { URL, STORE },
      new Arg[] { CLUSTER }
  ),
  ENABLE_STORE_READ(
      "enable-store-read", "Allow a store to serve read requests again after reads have been disabled",
      new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  DISABLE_STORE(
      "disable-store", "Disable store in both read and write path", new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  ENABLE_STORE(
      "enable-store", "Enable a store in both read and write path", new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  JOB_STATUS(
      "job-status",
      "Query the ingest status of a running push job. If a version is not specified, the job status of the last job will be printed.",
      new Arg[] { URL, STORE }, new Arg[] { CLUSTER, VERSION }
  ), KILL_JOB("kill-job", "Kill a running push job", new Arg[] { URL, STORE, VERSION }, new Arg[] { CLUSTER }),
  SKIP_ADMIN("skip-admin", "Skip an admin message", new Arg[] { URL, CLUSTER, OFFSET }, new Arg[] { SKIP_DIV }),
  NEW_STORE(
      "new-store", "", new Arg[] { URL, CLUSTER, STORE, KEY_SCHEMA, VALUE_SCHEMA }, new Arg[] { OWNER, VSON_STORE }
  ),
  DELETE_STORE(
      "delete-store", "Delete the given store including both metadata and all versions in this store",
      new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  BACKFILL_SYSTEM_STORES(
      "backfill-system-stores", "Create system stores of a given type for user stores in a cluster",
      new Arg[] { URL, CLUSTER, SYSTEM_STORE_TYPE }
  ),
  SET_VERSION(
      "set-version", "Set the version that will be served", new Arg[] { URL, STORE, VERSION }, new Arg[] { CLUSTER }
  ), ADD_SCHEMA("add-schema", "", new Arg[] { URL, STORE, VALUE_SCHEMA }, new Arg[] { CLUSTER }),
  ADD_SCHEMA_TO_ZK(
      "add-schema-to-zk", "",
      new Arg[] { VENICE_ZOOKEEPER_URL, STORE, VALUE_SCHEMA, VALUE_SCHEMA_ID, ZK_SSL_CONFIG_FILE },
      new Arg[] { CLUSTER }
  ),
  ADD_DERIVED_SCHEMA(
      "add-derived-schema", "", new Arg[] { URL, STORE, VALUE_SCHEMA_ID, DERIVED_SCHEMA }, new Arg[] { CLUSTER }
  ),
  REMOVE_DERIVED_SCHEMA(
      "remove-derived-schema", "remove derived schema for a given store by the value and derived schema Ids",
      new Arg[] { URL, STORE, VALUE_SCHEMA_ID, DERIVED_SCHEMA_ID }, new Arg[] { CLUSTER }
  ), LIST_STORAGE_NODES("list-storage-nodes", "", new Arg[] { URL, CLUSTER }),
  CLUSTER_HEALTH_INSTANCES(
      "cluster-health-instances", "List the status for every instance", new Arg[] { URL, CLUSTER },
      new Arg[] { ENABLE_DISABLED_REPLICA }
  ), CLUSTER_HEALTH_STORES("cluster-health-stores", "List the status for every store", new Arg[] { URL, CLUSTER }),
  NODE_REMOVABLE(
      "node-removable", "A node is removable if all replicas it is serving are available on other nodes",
      new Arg[] { URL, CLUSTER, STORAGE_NODE }
  ),
  ALLOW_LIST_ADD_NODE(
      "allow-list-add-node", "Add a storage node into the allowlist", new Arg[] { URL, CLUSTER, STORAGE_NODE }
  ),
  ALLOW_LIST_REMOVE_NODE(
      "allow-list-remove-node", "Remove a storage node from the allowlist", new Arg[] { URL, CLUSTER, STORAGE_NODE }
  ), REMOVE_NODE("remove-node", "Remove a storage node from the cluster", new Arg[] { URL, CLUSTER, STORAGE_NODE }),
  REPLICAS_OF_STORE(
      "replicas-of-store", "List the location and status of all replicas for a store",
      new Arg[] { URL, STORE, VERSION }, new Arg[] { CLUSTER }
  ),
  REPLICAS_ON_STORAGE_NODE(
      "replicas-on-storage-node", "List the store and status of all replicas on a storage node",
      new Arg[] { URL, CLUSTER, STORAGE_NODE }
  ),
  QUERY(
      "query", "Query a store that has a simple key schema", new Arg[] { URL, STORE, KEY },
      new Arg[] { CLUSTER, VSON_STORE, VENICE_CLIENT_SSL_CONFIG_FILE }
  ),
  SHOW_SCHEMAS(
      "schemas", "Show the key and value schemas for a store", new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  DELETE_ALL_VERSIONS(
      "delete-all-versions", "Delete all versions in given store", new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  DELETE_OLD_VERSION(
      "delete-old-version", "Delete the given version(non current version) in the given store",
      new Arg[] { URL, STORE, VERSION }, new Arg[] { CLUSTER }
  ),
  GET_EXECUTION(
      "get-execution", "Get the execution status for an async admin command", new Arg[] { URL, CLUSTER, EXECUTION }
  ),
  SET_OWNER(
      "set-owner", "Update owner info of an existing store", new Arg[] { URL, STORE, OWNER }, new Arg[] { CLUSTER }
  ),
  SET_PARTITION_COUNT(
      "set-partition-count", "Update the number of partitions of an existing store",
      new Arg[] { URL, STORE, PARTITION_COUNT }, new Arg[] { CLUSTER }
  ),
  UPDATE_STORE(
      "update-store", "update store metadata", new Arg[] { URL, STORE },
      new Arg[] { CLUSTER, OWNER, VERSION, LARGEST_USED_VERSION_NUMBER, PARTITION_COUNT, PARTITIONER_CLASS,
          PARTITIONER_PARAMS, READABILITY, WRITEABILITY, STORAGE_QUOTA, STORAGE_NODE_READ_QUOTA_ENABLED,
          HYBRID_STORE_OVERHEAD_BYPASS, READ_QUOTA, HYBRID_REWIND_SECONDS, HYBRID_OFFSET_LAG, HYBRID_TIME_LAG,
          HYBRID_DATA_REPLICATION_POLICY, HYBRID_BUFFER_REPLAY_POLICY, ACCESS_CONTROL, COMPRESSION_STRATEGY,
          CLIENT_DECOMPRESSION_ENABLED, CHUNKING_ENABLED, RMD_CHUNKING_ENABLED, BATCH_GET_LIMIT,
          NUM_VERSIONS_TO_PRESERVE, WRITE_COMPUTATION_ENABLED, READ_COMPUTATION_ENABLED, BACKUP_STRATEGY,
          AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED, INCREMENTAL_PUSH_ENABLED, BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOUR,
          HYBRID_STORE_DISK_QUOTA_ENABLED, REGULAR_VERSION_ETL_ENABLED, FUTURE_VERSION_ETL_ENABLED,
          ETLED_PROXY_USER_ACCOUNT, NATIVE_REPLICATION_ENABLED, PUSH_STREAM_SOURCE_ADDRESS,
          BACKUP_VERSION_RETENTION_DAY, REPLICATION_FACTOR, NATIVE_REPLICATION_SOURCE_FABRIC, REPLICATE_ALL_CONFIGS,
          ACTIVE_ACTIVE_REPLICATION_ENABLED, REGIONS_FILTER, DISABLE_META_STORE, DISABLE_DAVINCI_PUSH_STATUS_STORE,
          STORAGE_PERSONA, STORE_VIEW_CONFIGS, LATEST_SUPERSET_SCHEMA_ID, MIN_COMPACTION_LAG_SECONDS,
          MAX_COMPACTION_LAG_SECONDS, MAX_RECORD_SIZE_BYTES, MAX_NEARLINE_RECORD_SIZE_BYTES,
          UNUSED_SCHEMA_DELETION_ENABLED, BLOB_TRANSFER_ENABLED, SEPARATE_REALTIME_TOPIC_ENABLED,
          NEARLINE_PRODUCER_COMPRESSION_ENABLED, NEARLINE_PRODUCER_COUNT_PER_WRITER, TARGET_SWAP_REGION,
          TARGET_SWAP_REGION_WAIT_TIME, DAVINCI_HEARTBEAT_REPORTED, ENABLE_STORE_MIGRATION }
  ),
  UPDATE_CLUSTER_CONFIG(
      "update-cluster-config", "Update live cluster configs", new Arg[] { URL, CLUSTER },
      new Arg[] { FABRIC, SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND, ALLOW_STORE_MIGRATION,
          CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED }
  ),
  EMPTY_PUSH(
      "empty-push", "Do an empty push into an existing store", new Arg[] { URL, STORE, PUSH_ID, STORE_SIZE },
      new Arg[] { CLUSTER }
  ),
  ENABLE_THROTTLING(
      "enable-throttling", "Enable the feature that throttling read request on all routers", new Arg[] { URL, CLUSTER }
  ),
  DISABLE_THROTTLING(
      "disable-throttling", "Disable the feature that throttling read request on all routers",
      new Arg[] { URL, CLUSTER }
  ),
  ENABLE_MAX_CAPACITY_PROTECTION(
      "enable-max-capacity-protection",
      "Enable the feature that prevent read request usage exceeding the max capacity on all routers",
      new Arg[] { URL, CLUSTER }
  ),
  DISABLE_MAX_CAPACITY_PROTECTION(
      "disable-max-capacity-protection",
      "Disable the feature that prevent read request usage exceeding the max capacity on all routers.",
      new Arg[] { URL, CLUSTER }
  ),
  ENABLE_QUTOA_REBALANCE(
      "enable-quota-rebalance",
      "Enable the feature that quota could be rebalanced once live router count is changed on all routers",
      new Arg[] { URL, CLUSTER }
  ),
  DISABLE_QUTOA_REBALANCE(
      "disable-quota-rebalance",
      "Disable the feature that quota could be rebalanced once live router count is changed on all routers",
      new Arg[] { URL, CLUSTER, EXPECTED_ROUTER_COUNT }
  ),
  GET_ROUTERS_CLUSTER_CONFIG(
      "get-routers-cluster-config", "Get cluster level router's config", new Arg[] { URL, CLUSTER }
  ),
  CONVERT_VSON_SCHEMA(
      "convert-vson-schema", "Convert and print out Avro schemas based on input Vson schemas",
      new Arg[] { KEY_SCHEMA, VALUE_SCHEMA }
  ),
  GET_ALL_MIGRATION_PUSH_STRATEGIES(
      "get-all-migration-push-strategies", "Get migration push strategies for all the voldemort stores",
      new Arg[] { URL, CLUSTER }
  ),
  GET_MIGRATION_PUSH_STRATEGY(
      "get-migration-push-strategy", "Get migration push strategy for the specified voldemort store",
      new Arg[] { URL, CLUSTER, VOLDEMORT_STORE }
  ),
  SET_MIGRATION_PUSH_STRATEGY(
      "set-migration-push-strategy", "Setup migration push strategy for the specified voldemort store",
      new Arg[] { URL, CLUSTER, VOLDEMORT_STORE, MIGRATION_PUSH_STRATEGY }
  ),
  LIST_BOOTSTRAPPING_VERSIONS(
      "list-bootstrapping-versions", "List all versions which have at least one bootstrapping replica",
      new Arg[] { URL, CLUSTER }
  ),
  DELETE_KAFKA_TOPIC(
      "delete-kafka-topic", "Delete a Kafka topic directly (without interaction with the Venice Controller",
      new Arg[] { KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_NAME },
      new Arg[] { KAFKA_OPERATION_TIMEOUT, KAFKA_CONSUMER_CONFIG_FILE }
  ),
  DUMP_ADMIN_MESSAGES(
      "dump-admin-messages", "Dump admin messages",
      new Arg[] { CLUSTER, KAFKA_BOOTSTRAP_SERVERS, STARTING_OFFSET, MESSAGE_COUNT, KAFKA_CONSUMER_CONFIG_FILE }
  ),
  DUMP_CONTROL_MESSAGES(
      "dump-control-messages", "Dump control messages in a partition",
      new Arg[] { KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_CONFIG_FILE, KAFKA_TOPIC_NAME, KAFKA_TOPIC_PARTITION,
          STARTING_OFFSET, MESSAGE_COUNT }
  ),
  DUMP_KAFKA_TOPIC(
      "dump-kafka-topic",
      "Dump a Kafka topic for a Venice cluster.  If start offset and message count are not specified, the entire partition will be dumped.  PLEASE REFRAIN FROM USING SERVER CERTIFICATES, IT IS A GDPR VIOLATION, GET ADDED TO THE STORE ACL'S OR GET FAST ACCESS TO THE KAFKA TOPIC!!",
      new Arg[] { KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_CONFIG_FILE, KAFKA_TOPIC_NAME, CLUSTER, URL }
  ),
  QUERY_KAFKA_TOPIC(
      "query-kafka-topic", "Query some specific keys from the Venice Topic",
      new Arg[] { KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_CONFIG_FILE, KAFKA_TOPIC_NAME, CLUSTER, URL, START_DATE,
          KEY },
      new Arg[] { END_DATE, PROGRESS_INTERVAL }
  ),
  MIGRATE_STORE(
      "migrate-store", "Migrate store from one cluster to another within the same fabric",
      new Arg[] { URL, STORE, CLUSTER_SRC, CLUSTER_DEST }
  ),
  MIGRATION_STATUS(
      "migration-status", "Get store migration status", new Arg[] { URL, STORE, CLUSTER_SRC, CLUSTER_DEST }
  ),
  COMPLETE_MIGRATION(
      "complete-migration", "Update cluster discovery in a fabric",
      new Arg[] { URL, STORE, CLUSTER_SRC, CLUSTER_DEST, FABRIC }
  ),
  ABORT_MIGRATION(
      "abort-migration", "Kill store migration task and revert to previous state",
      new Arg[] { URL, STORE, CLUSTER_SRC, CLUSTER_DEST }, new Arg[] { FORCE }
  ),
  END_MIGRATION(
      "end-migration", "Send this command to delete the original store",
      new Arg[] { URL, STORE, CLUSTER_SRC, CLUSTER_DEST }
  ),
  SEND_END_OF_PUSH(
      "send-end-of-push", "Send this message after Samza reprocessing job to close offline batch push",
      new Arg[] { URL, STORE, VERSION }, new Arg[] { CLUSTER }
  ),
  NEW_STORE_ACL(
      "new-store-acl", "Create a new store with ACL permissions set",
      new Arg[] { URL, STORE, KEY_SCHEMA, VALUE_SCHEMA, ACL_PERMS }, new Arg[] { CLUSTER, OWNER, VSON_STORE }
  ),
  UPDATE_STORE_ACL(
      "update-store-acl", "Update ACL's for an existing store", new Arg[] { URL, STORE, ACL_PERMS },
      new Arg[] { CLUSTER }
  ), GET_STORE_ACL("get-store-acl", "Get ACL's for an existing store", new Arg[] { URL, STORE }, new Arg[] { CLUSTER }),
  DELETE_STORE_ACL(
      "delete-store-acl", "Delete ACL's for an existing store", new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  ADD_TO_STORE_ACL(
      "add-to-store-acl", "Add a principal to ACL's for an existing store", new Arg[] { URL, STORE, PRINCIPAL },
      new Arg[] { CLUSTER, READABILITY, WRITEABILITY }
  ),
  REMOVE_FROM_STORE_ACL(
      "remove-from-store-acl", "Remove a principal from ACL's for an existing store",
      new Arg[] { URL, STORE, PRINCIPAL }, new Arg[] { CLUSTER, READABILITY, WRITEABILITY }
  ),
  ENABLE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER(
      "enable-active-active-replication-for-cluster",
      "enable active active replication for certain stores (batch-only, hybrid-only, incremental-push, hybrid-or-incremental, all) in a cluster",
      new Arg[] { URL, CLUSTER, STORE_TYPE }, new Arg[] { REGIONS_FILTER }
  ),
  DISABLE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER(
      "disable-active-active-replication-for-cluster",
      "disable active active replication for certain stores (batch-only, hybrid-only, incremental-push, hybrid-or-incremental, all) in a cluster",
      new Arg[] { URL, CLUSTER, STORE_TYPE }, new Arg[] { REGIONS_FILTER }
  ),
  GET_DELETABLE_STORE_TOPICS(
      "get-deletable-store-topics",
      "Get a list of deletable store topics in the fabric that belongs to the controller handling the request",
      new Arg[] { URL, CLUSTER }
  ),
  WIPE_CLUSTER(
      "wipe-cluster", "Delete data and metadata of a cluster/store/version in a child fabric",
      new Arg[] { URL, CLUSTER, FABRIC }, new Arg[] { STORE, VERSION }
  ),
  REPLICAS_READINESS_ON_STORAGE_NODE(
      "node-replicas-readiness", "Get the readiness of all current replicas on a storage node from a child controller",
      new Arg[] { URL, CLUSTER, STORAGE_NODE }
  ),
  COMPARE_STORE(
      "compare-store", "Compare a store between two fabrics", new Arg[] { URL, STORE, FABRIC_A, FABRIC_B },
      new Arg[] { CLUSTER }
  ),
  REPLICATE_META_DATA(
      "replicate-meta-data",
      "Copy a cluster's all stores schemas and store level configs from source fabric to destination fabric",
      new Arg[] { URL, CLUSTER, SOURCE_FABRIC, DEST_FABRIC }
  ),
  LIST_CLUSTER_STALE_STORES(
      "list-cluster-stale-stores", "List all stores in a cluster which have stale replicas.", new Arg[] { URL, CLUSTER }
  ),
  LIST_STORE_PUSH_INFO(
      "list-store-push-info", "List information about current pushes and push history for a specific store.",
      new Arg[] { URL, STORE }, new Arg[] { CLUSTER, PARTITION_DETAIL_ENABLED }
  ),
  GET_KAFKA_TOPIC_CONFIGS(
      "get-kafka-topic-configs", "Get configs of a topic through controllers", new Arg[] { URL, KAFKA_TOPIC_NAME }
  ),
  UPDATE_KAFKA_TOPIC_LOG_COMPACTION(
      "update-kafka-topic-log-compaction", "Update log compaction config of a topic through controllers",
      new Arg[] { URL, KAFKA_TOPIC_NAME, KAFKA_TOPIC_LOG_COMPACTION_ENABLED }, new Arg[] { CLUSTER }
  ),
  UPDATE_KAFKA_TOPIC_RETENTION(
      "update-kafka-topic-retention", "Update retention config of a topic through controllers",
      new Arg[] { URL, KAFKA_TOPIC_NAME, KAFKA_TOPIC_RETENTION_IN_MS }, new Arg[] { CLUSTER }
  ),
  UPDATE_KAFKA_TOPIC_MIN_IN_SYNC_REPLICA(
      "update-kafka-topic-min-in-sync-replica", "Update minISR of a topic through controllers",
      new Arg[] { URL, KAFKA_TOPIC_NAME, KAFKA_TOPIC_MIN_IN_SYNC_REPLICA }, new Arg[] { CLUSTER }
  ),
  START_FABRIC_BUILDOUT(
      "start-fabric-buildout",
      "Start building a cluster in destination fabric by copying stores metadata and data from source fabric",
      new Arg[] { URL, CLUSTER, SOURCE_FABRIC, DEST_FABRIC }, new Arg[] { RETRY }
  ),
  CHECK_FABRIC_BUILDOUT_STATUS(
      "check-fabric-buildout-status", "Check the status of cluster building in destination fabric",
      new Arg[] { URL, CLUSTER, SOURCE_FABRIC, DEST_FABRIC }
  ),
  END_FABRIC_BUILDOUT(
      "end-fabric-buildout", "End the building of a cluster in destination fabric",
      new Arg[] { URL, CLUSTER, SOURCE_FABRIC, DEST_FABRIC }
  ),
  NEW_STORAGE_PERSONA(
      "new-storage-persona", "Creates a new storage persona.",
      new Arg[] { URL, CLUSTER, STORAGE_PERSONA, STORAGE_QUOTA, STORE, OWNER }
  ),
  GET_STORAGE_PERSONA(
      "get-storage-persona", "Gets info on an existing storage persona by name",
      new Arg[] { URL, CLUSTER, STORAGE_PERSONA }
  ),
  DELETE_STORAGE_PERSONA(
      "delete-storage-persona", "Deletes an existing storage persona", new Arg[] { URL, CLUSTER, STORAGE_PERSONA }
  ),
  UPDATE_STORAGE_PERSONA(
      "update-storage-persona", "Updates an existing storage persona", new Arg[] { URL, CLUSTER, STORAGE_PERSONA },
      new Arg[] { STORAGE_QUOTA, STORE, OWNER }
  ),
  GET_STORAGE_PERSONA_FOR_STORE(
      "get-storage-persona-for-store", "Gets the storage persona associated with a store name.",
      new Arg[] { URL, STORE }, new Arg[] { CLUSTER }
  ),
  LIST_CLUSTER_STORAGE_PERSONAS(
      "list-cluster-storage-personas", "Lists all storage personas in a cluster.", new Arg[] { URL, CLUSTER }
  ),
  CLEANUP_INSTANCE_CUSTOMIZED_STATES(
      "cleanup-instance-customized-states", "Cleanup any lingering instance level customized states",
      new Arg[] { URL, CLUSTER }
  ),
  EXECUTE_DATA_RECOVERY(
      "execute-data-recovery", "Execute data recovery for a group of stores. ('--stores' overwrites '--cluster' value)",
      new Arg[] { URL, RECOVERY_COMMAND, SOURCE_FABRIC, DEST_FABRIC, DATETIME },
      new Arg[] { STORES, CLUSTER, EXTRA_COMMAND_ARGS, DEBUG, NON_INTERACTIVE }
  ),
  ESTIMATE_DATA_RECOVERY_TIME(
      "estimate-data-recovery-time",
      "Estimates the time it would take to execute data recovery for a group of stores. ('--stores' overwrites '--cluster' value)",
      new Arg[] { URL, DEST_FABRIC }, new Arg[] { STORES, CLUSTER }
  ),
  MONITOR_DATA_RECOVERY(
      "monitor-data-recovery",
      "Monitor data recovery progress for a group of stores. ('--stores' overwrites '--cluster' value)",
      new Arg[] { URL, DEST_FABRIC, DATETIME }, new Arg[] { STORES, CLUSTER, INTERVAL }
  ),
  REQUEST_BASED_METADATA(
      "request-based-metadata",
      "Get the store's metadata using request based metadata endpoint via a transport client and a server URL",
      new Arg[] { URL, SERVER_URL, STORE }
  ),
  DUMP_INGESTION_STATE(
      "dump-ingestion-state",
      "Dump the real-time ingestion state for a certain store version in a certain storage node",
      new Arg[] { SERVER_URL, STORE, VERSION }, new Arg[] { PARTITION }
  ),
  DUMP_TOPIC_PARTITION_INGESTION_CONTEXT(
      "dump-topic-partition-ingestion-context",
      "Dump the topic partition ingestion context belong to a certain store version in a certain storage node",
      new Arg[] { SERVER_URL, STORE, VERSION, KAFKA_TOPIC_NAME, KAFKA_TOPIC_PARTITION }
  ),
  CONFIGURE_STORE_VIEW(
      "configure-store-view", "Configure store view of a certain store", new Arg[] { URL, STORE, VIEW_NAME },
      new Arg[] { CLUSTER, VIEW_CLASS, VIEW_PARAMS, REMOVE_VIEW }
  ),
  RECOVER_STORE_METADATA(
      "recover-store-metadata", "Recover store metadata in EI",
      new Arg[] { URL, STORE, VENICE_ZOOKEEPER_URL, KAFKA_BOOTSTRAP_SERVERS, GRAVEYARD_CLUSTERS },
      new Arg[] { ZK_SSL_CONFIG_FILE, KAFKA_CONSUMER_CONFIG_FILE, RECOVER_CLUSTER, SKIP_LAST_STORE_CREATION, REPAIR }
  ),
  BACKUP_STORE_METADATA_FROM_GRAVEYARD(
      "backup-store-metadata-from-graveyard", "Backup store metadata from graveyard in EI",
      new Arg[] { VENICE_ZOOKEEPER_URL, ZK_SSL_CONFIG_FILE, BACKUP_FOLDER }
  ),
  MIGRATE_VENICE_ZK_PATHS(
      "migrate-venice-zk-paths", "Migrate Venice-specific metadata from a source ZK to a destination ZK",
      new Arg[] { SRC_ZOOKEEPER_URL, SRC_ZK_SSL_CONFIG_FILE, DEST_ZOOKEEPER_URL, DEST_ZK_SSL_CONFIG_FILE, CLUSTER_LIST,
          BASE_PATH }
  ),
  EXTRACT_VENICE_ZK_PATHS(
      "extract-venice-zk-paths",
      "Extract Venice-specific paths from a ZK snapshot input text file to an output text file",
      new Arg[] { INFILE, OUTFILE, CLUSTER_LIST, BASE_PATH }
  ),
  AGGREGATED_HEALTH_STATUS(
      "cluster-health-status",
      "Returns the set of instances which can be safely remove and instances which cannot be removed.",
      new Arg[] { URL, CLUSTER, INSTANCES, TO_BE_STOPPED_NODES }
  ),
  DUMP_HOST_HEARTBEAT(
      "dump-host-heartbeat",
      "Dump all heartbeat belong to a certain storage node. You can use topic/partition to filter specific resource, and you can choose to filter resources that are lagging.",
      new Arg[] { SERVER_URL, KAFKA_TOPIC_NAME }, new Arg[] { PARTITION, LAG_FILTER_ENABLED }
  );

  private final String commandName;
  private final String description;
  private final Arg[] requiredArgs;
  private final Arg[] optionalArgs;

  Command(String argName, String description, Arg[] requiredArgs) {
    this(argName, description, requiredArgs, new Arg[] {});
  }

  Command(String argName, String description, Arg[] requiredArgs, Arg[] optionalArgs) {
    this.commandName = argName;
    this.description = description;
    this.requiredArgs = requiredArgs;
    this.optionalArgs = optionalArgs;
  }

  @Override
  public String toString() {
    return commandName;
  }

  public Arg[] getRequiredArgs() {
    return requiredArgs;
  }

  public Arg[] getOptionalArgs() {
    return optionalArgs;
  }

  public String getDesc() {
    StringJoiner sj = new StringJoiner("");
    if (!description.isEmpty()) {
      sj.add(description);
      sj.add(". ");
    }

    StringJoiner requiredArgs = new StringJoiner(" ");
    for (Arg arg: getRequiredArgs()) {
      requiredArgs.add("--" + arg.toString());
    }

    sj.add("\nRequires: " + requiredArgs);

    StringJoiner optionalArgs = new StringJoiner(" ");
    for (Arg arg: getOptionalArgs()) {
      optionalArgs.add("--" + arg.toString());
    }

    if (getOptionalArgs().length > 0) {
      sj.add("\nOptional args: " + optionalArgs.toString());
    }

    return sj.toString();
  }

  public static final Comparator<Command> commandComparator = new Comparator<Command>() {
    public int compare(Command c1, Command c2) {
      return c1.commandName.compareTo(c2.commandName);
    }
  };

  public static Command getCommand(String name, CommandLine cmdLine) {
    for (Command cmd: values()) {
      if (cmd.commandName.equals(name)) {
        return cmd;
      }
    }
    if (name == null) {
      List<String> candidateCommands = Arrays.stream(Command.values())
          .filter(
              command -> Arrays.stream(command.getRequiredArgs()).allMatch(arg -> cmdLine.hasOption(arg.toString())))
          .map(command -> "--" + command)
          .collect(Collectors.toList());
      if (!candidateCommands.isEmpty()) {
        throw new VeniceException(
            "No command found, potential commands compatible with the provided parameters include: "
                + Arrays.toString(candidateCommands.toArray()));
      }
    }
    String message = name == null
        ? " No command found, Please specify a command, eg [--describe-store] "
        : "No Command found with name: " + name;
    throw new VeniceException(message);
  }
}
