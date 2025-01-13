package com.linkedin.venice;

import com.linkedin.venice.migration.MigrationPushStrategy;


/**
 * TODO: Merge this with {@link com.linkedin.venice.controllerapi.ControllerApiConstants}
 */
public enum Arg {
  ACCESS_CONTROL("access-control", "acl", true, "Enable/disable store-level access control"),
  URL("url", "u", true, "Venice url, eg. http://localhost:1689  This can be a router or a controller"),
  SERVER_URL("server-url", "su", true, "Venice server url, eg. http://localhost:1690  This has to be a storage node"),
  VENICE_ZOOKEEPER_URL("venice-zookeeper-url", "vzu", true, "Venice Zookeeper url, eg. localhost:2622"),
  SRC_ZOOKEEPER_URL("src-zookeeper-url", "szu", true, "Source Zookeeper url, eg. localhost:2181"),
  DEST_ZOOKEEPER_URL("dest-zookeeper-url", "dzu", true, "Destination Zookeeper url, eg. localhost:2182"),
  INFILE("infile", "if", true, "Path to input text file"), OUTFILE("outfile", "of", true, "Path to output text file"),
  BASE_PATH("base-path", "bp", true, "Base path for ZK, eg. /venice-parent"),
  CLUSTER("cluster", "c", true, "Name of Venice cluster"),
  CLUSTER_SRC("cluster-src", "cs", true, "Store migration original Venice cluster name"),
  CLUSTER_DEST("cluster-dest", "cd", true, "Store migration destination Venice cluster name"),
  CLUSTER_LIST("cluster-list", "cl", true, "Comma separated list of cluster names, eg. venice-0, venice-1, ..."),
  STORE("store", "s", true, "Name of Venice store"), STORES("stores", "sts", true, "Name of a group of Venice stores"),
  VERSION("version", "v", true, "Active store version number"),
  LARGEST_USED_VERSION_NUMBER(
      "largest-used-version", "luv", true, "Largest used store version number (whether active or not)"
  ), PUSH_ID("push-id", "pid", true, "Push Id"),
  STORE_SIZE("store-size", "ss", true, "Size of the store in bytes, used to calculate partitioning"),
  KEY_SCHEMA("key-schema-file", "ks", true, "Path to text file with key schema"),
  VALUE_SCHEMA_ID("value-schema-id", "vid", true, "value schema id"),
  VALUE_SCHEMA("value-schema-file", "vs", true, "Path to text file with value schema"),
  ZK_SSL_CONFIG_FILE("zk-ssl-config-file", "zscf", true, "Path to text file with ZK SSL configs"),
  SRC_ZK_SSL_CONFIG_FILE("src-zk-ssl-config-file", "szscf", true, "Path to text file with source ZK SSL configs"),
  DEST_ZK_SSL_CONFIG_FILE(
      "dest-zk-ssl-config-file", "dzscf", true, "Path to text file with destination ZK SSL configs"
  ), DERIVED_SCHEMA_ID("derived-schema-id", "did", true, "derived schema id"),
  DERIVED_SCHEMA("derived-schema-file", "ds", true, "Path to text file with derived schema"),
  OWNER("owner", "o", true, "Owner email for new store creation"),
  STORAGE_NODE("storage-node", "n", true, "Helix instance ID for a storage node, eg. lva1-app1234_1690"),
  KEY("key", "k", true, "Plain-text key for identifying a record in a store"),
  OFFSET("offset", "of", true, "Kafka offset number"),
  EXECUTION("execution", "e", true, "Execution ID of async admin command"),
  PARTITION_COUNT("partition-count", "pn", true, "number of partitions a store has"),
  PARTITIONER_CLASS("partitioner-class", "pc", true, "Name of chosen partitioner class"),
  PARTITIONER_PARAMS("partitioner-params", "pp", true, "Additional parameters for partitioner."),
  READABILITY("readability", "rb", true, "store's readability"),
  WRITEABILITY("writeability", "wb", true, "store's writeability"),
  STORAGE_QUOTA("storage-quota", "sq", true, "maximum capacity a store version or storage persona could have"),
  STORAGE_NODE_READ_QUOTA_ENABLED(
      "storage-node-read-quota-enabled", "snrqe", true, "whether storage node read quota is enabled for this store"
  ),
  DISABLE_META_STORE(
      "disable-meta-store", "dms", false,
      "disable meta system store. This command sets storeMetaSystemStoreEnabled flag to false but does not delete any resources associated with the meta store. Please use this option with caution"
  ),
  DISABLE_DAVINCI_PUSH_STATUS_STORE(
      "disable-davinci-push-status-store", "ddvc", false,
      "disable davinci push status store. This command sets daVinciPushStatusStoreEnabled flag to false but does not delete any resources associated with the push status store. Please use this option with caution"
  ),
  HYBRID_STORE_DISK_QUOTA_ENABLED(
      "hybrid-store-disk-quota-enabled", "hsq", true, "whether or not enable disk quota for a hybrid store"
  ),
  HYBRID_STORE_OVERHEAD_BYPASS(
      "hybrid-store-overhead-bypass", "ob", true,
      "for hybrid stores, if set to false, updating storage quota will be multiplied by store db overhead ratio."
  ), READ_QUOTA("read-quota", "rq", true, "quota for read request hit this store. Measurement is capacity unit"),
  HYBRID_REWIND_SECONDS(
      "hybrid-rewind-seconds", "hr", true,
      "for hybrid stores, how far back to rewind in the nearline stream after a batch push completes"
  ),
  HYBRID_OFFSET_LAG(
      "hybrid-offset-lag", "ho", true,
      "for hybrid stores, what is the offset lag threshold for the storage nodes' consumption to be considered ONLINE"
  ),
  HYBRID_TIME_LAG(
      "hybrid-time-lag", "ht", true,
      "for hybrid stores, servers cannot report ready-to-serve until they see a message with producer timestamp bigger than (current time - this threshold)"
  ),
  HYBRID_DATA_REPLICATION_POLICY(
      "hybrid-data-replication-policy", "hdrp", true, "for hybrid stores, how real-time Samza data is replicated"
  ),
  HYBRID_BUFFER_REPLAY_POLICY(
      "hybrid-buffer-replay-policy", "hbrp", true,
      "for hybrid stores, how buffer replay start timestamps are calculated."
  ), EXPECTED_ROUTER_COUNT("expected-router-count", "erc", true, "How many routers that a cluster should have."),
  VOLDEMORT_STORE("voldemort-store", "vs", true, "Voldemort store name"),
  MIGRATION_PUSH_STRATEGY(
      "migration-push-strategy", "ps", true,
      "Migration push strategy, valid values: [" + MigrationPushStrategy.getAllEnumString() + "]"
  ), VSON_STORE("vson_store", "vson", true, "indicate whether it is Vson store or Avro store"),
  COMPRESSION_STRATEGY("compression-strategy", "cs", true, "strategies used to compress/decompress Record's value"),
  CLIENT_DECOMPRESSION_ENABLED(
      "client-decompression-enabled", "csd", true, "Enable/Disable client-side record decompression (default: true)"
  ),
  CHUNKING_ENABLED(
      "chunking-enabled", "ce", true, "Enable/Disable value chunking, mostly for large value store support"
  ),
  RMD_CHUNKING_ENABLED(
      "rmd-chunking-enabled", "rce", true,
      "Enable/Disable replication metadata chunking, mostly for Active/Active replication enabled store with partial update requirement support"
  ),
  INCREMENTAL_PUSH_ENABLED(
      "incremental-push-enabled", "ipe", true, "a flag to see if the store supports incremental push or not"
  ),
  SEPARATE_REALTIME_TOPIC_ENABLED(
      "separate-realtime-topic-enabled", "srte", true,
      "a flag to see if the store supports separate real-time topic or not"
  ), BATCH_GET_LIMIT("batch-get-limit", "bgl", true, "Key number limit inside one batch-get request"),
  NUM_VERSIONS_TO_PRESERVE("num-versions-to-preserve", "nvp", true, "Number of version that store should preserve."),
  KAFKA_BOOTSTRAP_SERVERS("kafka-bootstrap-servers", "kbs", true, "Kafka bootstrap server URL(s)"),
  KAFKA_TOPIC_NAME("kafka-topic-name", "ktn", true, "Kafka topic name"),
  KAFKA_TOPIC_PARTITION("kafka-topic-partition", "ktp", true, "Kafka topic partition number"),
  KAFKA_CONSUMER_CONFIG_FILE(
      "kafka-consumer-config-file", "kcc", true, "Configuration file for SSL (optional, if plain-text is available)"
  ),
  KAFKA_OPERATION_TIMEOUT(
      "kafka-operation-timeout", "kot", true, "Timeout in seconds for Kafka operations (default: 30 sec)"
  ),
  VENICE_CLIENT_SSL_CONFIG_FILE(
      "venice-client-ssl-config-file", "vcsc", true, "Configuration file for querying key in Venice client through SSL."
  ), STARTING_OFFSET("starting_offset", "so", true, "Starting offset when dumping admin messages, inclusive"),
  MESSAGE_COUNT("message_count", "mc", true, "Max message count when dumping admin messages"),
  PARENT_DIRECTORY(
      "parent_output_directory", "pod", true,
      "A directory where output can be dumped to.  If dumping a kafka topic, the output will be dumped under this directory."
  ),
  MAX_POLL_ATTEMPTS(
      "max_poll_attempts", "mpa", true,
      "The max amount of attempts to poll new data from a Kafka topic (should no new data be available)."
  ),
  WRITE_COMPUTATION_ENABLED(
      "write-computation-enabled", "wc", true, "Whether or not write computation is enabled for a store"
  ), READ_COMPUTATION_ENABLED("read-computation-enabled", "rc", true, "Enable/Disable read computation for a store"),
  BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOUR(
      "bootstrap-to-online-timeout", "btot", true,
      "Set the maximum number of hours allowed for the store to transition from bootstrap to online"
  ), SKIP_DIV("skip-div", "div", true, "Whether or not to only skip DIV for skip admin"),
  BACKUP_STRATEGY(
      "backup-strategy", "bus", true,
      "Strategies to preserve backup versions, eg KEEP_MIN_VERSIONS, DELETE_ON_NEW_PUSH_START. Default is KEEP_MIN_VERSIONS"
  ),
  AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED(
      "auto-schema-register-push-job-enabled", "asp", true, "whether or not to use auto-schema register for pushjob"
  ),
  LATEST_SUPERSET_SCHEMA_ID("latest-superset-schema-id", "lssi", true, "the latest superset schema id for this store"),
  REGULAR_VERSION_ETL_ENABLED(
      "regular-version-etl-enabled", "rve", true, "whether or not to enable regular version etl for this store."
  ),
  FUTURE_VERSION_ETL_ENABLED(
      "future-version-etl-enabled", "fve", true, "whether or not to enable future version etl for this store."
  ),
  ETLED_PROXY_USER_ACCOUNT(
      "etled-proxy-user-account", "epu", true,
      "if enabled ETL, the proxy user account for HDFS file directory where the ETLed snapshots will go."
  ),
  BACKUP_VERSION_RETENTION_DAY(
      "backup-version-retention-day", "bvrd", true,
      "Backup version retention time in day after a new version is promoted to the current version, if not specified, Venice will use the configured retention as the default policy"
  ), REPLICATION_FACTOR("replication-factor", "rf", true, "the number of replica each store version will have"),

  FLAT_JSON("flat-json", "flj", false, "Display output as flat json, without pretty-print indentation and line breaks"),
  HELP("help", "h", false, "Show usage"), FORCE("force", "f", false, "Force execute this operation"),
  INCLUDE_SYSTEM_STORES("include-system-stores", "iss", true, "Include internal stores maintained by the system."),
  SSL_CONFIG_PATH("ssl-config-path", "scp", true, "SSl config file path"),
  STORE_TYPE(
      "store-type", "st", true,
      "the type of the stores. The support type are 'batch_only', hybrid_only', `incremental_push', 'hybrid_or_incremental', 'system', 'all'"
  ),
  NATIVE_REPLICATION_ENABLED(
      "native-replication-enabled", "nr", true,
      "whether or not native replication is enabled for this store.  Leader Follow must also be enabled."
  ),
  PUSH_STREAM_SOURCE_ADDRESS(
      "push-stream-source-address", "pssa", true,
      "The url address for the kafka broker which hosts the topic which contains the push data for this store."
  ), FABRIC("fabric", "fc", true, "Which fabric to execute the admin command."),
  FABRIC_A("fabric-a", "fa", true, "The name of the first fabric in store comparison."),
  FABRIC_B("fabric-b", "fb", true, "The name of the second fabric in store comparison."),
  SOURCE_FABRIC("source-fabric", "sf", true, "The fabric where metadata/data copy over starts from"),
  DEST_FABRIC("dest-fabric", "df", true, "The fabric where metadata/data gets copy over into"),
  ACL_PERMS("acl-perms", "ap", true, "Acl permissions for the store"),
  LOG_METADATA("log-metadata", "lm", false, "Log the metadata for each kafka message on console"),
  LOG_DATA_RECORD("log-data-record", "ldr", false, "Log the data record for each kafka message on console"),
  LOG_RMD_RECORD("log-rmd-record", "lrr", false, "Log the RMD record for each kafka message on console"),
  LOG_TS_RECORD("log-ts-record", "lts", false, "Log the topic switch message on console"),
  NATIVE_REPLICATION_SOURCE_FABRIC(
      "native-replication-source-fabric", "nrsf", true,
      "The source fabric name to be used in native replication. Remote consumption will happen from kafka in this fabric."
  ), PRINCIPAL("principal", "p", true, "Principal to add/modify/delete ACLs"),
  REPLICATE_ALL_CONFIGS(
      "replicate-all-configs", "rac", false,
      "Whether all unchanged store configs in parent controller will be replicated to child controllers"
  ),
  ACTIVE_ACTIVE_REPLICATION_ENABLED(
      "active-active-replication-enabled", "aa", true,
      "A parameter flag to enable/disable Active/Active replication feature for a store"
  ),
  REGIONS_FILTER(
      "regions-filter", "regf", true,
      "A list of regions that will be impacted by the command; can be used by UpdateStore command"
  ),
  SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND(
      ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND, "kfq", true,
      "The quota of records to fetch from Kafka for the specified fabric."
  ),
  STORE_CONFIG_NAME_FILTER(
      "store-config-name-filter", "scnf", true,
      "An optional argument in list-store command; pass in a store config to select stores. If the config name argument is used in the command, users must specify the config value filter too."
  ),
  STORE_CONFIG_VALUE_FILTER(
      "store-config-value-filter", "scvf", true,
      "n optional argument in list-store command; if the config name argument is used in the command, users must specify the config value filter too."
  ),
  ALLOW_STORE_MIGRATION(
      ConfigKeys.ALLOW_STORE_MIGRATION, "asm", true, "whether stores are allowed to be migrated from/to a cluster"
  ),
  KAFKA_TOPIC_LOG_COMPACTION_ENABLED(
      "kafka-topic-log-compaction-enabled", "ktlce", true, "Enable/disable Kafka log compaction for a topic"
  ),
  KAFKA_TOPIC_RETENTION_IN_MS(
      "kafka-topic-retention-in-ms", "ktrim", true, "Kafka topic retention time in milliseconds"
  ), KAFKA_TOPIC_MIN_IN_SYNC_REPLICA("kafka-topic-min-in-sync-replica", "ktmisr", true, "Kafka topic minISR config"),
  CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED(
      ConfigKeys.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED, "atc", true,
      "whether child controller consumes admin topic"
  ),
  SYSTEM_STORE_TYPE(
      "system-store-type", "sst", true,
      "Type of system store to backfill. Supported types are davinci_push_status_store and meta_store"
  ), RETRY("retry", "r", false, "Retry this operation"),
  DISABLE_LOG("disable-log", "dl", false, "Disable logs from internal classes. Only print command output on console"),
  STORE_VIEW_CONFIGS(
      "storage-view-configs", "svc", true,
      "Config that describes views to be added for a store.  Input is a json map.  Example: {\"ExampleView\": {\"viewClassName\": \"com.linkedin.venice.views.ChangeCaptureView\",\"params\": {}}}"
  ), VIEW_NAME("view-name", "vn", true, "Name of a store view"),
  VIEW_CLASS("view-class", "vc", true, "Name of a store view class"),
  VIEW_PARAMS("view-params", "vp", true, "Additional parameter map of a store view class"),
  REMOVE_VIEW("remove-view", "rv", false, "Optional config to specify to disable certain store view"),
  PARTITION_DETAIL_ENABLED(
      "partition-detail-enabled", "pde", true, "A flag to indicate whether to retrieve partition details"
  ),

  START_DATE("start-date", "sd", true, "Start date in PST. Example: 2020-10-10 10:10:10"),
  END_DATE("end-date", "ed", true, "End date in PST. Example: 2020-10-10 10:10:10"),
  PROGRESS_INTERVAL("progress-interval", "pi", true, "Dump progress after processing this number of messages"),

  STORAGE_PERSONA("storage-persona", "sp", true, "Name of Storage Persona"),
  RECOVERY_COMMAND("recovery-command", "rco", true, "command to execute the data recovery"),
  EXTRA_COMMAND_ARGS("extra-command-args", "eca", true, "extra command arguments"),
  ENABLE_DISABLED_REPLICA("enable-disabled-replicas", "edr", true, "Reenable disabled replicas"),
  NON_INTERACTIVE("non-interactive", "nita", false, "non-interactive mode"),
  MIN_COMPACTION_LAG_SECONDS(
      "min-compaction-lag-seconds", "mcls", true, "Min compaction lag seconds for version topic of hybrid stores"
  ),
  MAX_COMPACTION_LAG_SECONDS(
      "max-compaction-lag-seconds", "mxcls", true, "Max compaction lag seconds for version topic of hybrid stores"
  ),
  MAX_RECORD_SIZE_BYTES(
      "max-record-size-bytes", "mrsb", true,
      "Store-level max record size for VeniceWriter to determine whether to fail batch push jobs. This setting can potentially converge with the nearline setting in the future."
  ),
  MAX_NEARLINE_RECORD_SIZE_BYTES(
      "max-nearline-record-size-bytes", "mnrsb", true,
      "Store-level max record size for VeniceWriter to determine whether to pause consumption on nearline jobs with partial updates."
  ), UNUSED_SCHEMA_DELETION_ENABLED("enable-unused-schema-deletion", "usde", true, "Enable unused schema deletion"),
  PARTITION("partition", "p", true, "Partition Id"),
  INTERVAL(
      "interval", "itv", true,
      "monitor data recovery progress at seconds close to the number specified by the interval parameter until tasks are finished"
  ), DATETIME("datetime", "dtm", true, "Date and time stamp (YYYY-MM-DDTHH:MM:SS) in UTC time zone for data recovery"),
  SKIP_LAST_STORE_CREATION(
      "skip-last-store-creation", "slsc", true,
      "Skip last round of store creation and the following schema manipulation"
  ), REPAIR("repair", "re", true, "Repair the store"),
  GRAVEYARD_CLUSTERS(
      "graveyard-clusters", "gc", true, "Clusters to scan store graveyard to retrieve metadata, eg. cluster-1,cluster-2"
  ), RECOVER_CLUSTER("recover-cluster", "rc", true, "Cluster to recover from"),
  BACKUP_FOLDER("backup-folder", "bf", true, "Backup folder path"),
  DEBUG("debug", "d", false, "Print debugging messages for execute-data-recovery"),
  BLOB_TRANSFER_ENABLED("blob-transfer-enabled", "bt", true, "Flag to indicate if the blob transfer is allowed or not"),
  NEARLINE_PRODUCER_COMPRESSION_ENABLED(
      "nearline-producer-compression-enabled", "npce", true,
      "Flag to control whether KafkaProducer will use compression or not for nearline workload"
  ),
  NEARLINE_PRODUCER_COUNT_PER_WRITER(
      "nearline-producer-count-per-writer", "npcpw", true,
      "How many producers will be used to write nearline workload in Server"
  ), INSTANCES("instances", "in", true, "Input list of helix ids of nodes to check if they can removed or not"),
  TO_BE_STOPPED_NODES("to-be-stopped-nodes", "tbsn", true, "List of helix ids of nodes assumed to be stopped"),
  LAG_FILTER_ENABLED("lag-filter-enabled", "lfe", true, "Enable heartbeat lag filter for a heartbeat request"),
  TARGET_SWAP_REGION("target-region-swap", "trs", true, "Region to swap current version during target colo push"),
  TARGET_SWAP_REGION_WAIT_TIME(
      "target-region-swap-wait-time", "trswt", true,
      "How long to wait in minutes before swapping to the new version in a target colo push"
  ),
  DAVINCI_HEARTBEAT_REPORTED(
      "dvc-heartbeat-reported", "dvchb", true, "Flag to indicate whether DVC is bootstrapping and sending heartbeats"
  ), ENABLE_STORE_MIGRATION("enable-store-migration", "esm", true, "Toggle store migration store config");

  private final String argName;
  private final String first;
  private final boolean parameterized;
  private final String helpText;

  Arg(String argName, String first, boolean parameterized, String helpText) {
    this.argName = argName;
    this.first = first;
    this.parameterized = parameterized;
    this.helpText = helpText;
  }

  @Override
  public String toString() {
    return argName;
  }

  public String first() {
    return first;
  }

  public String getArgName() {
    return argName;
  }

  public String getHelpText() {
    return helpText;
  }

  public boolean isParameterized() {
    return parameterized;
  }
}
