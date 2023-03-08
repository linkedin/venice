package com.linkedin.venice;

import com.linkedin.venice.migration.MigrationPushStrategy;


/**
 * TODO: Merge this with {@link com.linkedin.venice.controllerapi.ControllerApiConstants}
 */
public enum Arg {
  ACCESS_CONTROL("access-control", "acl", true, "Enable/disable store-level access control"),
  URL("url", "u", true, "Venice url, eg. http://localhost:1689  This can be a router or a controller"),
  VENICE_ZOOKEEPER_URL("venice-zookeeper-url", "vzu", true, "Venice Zookeeper url, eg. localhost:2622"),
  CLUSTER("cluster", "c", true, "Name of Venice cluster"),
  CLUSTER_SRC("cluster-src", "cs", true, "Store migration original Venice cluster name"),
  CLUSTER_DEST("cluster-dest", "cd", true, "Store migration destination Venice cluster name"),
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
  DERIVED_SCHEMA_ID("derived-schema-id", "did", true, "derived schema id"),
  DERIVED_SCHEMA("derived-schema-file", "ds", true, "Path to text file with derived schema"),
  OWNER("owner", "o", true, "Owner email for new store creation"),
  STORAGE_NODE("storage-node", "n", true, "Helix instance ID for a storage node, eg. lva1-app1234_1690"),
  KEY("key", "k", true, "Plain-text key for identifying a record in a store"),
  OFFSET("offset", "of", true, "Kafka offset number"),
  EXECUTION("execution", "e", true, "Execution ID of async admin command"),
  PARTITION_COUNT("partition-count", "pn", true, "number of partitions a store has"),
  PARTITIONER_CLASS("partitioner-class", "pc", true, "Name of chosen partitioner class"),
  PARTITIONER_PARAMS("partitioner-params", "pp", true, "Additional parameters for partitioner."),
  AMPLIFICATION_FACTOR("amplification-factor", "af", true, "Amplification factor for store"),
  READABILITY("readability", "rb", true, "store's readability"),
  WRITEABILITY("writeability", "wb", true, "store's writeability"),
  STORAGE_QUOTA("storage-quota", "sq", true, "maximum capacity a store version or storage persona could have"),
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
  ), BATCH_GET_LIMIT("batch-get-limit", "bgl", true, "Key number limit inside one batch-get request"),
  NUM_VERSIONS_TO_PRESERVE("num-versions-to-preserve", "nvp", true, "Number of version that store should preserve."),
  KAFKA_BOOTSTRAP_SERVERS("kafka-bootstrap-servers", "kbs", true, "Kafka bootstrap server URL(s)"),
  KAFKA_TOPIC_NAME("kafka-topic-name", "ktn", true, "Kafka topic name"),
  KAFKA_TOPIC_PARTITION("kafka-topic-partition", "ktp", true, "Kafka topic partition number"),
  KAFKA_CONSUMER_CONFIG_FILE(
      "kafka-conumer-config-file", "kcc", true, "Configuration file for SSL (optional, if plain-text is available)"
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
  LOG_METADATA("log-metedata", "lm", false, "Only log the metadata for each kafka message on console"),
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
  ),
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
  ),
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
  DEBUG("debug", "d", false, "Print debugging messages for execute-data-recovery");

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
