package com.linkedin.venice.controllerapi;

public class ControllerApiConstants {
  public static final String HOSTNAME = "hostname";
  public static final String CLUSTER = "cluster_name";
  public static final String CLUSTER_DEST = "cluster_name_dest";
  public static final String SOURCE_GRID_FABRIC = "source_grid_fabric";
  public static final String BATCH_JOB_HEARTBEAT_ENABLED = "batch_job_heartbeat_enabled";

  public static final String NAME = "store_name";
  public static final String STORE_PARTITION = "store_partition";
  public static final String STORE_VERSION = "store_version";
  public static final String OWNER = "owner";
  public static final String FABRIC = "fabric";
  public static final String FABRIC_A = "fabric_a";
  public static final String FABRIC_B = "fabric_b";
  public static final String PARTITION_COUNT = "partition_count";
  public static final String PARTITIONER_CLASS = "partitioner_class";
  public static final String PARTITIONER_PARAMS = "partitioner_params";
  public static final String AMPLIFICATION_FACTOR = "amplification_factor";
  public static final String STORE_SIZE = "store_size";
  public static final String VERSION = "version";
  public static final String INCREMENTAL_PUSH_VERSION = "incremental_push_version";
  public static final String STATUS = "status";
  public static final String FROZEN = "frozen";
  public static final String ERROR = "error";
  public static final String STORAGE_NODE_ID = "storage_node_id"; /* host_port */

  // We need the following 3 constants for Helix API aggregatedHealthStatus
  public static final String CLUSTER_ID = "cluster_id";
  public static final String INSTANCES = "instances";
  public static final String TO_BE_STOPPED_INSTANCES = "to_be_stopped_instances";

  public static final String KEY_SCHEMA = "key_schema";
  public static final String VALUE_SCHEMA = "value_schema";
  public static final String DERIVED_SCHEMA = "derived_schema";
  public static final String SCHEMA_ID = "schema_id";
  public static final String SCHEMA_COMPAT_TYPE = "schema_compat_type";
  public static final String DERIVED_SCHEMA_ID = "derived_schema_id";
  public static final String TOPIC = "topic";
  public static final String OFFSET = "offset";
  public static final String OPERATION = "operation";
  public static final String READ_OPERATION = "read";
  public static final String WRITE_OPERATION = "write";
  public static final String READ_WRITE_OPERATION = READ_OPERATION + WRITE_OPERATION;
  public static final String EXECUTION_ID = "execution_id";
  public static final String ENABLE_READS = "enable_reads";
  public static final String ENABLE_WRITES = "enable_writes";
  public static final String STORAGE_QUOTA_IN_BYTE = "storage_quota_in_byte";
  public static final String HYBRID_STORE_DISK_QUOTA_ENABLED = "hybrid-store-disk-quota-enabled";
  public static final String HYBRID_STORE_OVERHEAD_BYPASS = "hybrid_store_overhead_bypass";
  public static final String READ_QUOTA_IN_CU = "read_quota_in_cu";
  public static final String REWIND_TIME_IN_SECONDS = "rewind_time_seconds";
  public static final String OFFSET_LAG_TO_GO_ONLINE = "offset_lag_to_go_online";
  public static final String TIME_LAG_TO_GO_ONLINE = "time_lag_to_go_online";
  public static final String DATA_REPLICATION_POLICY = "data_replication_policy";
  public static final String BUFFER_REPLAY_POLICY = "buffer_replay_policy";
  public static final String REAL_TIME_TOPIC_NAME = "real_time_topic_name";
  public static final String COMPRESSION_STRATEGY = "compression_strategy";
  public static final String CLIENT_DECOMPRESSION_ENABLED = "client_decompression_enabled";
  public static final String CHUNKING_ENABLED = "chunking_enabled";
  public static final String RMD_CHUNKING_ENABLED = "rmd_chunking_enabled";
  public static final String INCREMENTAL_PUSH_ENABLED = "incremental_push_enabled";
  public static final String SEPARATE_REAL_TIME_TOPIC_ENABLED = "separate_realtime_topic_enabled";
  public static final String SINGLE_GET_ROUTER_CACHE_ENABLED = "single_get_router_cache_enabled";
  public static final String BATCH_GET_ROUTER_CACHE_ENABLED = "batch_get_router_cache_enabled";
  public static final String BATCH_GET_LIMIT = "batch_get_limit";
  public static final String LARGEST_USED_VERSION_NUMBER = "largest_used_version_number";
  public static final String NUM_VERSIONS_TO_PRESERVE = "num_versions_to_preserve";
  public static final String DISABLE_META_STORE = "disable_meta_store";
  public static final String DISABLE_DAVINCI_PUSH_STATUS_STORE = "disable_davinci_push_status_store";
  public static final String PUSH_TYPE = "push_type";
  public static final String PUSH_JOB_ID = "push_job_id";
  public static final String SEND_START_OF_PUSH = "start_of_push";
  public static final String PUSH_IN_SORTED_ORDER = "push_in_sorted_order";
  public static final String PARTITIONERS = "partitioners";
  public static final String COMPRESSION_DICTIONARY = "compression_dictionary";
  public static final String REMOTE_KAFKA_BOOTSTRAP_SERVERS = "remote_kafka_bootstrap_servers";

  public static final String EXPECTED_ROUTER_COUNT = "expected_router_count";

  public static final String VOLDEMORT_STORE_NAME = "voldemort_store_name";
  public static final String PUSH_STRATEGY = "push_strategy";

  public static final String ACCESS_CONTROLLED = "access_controlled";
  /**
   * @deprecated Use {@link #ENABLE_STORE_MIGRATION} instead. This constant is kept for backward compatibility
   * and will be removed in a future release.
   */
  @Deprecated
  public static final String STORE_MIGRATION = "store_migration";

  /**
   * Constant for enabling store migration. Replaces the deprecated {@link #STORE_MIGRATION}.
   */
  public static final String ENABLE_STORE_MIGRATION = "enable_store_migration";

  public static final String PUSH_JOB_STATUS = "push_job_status";
  public static final String PUSH_JOB_DURATION = "push_job_duration";
  public static final String PUSH_JOB_DETAILS = "push_job_details";

  public static final String WRITE_COMPUTATION_ENABLED = "write_computation_enabled";
  public static final String REPLICATION_METADATA_PROTOCOL_VERSION_ID = "replication_metadata_protocol_version_id";
  public static final String READ_COMPUTATION_ENABLED = "read_computation_enabled";
  public static final String BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS = "bootstrap_to_online_timeout_in_hours";

  public static final String INCLUDE_SYSTEM_STORES = "include_system_stores";

  public static final String STORE_VIEW = "store_view";
  public static final String STORE_VIEW_NAME = "store_view_name";
  public static final String STORE_VIEW_CLASS = "store_view_class";
  public static final String STORE_VIEW_PARAMS = "store_view_params";
  public static final String DISABLE_STORE_VIEW = "disable_store_view";

  public static final String NATIVE_REPLICATION_ENABLED = "native_replication_enabled";
  public static final String PUSH_STREAM_SOURCE_ADDRESS = "push_stream_source_address";

  public static final String BACKUP_STRATEGY = "backup_strategy";

  public static final String AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED = "auto_auto_register_for_pushjob_enabled";

  public static final String REGULAR_VERSION_ETL_ENABLED = "regular_version_etl_enabled";

  public static final String FUTURE_VERSION_ETL_ENABLED = "future_version_etl_enabled";

  public static final String ETLED_PROXY_USER_ACCOUNT = "etled_proxy_user_account";

  public static final String SKIP_DIV = "skip_div";

  public static final String STORE_TYPE = "store_type";

  public static final String IS_SYSTEM_STORE = "is_system_store";

  public static final String TOPIC_COMPACTION_POLICY = "topic_compaction_policy";

  public static final String BACKUP_VERSION_RETENTION_MS = "backup_version_retention_ms";

  public static final String REPLICATION_FACTOR = "replication_factor";

  public static final String ACCESS_PERMISSION = "access_permission";

  public static final String IS_WRITE_COMPUTE_ENABLED = "is_write_compute_enabled";

  public static final String MIGRATION_DUPLICATE_STORE = "migration_duplicate_store";

  public static final String NATIVE_REPLICATION_SOURCE_FABRIC = "native_replication_source_fabric";

  public static final String UPDATED_CONFIGS_LIST = "updated_configs_list";

  public static final String REPLICATE_ALL_CONFIGS = "replicate_all_configs";

  public static final String ACTIVE_ACTIVE_REPLICATION_ENABLED = "active_active_replication_enabled";

  public static final String REGIONS_FILTER = "regions_filter";

  public static final String REWIND_TIME_IN_SECONDS_OVERRIDE = "rewind_time_in_seconds_override";

  public static final String DEFER_VERSION_SWAP = "defer_version_swap";

  public static final String REPUSH_SOURCE_VERSION = "repush_source_version";

  public static final String REPLICATION_METADATA_VERSION_ID = "replication_metadata_version_id";

  public static final String PARTITION_DETAIL_ENABLED = "partition_detail_enabled";

  /**
   * How many records that one server could consume from Kafka at most in one second from the specified regions.
   * If the consume rate reached this quota, the consumption thread will be blocked until there is the available quota.
   * The value for this config is read from cluster configs in Zk.
   */
  public static final String SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND =
      "server.kafka.fetch.quota.records.per.second";

  /**
   * An optional argument in list-store command; pass in a store config to select stores. If the config name argument is
   * used in the command, users must specify the config value filter too.
   */
  public static final String STORE_CONFIG_NAME_FILTER = "store_config_name_filter";

  /**
   * An optional argument in list-store command; if the config name argument is used in the command, users must specify
   * the config value filter too.
   */
  public static final String STORE_CONFIG_VALUE_FILTER = "store_config_value_filter";

  /**
   * Whether stores are allowed to be migrated from/to a specific cluster.
   * The value for this config is read from cluster configs in Zk.
   */
  public static final String ALLOW_STORE_MIGRATION = "allow.store.migration";

  /**
   * Whether admin consumption should be enabled. This config will only control the behavior in Child Controller.
   * The value for this config is read from cluster configs in Zk.
   */
  public static final String CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED =
      "child.controller.admin.topic.consumption.enabled";

  /**
   * For Blueshift data copy over across fabrics.
   */
  public static final String SOURCE_FABRIC = "source_fabric";

  public static final String DEST_FABRIC = "dest_fabric";

  /**
   * Whether or not to copy all version configs from the source fabric when performing data recovery in a destination
   * fabric. This config is set to false by default and only essential configs (such as partition count etc.) are copied
   * from the source fabric and the remaining configs will be generated based on destination fabric's Store configs.
   */
  public static final String DATA_RECOVERY_COPY_ALL_VERSION_CONFIGS = "data.recovery.copy.all.version.configs";

  /**
   * Boolean flag to indicate whether the {@link com.linkedin.venice.meta.Version} needed for data recover from the
   * source fabric is included in the request body as byte array or not.
   */
  public static final String SOURCE_FABRIC_VERSION_INCLUDED = "source.fabric.version.included";

  public static final String KAFKA_TOPIC_LOG_COMPACTION_ENABLED = "kafka.topic.log.compaction.enabled";
  public static final String KAFKA_TOPIC_RETENTION_IN_MS = "kafka.topic.retention.in.ms";
  public static final String KAFKA_TOPIC_MIN_IN_SYNC_REPLICA = "kafka.topic.min.in.sync.replica";
  public static final String UPSTREAM_OFFSET = "upstream_offset";

  public static final String PERSONA_NAME = "persona_name";
  public static final String PERSONA_OWNERS = "persona_owners";
  public static final String PERSONA_STORES = "persona_stores";
  public static final String PERSONA_QUOTA = "persona_quota";
  public static final String LATEST_SUPERSET_SCHEMA_ID = "latest_superset_schema_id";
  public static final String ENABLE_DISABLED_REPLICAS = "enable_disabled_replicas";

  public static final String VALUE_SCHEMA_IDS = "value_schema_ids";

  /**
   * String representation of the list of regions that is separated by comma for targeted region push
   */
  public static final String TARGETED_REGIONS = "targeted_regions";

  public static final String STORAGE_NODE_READ_QUOTA_ENABLED = "storage_node_read_quota_enabled";

  public static final String MIN_COMPACTION_LAG_SECONDS = "min_compaction_lag_seconds";

  public static final String MAX_COMPACTION_LAG_SECONDS = "max_compaction_lag_seconds";

  public static final String MAX_RECORD_SIZE_BYTES = "max_record_size_bytes";
  public static final String MAX_NEARLINE_RECORD_SIZE_BYTES = "max_nearline_record_size_bytes";

  public static final String UNUSED_SCHEMA_DELETION_ENABLED = "unused_schema_deletion_enabled";

  public static final String BLOB_TRANSFER_ENABLED = "blob_transfer_enabled";

  public static final String HEARTBEAT_TIMESTAMP = "heartbeat_timestamp";

  public static final String NEARLINE_PRODUCER_COMPRESSION_ENABLED = "nearline_producer_compression_enabled";
  public static final String NEARLINE_PRODUCER_COUNT_PER_WRITER = "nearline_producer_count_per_writer";
  public static final String TARGET_SWAP_REGION = "target_swap_region";
  public static final String TARGET_SWAP_REGION_WAIT_TIME = "target_swap_region_wait_time";
  public static final String IS_DAVINCI_HEARTBEAT_REPORTED = "is_davinci_heartbeat_reported";
}
