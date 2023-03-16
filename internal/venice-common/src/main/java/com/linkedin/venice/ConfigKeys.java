package com.linkedin.venice;

import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;


public class ConfigKeys {
  private ConfigKeys() {
  }

  // cluster specific properties
  public static final String CLUSTER_NAME = "cluster.name";
  public static final String ZOOKEEPER_ADDRESS = "zookeeper.address";

  public static final String ADMIN_PORT = "admin.port";
  public static final String ADMIN_HOSTNAME = "admin.hostname";

  public static final String ADMIN_SECURE_PORT = "admin.secure.port";

  /**
   * Whether controller should check "Read" method against Kafka wildcard ACL while users request
   * for a topic to write.
   *
   * By default, the config value should be true, but setting it to false would allow us to release
   * new version of controller when the "Read" method check is not working as expected.
   */
  public static final String ADMIN_CHECK_READ_METHOD_FOR_KAFKA = "admin.check.read.method.for.kafka";

  // store specific properties
  public static final String PERSISTENCE_TYPE = "persistence.type";

  public static final String KAFKA_CONFIG_PREFIX = ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;
  public static final String KAFKA_BOOTSTRAP_SERVERS = ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
  public static final String SSL_KAFKA_BOOTSTRAP_SERVERS = ApacheKafkaProducerConfig.SSL_KAFKA_BOOTSTRAP_SERVERS;
  public static final String KAFKA_LINGER_MS = ApacheKafkaProducerConfig.KAFKA_LINGER_MS;
  public static final String KAFKA_BATCH_SIZE = ApacheKafkaProducerConfig.KAFKA_BATCH_SIZE;
  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS =
      ApacheKafkaProducerConfig.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS;
  public static final String KAFKA_PRODUCER_RETRIES_CONFIG = ApacheKafkaProducerConfig.KAFKA_PRODUCER_RETRIES_CONFIG;
  public static final String KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS =
      ApacheKafkaProducerConfig.KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS;
  public static final String KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC =
      "kafka.admin.get.topic.config.max.retry.sec";

  /**
   * The time window used by the consumption throttler. Throttler will sum the requests during the time window and
   * compare with the quota accumulated in the time window to see whether the usage exceeds quota or not.
   */
  public static final String KAFKA_FETCH_QUOTA_TIME_WINDOW_MS = "kafka.fetch.quota.time.window.ms";

  public static final String KAFKA_FETCH_QUOTA_BYTES_PER_SECOND = "kafka.fetch.quota.bytes.per.second";
  /**
   * How many records that one server could consume from Kafka at most in one second.
   * If the consume rate reached this quota, the consumption thread will be blocked until there is the available quota.
   */
  public static final String KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND = "kafka.fetch.quota.records.per.second";

  /**
   * How many records that one server could consume from Kafka at most in one second from the specified regions.
   * If the consume rate reached this quota, the consumption thread will be blocked until there is the available quota.
   * The value for this config is read from cluster configs in Zk.
   */
  public static final String SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND =
      "server.kafka.fetch.quota.records.per.second";

  // Unordered throttlers aren't compatible with Shared Kafka Consumer and have no effect when Shared Consumer is used.
  public static final String KAFKA_FETCH_QUOTA_UNORDERED_BYTES_PER_SECOND =
      "kafka.fetch.quota.unordered.bytes.per.second";
  public static final String KAFKA_FETCH_QUOTA_UNORDERED_RECORDS_PER_SECOND =
      "kafka.fetch.quota.unordered.records.per.second";

  // Kafka security protocol
  public static final String KAFKA_SECURITY_PROTOCOL = "security.protocol";

  // Cluster specific configs for controller
  public static final String CONTROLLER_NAME = "controller.name";

  /**
   * Whether to turn on Kafka's log compaction for the store-version topics of hybrid (and real-time only) stores.
   *
   * Will take effect at topic creation time, and when the hybrid config for the store is turned on.
   */
  public static final String KAFKA_LOG_COMPACTION_FOR_HYBRID_STORES = "kafka.log.compaction.for.hybrid.stores";

  /**
   * Whether to turn on Kafka's log compaction for the store-version topics of incremental push stores.
   *
   * Will take effect at topic creation time, and when the incremental push config for the store is turned on.
   */
  public static final String KAFKA_LOG_COMPACTION_FOR_INCREMENTAL_PUSH_STORES =
      "kafka.log.compaction.for.incremental.push.stores";

  /**
   * For log compaction enabled topics, this config will define the minimum time a message will remain uncompacted in the log.
   */
  public static final String KAFKA_MIN_LOG_COMPACTION_LAG_MS = "kafka.min.log.compaction.lag.ms";

  /**
   * The minimum number of in sync replicas to set for store version topics.
   *
   * Will use the Kafka cluster's default if not set.
   */
  public static final String KAFKA_MIN_IN_SYNC_REPLICAS = "kafka.min.in.sync.replicas";

  /**
   * The minimum number of in sync replicas to set for real-time buffer topics.
   *
   * Will use the Kafka cluster's default if not set.
   */
  public static final String KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS = "kafka.min.in.sync.replicas.rt.topics";

  /**
   * The minimum number of in sync replicas to set for admin topics.
   *
   * Will use the Kafka cluster's default if not set.
   */
  public static final String KAFKA_MIN_IN_SYNC_REPLICAS_ADMIN_TOPICS = "kafka.min.in.sync.replicas.admin.topics";

  /**
   * The replication factor to set for store-version topics.
   */
  public static final String KAFKA_REPLICATION_FACTOR = "kafka.replication.factor";

  /**
   * The replication factor to set for real-time buffer topics.
   */
  public static final String KAFKA_REPLICATION_FACTOR_RT_TOPICS = "kafka.replication.factor.rt.topics";

  /**
   * TODO: the following 3 configs will be deprecated after the native replication migration is changed to a two-step
   *       process: 1. Turn on the cluster level config that takes care of newly created stores; 2. Run admin command
   *       to convert existing stores to native replication.
   */
  /**
   * Cluster-level config to enable native replication for all batch-only stores.
   */
  public static final String ENABLE_NATIVE_REPLICATION_FOR_BATCH_ONLY = "enable.native.replication.for.batch.only";

  /**
   * Cluster-level config to enable native replication for all incremental push stores.
   */
  public static final String ENABLE_NATIVE_REPLICATION_FOR_INCREMENTAL_PUSH =
      "enable.native.replication.for.incremental.push";

  /**
   * Cluster-level config to enable native replication for all hybrid stores.
   */
  public static final String ENABLE_NATIVE_REPLICATION_FOR_HYBRID = "enable.native.replication.for.hybrid";

  /**
   * Cluster-level config to enable native replication for new batch-only stores.
   */
  public static final String ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY =
      "enable.native.replication.as.default.for.batch.only";

  /**
   * Cluster-level config to enable native replication for new incremental push stores.
   */
  public static final String ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH =
      "enable.native.replication.as.default.for.incremental.push";

  /**
   * Cluster-level config to enable native replication for new hybrid stores.
   */
  public static final String ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID =
      "enable.native.replication.as.default.for.hybrid";

  /**
   * Cluster-level config to enable active-active replication for new batch-only stores.
   */
  public static final String ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE =
      "enable.active.active.replication.as.default.for.batch.only.store";

  /**
   * Cluster-level config to enable active-active replication for new hybrid stores.
   */
  public static final String ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE =
      "enable.active.active.replication.as.default.for.hybrid.store";

  /**
   * Cluster-level config to enable active-active replication for new incremental push stores.
   */
  public static final String ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORE =
      "enable.active.active.replication.as.default.for.incremental.push.store";

  /**
   * Sets the default for whether or not do schema validation for all stores
   */
  public static final String CONTROLLER_SCHEMA_VALIDATION_ENABLED = "controller.schema.validation.enabled";

  /**
   * Fallback to remain compatible with the old config spelling.
   *
   * Ignored if {@value KAFKA_REPLICATION_FACTOR} is present.
   */
  public static final String DEFAULT_READ_STRATEGY = "default.read.strategy";
  public static final String DEFAULT_OFFLINE_PUSH_STRATEGY = "default.offline.push.strategy";
  public static final String DEFAULT_ROUTING_STRATEGY = "default.routing.strategy";
  public static final String DEFAULT_REPLICA_FACTOR = "default.replica.factor";
  public static final String DEFAULT_NUMBER_OF_PARTITION = "default.partition.count";
  public static final String DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID = "default.partition.count.for.hybrid";
  public static final String DEFAULT_MAX_NUMBER_OF_PARTITIONS = "default.partition.max.count";
  public static final String DEFAULT_PARTITION_SIZE = "default.partition.size";
  public static final String OFFLINE_JOB_START_TIMEOUT_MS = "offline.job.start.timeout.ms";
  public static final String DELAY_TO_REBALANCE_MS = "delay.to.rebalance.ms";
  public static final String MIN_ACTIVE_REPLICA = "min.active.replica";
  public static final String CLUSTER_TO_D2 = "cluster.to.d2";
  public static final String HELIX_SEND_MESSAGE_TIMEOUT_MS = "helix.send.message.timeout.ms";
  public static final String REFRESH_ATTEMPTS_FOR_ZK_RECONNECT = "refresh.attempts.for.zk.reconnect";
  public static final String REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS = "refresh.interval.for.zk.reconnect.ms";
  public static final String KAFKA_READ_CYCLE_DELAY_MS = "kafka.read.cycle.delay.ms";
  public static final String KAFKA_EMPTY_POLL_SLEEP_MS = "kafka.empty.poll.sleep.ms";
  public static final String KAFKA_FETCH_MIN_SIZE_PER_SEC = "kafka.fetch.min.size.per.sec";
  public static final String KAFKA_FETCH_MAX_SIZE_PER_SEC = "kafka.fetch.max.size.per.sec";
  public static final String KAFKA_FETCH_MAX_WAIT_TIME_MS = "kafka.fetch.max.wait.time.ms";
  public static final String KAFKA_FETCH_PARTITION_MAX_SIZE_PER_SEC = "kafka.fetch.partition.max.size.per.sec";

  // Controller specific configs
  public static final String CONTROLLER_CLUSTER_ZK_ADDRESSS = "controller.cluster.zk.address";
  /** Cluster name for all parent controllers */
  public static final String CONTROLLER_CLUSTER = "controller.cluster.name";

  /** List of forbidden admin paths */
  public static final String CONTROLLER_DISABLED_ROUTES = "controller.cluster.disabled.routes";

  /**
   * The retention policy for deprecated topics, which includes topics for both failed jobs and retired store versions.
   */
  public static final String DEPRECATED_TOPIC_RETENTION_MS = "deprecated.topic.retention.ms";

  /**
   * This config is to indicate the max retention policy we have setup for deprecated jobs currently and in the past.
   * And this is used to decide whether the topic is deprecated or not during topic cleanup.
   *
   * The reason to have this config instead of using {@link #DEPRECATED_TOPIC_RETENTION_MS} since the retention
   * policy for deprecated jobs could change from time to time, and we need to use a max threshold to cover all the
   * historical deprecated job topics.
   */
  public static final String DEPRECATED_TOPIC_MAX_RETENTION_MS = "deprecated.topic.max.retention.ms";

  /**
   * Sleep interval between each topic list fetch from Kafka ZK in TopicCleanup service.
   * We don't want to hit Kafka Zookeeper too frequently.
   */
  public static final String TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS =
      "topic.cleanup.sleep.interval.between.topic.list.fetch.ms";
  public static final String TOPIC_CLEANUP_DELAY_FACTOR = "topic.cleanup.delay.factor";
  public static final String TOPIC_CLEANUP_SEND_CONCURRENT_DELETES_REQUESTS =
      "topic.cleanup.send.concurrent.delete.requests.enabled";

  /**
   * Sleep interval for polling topic deletion status from ZK.
   */
  public static final String TOPIC_DELETION_STATUS_POLL_INTERVAL_MS = "topic.deletion.status.poll.interval.ms";

  /**
   * The following config is to control the default retention time in milliseconds if it is not specified in store level.
   */
  public static final String CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS =
      "controller.backup.version.default.retention.ms";

  /**
   * The following config is to control whether to enable backup version cleanup based on retention policy or not at cluster level.
   */
  public static final String CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED =
      "controller.backup.version.retention.based.cleanup.enabled";

  /**
   * Whether to automatically create zk shared metadata system store in Controller or not
   */
  public static final String CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED =
      "controller.zk.shared.metadata.system.schema.store.auto.creation.enabled";

  /**
   * Whether controller should enforce SSL.
   */
  public static final String CONTROLLER_ENFORCE_SSL = "controller.enforce.ssl";

  /**
   * Whether child controllers will directly consume the source admin topic in the parent Kafka cluster.
   */
  public static final String ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED = "admin.topic.remote.consumption.enabled";

  /**
   * This config defines the source region name of the admin topic
   */
  public static final String ADMIN_TOPIC_SOURCE_REGION = "admin.topic.source.region";

  /**
   * This following config defines whether admin consumption should be enabled or not, and this config will only control the behavior in Child Controller.
   */
  public static final String CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED =
      "child.controller.admin.topic.consumption.enabled";

  /**
   * This config defines the source region of aggregate hybrid store real-time data when native replication is enabled
   */
  public static final String AGGREGATE_REAL_TIME_SOURCE_REGION = "aggregate.real.time.source.region";

  /**
   * Whether stores are allowed to be migrated from/to a specific cluster.
   * The value for this config is read from cluster configs in Zk.
   */
  public static final String ALLOW_STORE_MIGRATION = "allow.store.migration";

  /**
   * Whether a cluster in a data center could be wiped. Default is false.
   */
  public static final String ALLOW_CLUSTER_WIPE = "allow.cluster.wipe";

  /**
   * Whether the controller is in Azure fabric. Default is false.
   */
  public static final String CONTROLLER_IN_AZURE_FABRIC = "controller.in.azure.fabric";

  /**
   * Whether to enable graveyard cleanup for batch-only store at cluster level. Default is false.
   */
  public static final String CONTROLLER_STORE_GRAVEYARD_CLEANUP_ENABLED = "controller.store.graveyard.cleanup.enabled";

  /**
   * When store graveyard cleanup is enabled, delete the graveyard znode if it has not been changed for a specific time.
   * Default is 0 min.
   */
  public static final String CONTROLLER_STORE_GRAVEYARD_CLEANUP_DELAY_MINUTES =
      "controller.store.graveyard.cleanup.delay.minutes";

  /**
   * Sleep interval between each graveyard list fetch from ZK in StoreGraveyardCleanup service. Default is 15 min.
   * */
  public static final String CONTROLLER_STORE_GRAVEYARD_CLEANUP_SLEEP_INTERVAL_BETWEEN_LIST_FETCH_MINUTES =
      "controller.store.graveyard.cleanup.sleep.interval.between.list.fetch.minutes";

  // Server specific configs
  public static final String LISTENER_PORT = "listener.port";

  public static final String LISTENER_HOSTNAME = "listener.hostname";

  public static final String DATA_BASE_PATH = "data.base.path";
  public static final String AUTOCREATE_DATA_PATH = "autocreate.data.path";
  public static final String ENABLE_SERVER_ALLOW_LIST = "enable.server.allowlist";
  public static final String MAX_ONLINE_OFFLINE_STATE_TRANSITION_THREAD_NUMBER = "max.state.transition.thread.number";
  public static final String MAX_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER =
      "max.leader.follower.state.transition.thread.number";
  public static final String MAX_FUTURE_VERSION_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER =
      "max.future.version.leader.follower.state.transition.thread.number";
  public static final String LEADER_FOLLOWER_STATE_TRANSITION_THREAD_POOL_STRATEGY =
      "leader.follower.state.transition.thread.pool.strategy";
  public static final String STORE_WRITER_NUMBER = "store.writer.number";
  public static final String SORTED_INPUT_DRAINER_SIZE = "sorted.input.drainer.size";
  public static final String UNSORTED_INPUT_DRAINER_SIZE = "unsorted.input.drainer.size";
  public static final String STORE_WRITER_BUFFER_AFTER_LEADER_LOGIC_ENABLED =
      "store.writer.buffer.after.leader.logic.enabled";
  public static final String STORE_WRITER_BUFFER_MEMORY_CAPACITY = "store.writer.buffer.memory.capacity";
  public static final String STORE_WRITER_BUFFER_NOTIFY_DELTA = "store.writer.buffer.notify.delta";
  public static final String SERVER_REST_SERVICE_STORAGE_THREAD_NUM = "server.rest.service.storage.thread.num";
  public static final String SERVER_NETTY_IDLE_TIME_SECONDS = "server.netty.idle.time.seconds";
  public static final String SERVER_MAX_REQUEST_SIZE = "server.max.request.size";
  public static final String SERVER_SOURCE_TOPIC_OFFSET_CHECK_INTERVAL_MS =
      "server.source.topic.offset.check.interval.ms";
  public static final String SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS =
      "server.netty.graceful.shutdown.period.seconds";
  public static final String SERVER_NETTY_WORKER_THREADS = "server.netty.worker.threads";
  public static final String SSL_TO_KAFKA = ApacheKafkaProducerConfig.SSL_TO_KAFKA;
  public static final String SERVER_COMPUTE_THREAD_NUM = "server.compute.thread.num";
  public static final String HYBRID_QUOTA_ENFORCEMENT_ENABLED = "server.hybrid.quota.enforcement.enabled";
  public static final String SERVER_DATABASE_MEMORY_STATS_ENABLED = "server.database.memory.stats.enabled";

  public static final String ROUTER_MAX_READ_CAPACITY = "router.max.read.capacity";
  public static final String ROUTER_QUOTA_CHECK_WINDOW = "router.quota.check.window";

  public static final String SERVER_REMOTE_INGESTION_REPAIR_SLEEP_INTERVAL_SECONDS =
      "server.remote.ingestion.repair.sleep.interval.seconds";
  /**
   * Whether to enable epoll in rest service layer.
   * This will be a best-effort since epoll support is only available in Linux, not Mac.
   */
  public static final String SERVER_REST_SERVICE_EPOLL_ENABLED = "server.rest.service.epoll.enabled";
  /**
   * Database sync per bytes for transactional mode.
   * This parameter will impact the sync frequency of database after batch push.
   * For BDB-JE transactional mode, it won't matter since BDB-JE will persist every update in the database right away;
   * For RocksDB transactional mode, it will impact the flush frequency of memtable to SST file, and normally we would
   * like to have this config to be comparable to the memtable size;
   *
   * Negative value will disable this threshold.
   */
  public static final String SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE =
      "server.database.sync.bytes.interval.for.transactional.mode";
  /**
   * Database sync per bytes for deferred-write mode.
   * This parameter will impact the sync frequency of database during batch push.
   * For BDB-JE deferred-write mode, it will impact the sync frequency, but BDB-JE will do auto-flush if the memory is full;
   * For RocksDB deferred-write mode, it will decide the file size of each SST file since every sync invocation will
   * generate a new SST file;
   *
   * Negative value will disable this threshold.
   */
  public static final String SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE =
      "server.database.sync.bytes.interval.for.deferred.write.mode";

  /**
   * When load balance happens, a replica could be moved to another storage node.
   * When dropping the existing replica through Helix state transition: 'ONLINE' -> 'OFFLINE' and 'OFFLINE' -> 'DROPPED',
   * a race condition could happen since Router in-memory partition assignment update through Zookeeper
   * is independent from database drop in storage node, so Router could possibly forward the request to the storage node,
   * which has just dropped the partition.
   *
   * To mitigate this issue, we will add a delay in state transition: 'OFFLINE' -> 'DROPPED' to drain all the incoming
   * requests to the to-drop partition, and we will enable error-retry on Router as well.
   */
  public static final String SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS =
      "server.partition.graceful.drop.time.in.seconds";

  /**
   * When a BDB partition is dropped, the disk space is not released immediately; a checkpoint is needed to release the disk space;
   * so a cleaner thread is spawned for the entire storage service; the cleaner thread will wake up every few hours and check
   * whether it needs to do a checkpoint; if so, clean up each store sequentially.
   */
  public static final String SERVER_LEAKED_RESOURCE_CLEAN_UP_INTERVAL_IN_MINUTES =
      "server.leaked.resource.clean.up.interval.in.minutes";

  /**
   * Set to true to enable enforcement of quota by the storage node
   */
  public static final String SERVER_QUOTA_ENFORCEMENT_ENABLED = "server.quota.enforcement.enabled";

  /**
   * Set to true to enable disk quota usage based on partitions assignment reported by the storage node
   */
  public static final String SEVER_CALCULATE_QUOTA_USAGE_BASED_ON_PARTITIONS_ASSIGNMENT_ENABLED =
      "server.calculate.quota.usage.based.on.partitions.assignment.enabled";

  /**
   * Number of Read Capacity Units per second that the node can handle across all stores.
   */
  public static final String SERVER_NODE_CAPACITY_RCU = "server.node.capacity.rcu.per.second";

  /**
   * This config is used to control the maximum records returned by every poll request.
   * So far, Store Ingestion is throttling per poll, so if the configured value is too big,
   * the throttling could be inaccurate and it may impact GC as well.
   *
   * We should try to avoid too many long-lasting objects in JVM to minimize GC overhead.
   */
  public static final String SERVER_KAFKA_MAX_POLL_RECORDS = "server.kafka.max.poll.records";

  /**
   * This config is used to control how many times Kafka consumer would retry polling during ingestion
   * when hitting {@literal org.apache.kafka.common.errors.RetriableException}.
   */
  public static final String SERVER_KAFKA_POLL_RETRY_TIMES = "server.kafka.poll.retry.times";

  /**
   * This config is used to control the backoff time between Kafka consumer poll retries.
   */
  public static final String SERVER_KAFKA_POLL_RETRY_BACKOFF_MS = "server.kafka.poll.backoff.ms";

  /**
   * This config decides the frequency of the disk health check; the disk health check service writes
   * 64KB data to a temporary file in the database directory and read from the file for each health check.
   */
  public static final String SERVER_DISK_HEALTH_CHECK_INTERVAL_IN_SECONDS =
      "server.disk.health.check.interval.in.seconds";

  /**
   * When there is an actual disk failure, health check operation would hang, so this config decides how fast the
   * servers will start reporting unhealthy after the health check stop updating status; however, in order to
   * reduce the possibility of false alerts (for example, the health check updates can be delayed by GC), we couldn't
   * set the timeout too small. Currently by default, the timeout is 30 seconds.
   */
  public static final String SERVER_DISK_HEALTH_CHECK_TIMEOUT_IN_SECONDS =
      "server.disk.health.check.timeout.in.seconds";

  /**
   * This config is used to enable/disable the disk health check service.
   */
  public static final String SERVER_DISK_HEALTH_CHECK_SERVICE_ENABLED = "server.disk.health.check.service.enabled";

  /**
   * Whether to enable fast-avro in compute request path.
   */
  public static final String SERVER_COMPUTE_FAST_AVRO_ENABLED = "server.compute.fast.avro.enabled";

  /**
   * Whether to enable parallel lookup for batch-get.
   */
  public static final String SERVER_ENABLE_PARALLEL_BATCH_GET = "server.enable.parallel.batch.get";

  /**
   * Chunk size of each task for parallel lookup of batch-get.
   */
  public static final String SERVER_PARALLEL_BATCH_GET_CHUNK_SIZE = "server.parallel.batch.get.chunk.size";

  /**
   * The request early termination threshold map:
   * The key will be store name, and the value will be the actual threshold.
   * This config is temporary, and in the long run, we will ask Venice Client to pass the actual timeout to the backend.
   */
  public static final String SERVER_STORE_TO_EARLY_TERMINATION_THRESHOLD_MS_MAP =
      "server.store.to.early.termination.threshold.ms.map";

  /**
   * The following config is used to control the maximum database lookup requests queued, when the queue is full,
   * server will propagate the back pressure to the caller.
   */
  public static final String SERVER_DATABASE_LOOKUP_QUEUE_CAPACITY = "server.database.lookup.queue.capacity";

  /**
   * Check @{@link #SERVER_DATABASE_LOOKUP_QUEUE_CAPACITY} for the explanation.
   * The following config is specifically for compute request.
   */
  public static final String SERVER_COMPUTE_QUEUE_CAPACITY = "server.compute.queue.capacity";

  /**
   * Check the available types in {@literal com.linkedin.venice.config.BlockingQueueType}
   */
  public static final String SERVER_BLOCKING_QUEUE_TYPE = "server.blocking.queue.type";

  /**
   * This config is used to control whether openssl is enabled for Kafka consumers in server.
   */
  public static final String SERVER_ENABLE_KAFKA_OPENSSL = "server.enable.kafka.openssl";

  /**
   * This config is used to control how much time Server will wait for connection warming from Routers.
   * This is trying to avoid availability issue when router connection warming happens when Server restarts.
   * In theory, this config should be equal to or bigger than {@link #ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_NEW_INSTANCE_DELAY_JOIN_MS}.
   */
  public static final String SERVER_ROUTER_CONNECTION_WARMING_DELAY_MS = "server.router.connection.warming.delay.ms";

  /**
   * Consumer pool size per Kafka cluster.
   */
  public static final String SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER =
      "server.consumer.pool.size.per.kafka.cluster";

  /**
   * Whether to enable partition wise balanced shared consumer assignment.
   */
  public static final String SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY = "server.shared.consumer.assignment.strategy";

  /**
   * Whether to enable leaked resource cleanup in storage node.
   * Right now, it only covers leaked storage partitions on disk.
   */
  public static final String SERVER_LEAKED_RESOURCE_CLEANUP_ENABLED = "server.leaked.resource.cleanup.enabled";

  /**
   * The delay serving of the newly started storage node.
   * The reason to have this config is that we noticed a high GC pause for some time because of connection warming or initializing the
   * internal components.
   * We will need to do some experiment to find the right value.
   */
  public static final String SERVER_DELAY_REPORT_READY_TO_SERVE_MS = "server.delay.report.ready.to.serve.ms";

  /**
   * Ingestion mode in target storage instance.
   * This will be applied to Da Vinci and Storage Node.
   */
  public static final String SERVER_INGESTION_MODE = "server.ingestion.mode";

  /**
   * Unsubscribe from kakfa topic once a batch-store push finishes
   */
  public static final String SERVER_UNSUB_AFTER_BATCHPUSH = "server.unsub.after.batch.push";

  /**
   * Use a seprate drainer queue for sorted ingestion and un-sorted ingestion.
   */
  public static final String SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED =
      "server.dedicated.drainer.queue.for.sorted.input.enabled";

  /**
   * A boolean config to specify if we are using Da Vinci client for ingestion. This config will be parsed by
   * isDaVinciConfig variable in VeniceServerConfig. By default, it is false (use Venice Server)
   */
  public static final String INGESTION_USE_DA_VINCI_CLIENT = "ingestion.use.da.vinci.client";

  /**
   * Number of retries allowed for stopConsumptionAndWait() API in StoreIngestionService.
   */
  public static final String SERVER_STOP_CONSUMPTION_WAIT_RETRIES_NUM = "server.stop.consumption.wait.retries.num";

  /**
   * Service listening port number for main ingestion service.
   */
  public static final String SERVER_INGESTION_ISOLATION_SERVICE_PORT = "server.ingestion.isolation.service.port";

  /**
   * Service listening port number for forked ingestion process.
   */
  public static final String SERVER_INGESTION_ISOLATION_APPLICATION_PORT =
      "server.ingestion.isolation.application.port";

  public static final String SERVER_DB_READ_ONLY_FOR_BATCH_ONLY_STORE_ENABLED =
      "server.db.read.only.for.batch.only.store.enabled";
  /**
   * A list of fully-qualified class names of all stats classes that needs to be initialized in isolated ingestion process,
   * separated by comma. This config will help isolated ingestion process to register extra stats needed for monitoring,
   * for example: JVM GC/Memory stats. All the classes defined here will be extending {@link com.linkedin.venice.stats.AbstractVeniceStats},
   * and will take {@link io.tehuti.metrics.MetricsRepository} as the only parameter in their constructor.
   */
  public static final String SERVER_INGESTION_ISOLATION_STATS_CLASS_LIST =
      "server.ingestion.isolation.stats.class.list";

  public static final String SERVER_INGESTION_ISOLATION_SSL_ENABLED = "server.ingestion.isolation.ssl.enabled";

  public static final String SERVER_INGESTION_ISOLATION_ACL_ENABLED = "server.ingestion.isolation.acl.enabled";

  public static final String SERVER_INGESTION_ISOLATION_PRINCIPAL_NAME = "server.ingestion.isolation.principal.name";

  /**
   * A list of JVM arguments for forked child process, separated by semicolon.
   */
  public static final String SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST = "server.forked.process.jvm.arg.list";

  /**
   * Timeout for connection between main process and forked ingestion process. If heartbeat is not refreshed within this
   * timeout, both processes should act to reconstruct the state in order to restore connection and service.
   */
  public static final String SERVER_INGESTION_ISOLATION_CONNECTION_TIMEOUT_SECONDS =
      "server.ingestion.isolation.connection.timeout.seconds";

  /**
   * Timeout for single ingestion command request sent from main process to forked ingestion process.
   */
  public static final String SERVER_INGESTION_ISOLATION_REQUEST_TIMEOUT_SECONDS =
      "server.ingestion.isolation.request.timeout.seconds";

  /**
   * Timeout for single heartbeat request sent from main process to forked ingestion process.
   */
  public static final String SERVER_INGESTION_ISOLATION_HEARTBEAT_REQUEST_TIMEOUT_SECONDS =
      "server.ingestion.isolation.heartbeat.request.timeout.seconds";

  /**
   * Timeout for single metric request sent from main process to forked ingestion process.
   */
  public static final String SERVER_INGESTION_ISOLATION_METRIC_REQUEST_TIMEOUT_SECONDS =
      "server.ingestion.isolation.metric.request.timeout.seconds";

  /**
   * whether to enable checksum verification in the ingestion path from kafka to database persistency. If enabled it will
   * keep a running checksum for all and only PUT kafka data message received in the ingestion task and periodically
   * verify it against the key/values saved in the database persistency layer.
   */
  public static final String SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED =
      "server.database.checksum.verification.enabled";

  /**
   * Any server config that start with "server.local.consumer.config.prefix" will be used as a customized consumer config
   * for local consumer.
   */
  public static final String SERVER_LOCAL_CONSUMER_CONFIG_PREFIX = "server.local.consumer.config.prefix.";

  /**
   * Any server config that start with "server.remote.consumer.config.prefix" will be used as a customized consumer config
   * for remote consumer.
   */
  public static final String SERVER_REMOTE_CONSUMER_CONFIG_PREFIX = "server.remote.consumer.config.prefix.";

  /**
   * Whether to enable to check the RocksDB storage type used to open the RocksDB storage engine and how it was built.
   * Having different storage types (BlockBasedTable and PlainTable) in read ops and write ops may lead to corruption of
   * RocksDB storage and crash of servers.
   */
  public static final String SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED =
      "server.rocksdb.storage.config.check.enabled";

  /**
   * This config is used to control how much time we should wait before cleaning up the corresponding ingestion task
   * when an non-existing topic is discovered.
   * The reason to introduce this config is that `consumer#listTopics` could only guarantee eventual consistency, so
   * `consumer#listTopics` not returning the topic doesn't mean the topic doesn't exist in Kafka.
   * If `consumer#listTopics` still doesn't return the topic after the configured delay, Venice SN will unsubscribe the topic,
   * and fail the corresponding ingestion job.
   */
  public static final String SERVER_SHARED_CONSUMER_NON_EXISTING_TOPIC_CLEANUP_DELAY_MS =
      "server.shared.cosnumer.non.existing.topic.cleanup.delay.ms";

  /**
   * This config will determine whether live update will be suppressed. When the feature is turned on, ingestion will stop
   * once a partition is ready to serve; after Da Vinci client restarts or server restarts, if local data exists, ingestion
   * will not start in Da Vinci or report ready-to-serve immediately without ingesting new data in Venice.
   */
  public static final String FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS =
      "freeze.ingestion.if.ready.to.serve.or.local.data.exists";

  /**
   * Whether to enable shared kafka producer in storage node.
   */
  public static final String SERVER_SHARED_KAFKA_PRODUCER_ENABLED = "server.shared.kafka.producer.enabled";

  /**
   * Shared kafka producer pool size per Kafka cluster.
   */
  public static final String SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER =
      "server.kafka.producer.pool.size.per.kafka.cluster";

  /**
   * a comma seperated list of kafka producer metrics that will be reported.
   * For ex. "outgoing-byte-rate,record-send-rate,batch-size-max,batch-size-avg,buffer-available-bytes,buffer-exhausted-rate"
   */
  public static final String KAFKA_PRODUCER_METRICS = "list.of.producer.metrics.from.kafka";

  /**
   * Whether to print logs that are used for troubleshooting only.
   */
  public static final String SERVER_DEBUG_LOGGING_ENABLED = "server.debug.logging.enabled";

  /**
   * Number of value schemas for which fast avro classes are generated for read-compute stores before push completion
   * is reported.
   */
  public static final String SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP = "server.num.schema.fast.class.warmup";

  /**
   * Timeout duration of schema generation for fast class warmup period
   */
  public static final String SERVER_SCHEMA_FAST_CLASS_WARMUP_TIMEOUT = "server.schema.fast.class.warmup.timeout";

  // Router specific configs
  // TODO the config names are same as the names in application.src, some of them should be changed to keep consistent
  // TODO with controller and server.
  public static final String LISTENER_SSL_PORT = "listener.ssl.port";
  public static final String HEARTBEAT_TIMEOUT = "heartbeat.timeout";
  public static final String HEARTBEAT_CYCLE = "heartbeat.cycle";
  public static final String MAX_READ_CAPACITY = "max.read.capacity";
  public static final String SSL_TO_STORAGE_NODES = "sslToStorageNodes";
  /**
   * After this amount of time, DDS Router will retry once for the slow storage node request.
   *
   * Practically, we need to manually select the threshold (e.g. P95) for retrying based on latency metrics.
   */
  public static final String ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS =
      "router.long.tail.retry.for.single.get.threshold.ms";

  /**
   * After this amount of time, DDS Router will retry once for the slow storage node request.
   *
   * The configured format will be like this way:
   * "1-10:20,11-50:50,51-200:80,201-:1000"
   *
   * Let me explain the config by taking one example:
   * If the request key count for a batch-get request is '32', and it fails into this key range: [11-50], so the retry
   * threshold for this batch-get request is 50ms.
   *
   * That is a limitation here:
   * The retry threshold is actually for each scatter-gather request, but this config is not strictly with the actual key count
   * inside each scatter-gather request, which means even if there is only one key in a scatter-gather request with
   * the above example, Router will wait for 50ms to retry this scatter-gather request.
   *
   * For now, it is not big issue since for now we mostly want to use this config to skip the storage node, which
   * is experiencing long GC pause.
   * So coarse-grained config should be good enough.
   *
   */
  public static final String ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS =
      "router.long.tail.retry.for.batch.get.threshold.ms";

  /**
   * Whether to enable smart long tail retry logic, and this logic is only useful for batch-get retry currently.
   * This feature is used to avoid the unnecessary retries in the following scenarios:
   * 1. Router is suffering long GC pause, no matter whether Storage Node is fast or not;
   * 2. The retried Storage Node is slow according to the original request;
   *
   * For case 1, unnecessary retries will make Router GC behavior even worse;
   * For case 2, unnecessary retries to the slow Storage Node will make the slow Storage Node even slower, and the
   * overall latency won't be improved;
   *
   * For case 1, here is how smart retry works:
   * 1. When the delay between the retry request and the original request is over {@link #ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS}
   *   + {@link #ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS}, smart retry logic will treat the current Router to be
   *   in bad state (long GC pause or too busy), the retry request will be aborted;
   * 2.{@link #ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS} is the way to measure whether Router is in good state or not,
   *   and need to be tuned in prod;
   *
   * For case 2, the retry request will be aborted if the original request to the same storage node hasn't returned,
   * and the slowness measurement is inside one request when scatter-gathering.
   */
  public static final String ROUTER_SMART_LONG_TAIL_RETRY_ENABLED = "router.smart.long.tail.retry.enabled";

  /**
   * This config is used to tune the smart long-tail retry logic to avoid unnecessary retries,
   * check more details: {@link #ROUTER_SMART_LONG_TAIL_RETRY_ENABLED}
   */
  public static final String ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS =
      "router.smart.long.tail.retry.abort.threshold.ms";

  /**
   * This config is used to limit the maximum retries in route unit.
   * In large batch-get/compute cluster, when enabling long-tail retry, in the worst scenarios, Router could trigger a
   * retry storm since each route could retry independently.
   * This config is used to specify the maximum retry in route unit.
   * If the configured value is 1, it means the current request will at most one route.
   * This could mitigate the latency issue in most of the case since the chance to have multiple slow storage nodes is low,
   * also even with unlimited retries, it won't help since multiple replicas for the same partition are in a degraded state.
   */
  public static final String ROUTER_LONG_TAIL_RETRY_MAX_ROUTE_FOR_MULTI_KEYS_REQ =
      "router.long.tail.retry.max.route.for.multi.keys.req";

  /**
   * The max key count allowed in one multi-get request.
   * For now, it is configured in host level, and we could consider to configure it in store level.
   */
  public static final String ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ = "router.max.key_count.in.multiget.req";
  public static final String ROUTER_CONNECTION_LIMIT = "router.connection.limit";
  /**
   * The http client pool size being used in one Router;
   */
  public static final String ROUTER_HTTP_CLIENT_POOL_SIZE = "router.http.client.pool.size";

  /**
   * Config to control the Netty IO thread count for the Router Server
   */
  public static final String ROUTER_IO_WORKER_COUNT = "router.io.worker.count";
  /**
   * The max connection number per route (to one storage node);
   */
  public static final String ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE = "router.max.outgoing.connection.per.route";
  /**
   * The max connection number in one Router to storage nodes;
   */
  public static final String ROUTER_MAX_OUTGOING_CONNECTION = "router.max.outgoing.connection";

  /**
   * Enable per router per storage node throttler by distributing the store quota among the partitions and replicas.
   */
  public static final String ROUTER_PER_STORAGE_NODE_THROTTLER_ENABLED = "router.per.storage.node.throttler.enabled";

  /**
   * This config is used to bound the pending request.
   * Without this config, the accumulated requests in Http Async Client could grow unlimitedly,
   * which would put Router in a non-recoverable state because of long GC pause introduced
   * by the increasing memory usage.
   *
   * If the incoming request exceeds this configured threshold, Router will return 503 (Service Unavailable).
   */
  public static final String ROUTER_MAX_PENDING_REQUEST = "router.max.pending.request";

  /**
   * This config is used to define the routing strategy for multi-key requests.
   * Please check {@literal VeniceMultiKeyRoutingStrategy} to find available routing strategy.
   */
  public static final String ROUTER_MULTI_KEY_ROUTING_STRATEGY = "router.multi.key.routing.strategy";

  /**
   * The Helix virtual group field name in domain, and the allowed values: {@link com.linkedin.venice.helix.HelixInstanceConfigRepository#GROUP_FIELD_NAME_IN_DOMAIN}
   * and {@link com.linkedin.venice.helix.HelixInstanceConfigRepository#ZONE_FIELD_NAME_IN_DOMAIN}.
   */
  public static final String ROUTER_HELIX_VIRTUAL_GROUP_FIELD_IN_DOMAIN = "router.helix.virtual.group.field.in.domain";

  /**
   * Helix group selection strategy when Helix assisted routing is enabled.
   * Available strategies listed here: {@literal HelixGroupSelectionStrategyEnum}.
   */
  public static final String ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY =
      "router.helix.assisted.routing.group.selection.strategy";

  /**
   * The buffer we will add to the per storage node read quota. E.g 0.5 means 50% extra quota.
   */
  public static final String ROUTER_PER_STORAGE_NODE_READ_QUOTA_BUFFER = "router.per.storage.node.read.quota.buffer";

  public static final String ROUTER_PER_STORE_ROUTER_QUOTA_BUFFER = "router.per.store.router.quota.buffer";

  /**
   * Whether to enable customized dns cache in router or not.
   * This is mostly to address slow DNS lookup issue.
   */
  public static final String ROUTER_DNS_CACHE_ENABLED = "router.dns.cache.enabled";

  /**
   * The host matching the configured host pattern will be cached if {@link #ROUTER_DNS_CACHE_ENABLED} is true.
   */
  public static final String ROUTE_DNS_CACHE_HOST_PATTERN = "router.dns.cache.host.pattern";

  /**
   * Refresh interval of cached dns entries if {@link #ROUTER_DNS_CACHE_ENABLED} is true.
   */
  public static final String ROUTER_DNS_CACHE_REFRESH_INTERVAL_MS = "router.dns.cache.refresh.interval.ms";

  /**
   * Whether the router use netty http client or apache http async client
   */
  public static final String ROUTER_STORAGE_NODE_CLIENT_TYPE = "router.storage.node.client.type";

  /**
   * Netty graceful shutdown period considering the following factors:
   * 1. D2 de-announcement could take some time;
   * 2. Client could take some  time to receive/apply the zk update event from D2 server about router shutdown;
   * 3. Router needs some time to handle already-received client requests;
   */
  public static final String ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS =
      "router.netty.graceful.shutdown.period.seconds";

  public static final String ROUTER_CLIENT_DECOMPRESSION_ENABLED = "router.client.decompression.enabled";

  /**
   * Whether to enable fast-avro in router;
   */
  public static final String ROUTER_COMPUTE_FAST_AVRO_ENABLED = "router.compute.fast.avro.enabled";

  /**
   * Socket timeout config for the connection manager from router to server
   */
  public static final String ROUTER_SOCKET_TIMEOUT = "router.socket.timeout";

  /**
   * Timeout for building a new connection from router to server
   */
  public static final String ROUTER_CONNECTION_TIMEOUT = "router.connection.timeout";

  /**
   * Whether to enable the cleanup of the idle connections to storage node.
   * Recently, we are seeing latency spike because of new connection setup, and we hope the total available connections will be
   * more stable by disabling the idle connection cleanup.
   * The potential long-term solutions could be connection warm-up for HTTP/1.1 or adopting HTTP/2
   */
  public static final String ROUTER_IDLE_CONNECTION_TO_SERVER_CLEANUP_ENABLED =
      "router.idle.connection.to.server.cleanup.enabled";

  /**
   * The idle threshold for cleaning up the connections to storage node.
   */
  public static final String ROUTER_IDLE_CONNECTION_TO_SERVER_CLEANUP_THRESHOLD_MINS =
      "router.idle.connection.to.server.cleanup.threshold.mins";

  /**
   * The following config controls how long the server with full pending queue will be taken OOR.
   */
  public static final String ROUTER_FULL_PENDING_QUEUE_SERVER_OOR_MS = "router.full.pending.queue.server.oor.ms";

  /**
   * Connection warming feature for httpasynclient.
   * So far, it only works when Router starts and runs in http-client-per-route mode, and it will try to warm up {@link #ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE}
   * connections per route.
   */
  public static final String ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED =
      "router.httpasyncclient.connection.warming.enabled";

  /**
   * When Router starts, for a given route, the following config controls the warming up speed to minimize the impact to storage nodes.
   */
  public static final String ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_SLEEP_INTERVAL_MS =
      "router.httpasyncclient.connection.warming.sleep.interval.ms";

  /**
   * When the available connections in an httpasyncclient is below the low water mark, the connection warming service will try to
   * spin up a new client to replace it.
   * In theory, this config must be lower than  {@link #ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE}.
   */
  public static final String ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_LOW_WATER_MARK =
      "router.httpasyncclient.connection.warming.low.water.mark";

  /**
   * Connection warming executor thread num.
   */
  public static final String ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_EXECUTOR_THREAD_NUM =
      "router.httpasyncclient.connection.warming.executor.thread.num";

  /**
   * For the new instance (Storage Node) detected by Router, the following config defines how much delay because of connection warming it could tolerate.
   * If the connection warming takes longer than it, Router will put it in to serve online traffic by creating a new client without connection warming.
   */
  public static final String ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_NEW_INSTANCE_DELAY_JOIN_MS =
      "router.httpasyncclient.connection.warming.new.instance.delay.join.ms";

  /**
   * This config is used to control the socket timeout for connection warming requests.
   * In some cases, we would like to have different(maybe longer timeout) than the regular requests considering the deployment procedure,
   * and the connection warming requests could be very instensive.
   */
  public static final String ROUTER_HTTPAYSNCCLIENT_CONNECTION_WARMING_SOCKET_TIMEOUT_MS =
      "router.httpasyncclient.connection.warming.socket.timeout.ms";

  /**
   * Whether to enable async start in Router startup procedure.
   * The reason to introduce this feature is that in some env, the dependent services could be started out of order.
   *
   * IMPORTANT: enabling this feature won't guarantee that a successful restarted Router will be in a healthy state,
   * since async start will exit later if it detects any errors.
   */
  public static final String ROUTER_ASYNC_START_ENABLED = "router.async.start.enabled";

  /**
   * Venice uses a helix cluster to assign controllers to each named venice cluster.  This is the number of controllers
   * assigned to each venice cluster.  Should normally be 3; one leader controller and 2 standby controllers.
   * */
  public static final String CONTROLLER_CLUSTER_REPLICA = "controller.cluster.replica";

  /**
   * The time window in ms used to throttle the Kafka topic creation, during the time window, only 1 topic is allowed to
   * be created.
   */
  public static final String TOPIC_CREATION_THROTTLING_TIME_WINDOW_MS = "topic.creation.throttling.time.window.ms";

  /** Timeout for create topic and delete topic operations. */
  public static final String TOPIC_MANAGER_KAFKA_OPERATION_TIMEOUT_MS = "topic.manager.kafka.operation.timeout.ms";

  /**
   * This is the minimum number of Kafka topics that are guaranteed to be preserved by the leaky topic clean
   * up routine. The topics with the highest version numbers will be favored by this preservative behavior.
   * All other topics (i.e.: those with smaller version numbers) which Venice does not otherwise know about
   * from its metadata will be considered leaked resources and thus be eligible for clean up.
   *
   * A value greater than zero is recommended for Storage Node to not run into UNKNOWN_TOPIC_OR_PARTITION issues
   *
   * N.B.: A known limitation of this preservation setting is that during store deletion, if a topic has been
   * leaked recently due to an aborted push, then there is an edge case where that topic may leak forever.
   * This leak does not happen if the latest store-versions are successful pushes, rather than failed ones.
   * Furthermore, if a store with the same name is ever re-created, then the clean up routine would resume
   * and clean up the older leaky topics successfully. This edge case is deemed a small enough concern for
   * now, though it could be addressed with a more significant redesign of the replication pipeline.
   */
  public static final String MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE =
      "min.number.of.unused.kafka.topics.to.preserve";

  /**
   * This is the number of fully-functional store-versions we wish to maintain. All resources of these versions
   * will be preserved (Helix resource, Storage Node data, Kafka topic, replication streams, etc.), and a swap
   * to these versions should be possible at all times.
   *
   * This setting must be set to 1 or greater.
   */
  public static final String MIN_NUMBER_OF_STORE_VERSIONS_TO_PRESERVE = "min.number.of.store.versions.to.preserve";

  /** Whether current controller is parent or not */
  public static final String CONTROLLER_PARENT_MODE = "controller.parent.mode";

  /**
   * This config is used to control how many errored topics we are going to keep in parent cluster.
   * This is mostly used to investigate the Kafka missing message issue.
   * If the issue gets resolved, we could change this config to be '0'.
   */
  public static final String PARENT_CONTROLLER_MAX_ERRORED_TOPIC_NUM_TO_KEEP =
      "parent.controller.max.errored.topic.num.to.keep";

  /**
   * Only required when controller.parent.mode=true
   * This prefix specifies the location of every child cluster that is being fed by this parent cluster.
   * The format for key/value would be like "key=child.cluster.url.dc-0, value=url1;url2;url3"
   * the cluster name should be human-readable, ex: dc-0
   * the url should be of the form http://host:port
   * Note that every cluster name supplied must also be specified in the child.cluster.allowlist in order to be included
   * */
  public static final String CHILD_CLUSTER_URL_PREFIX = "child.cluster.url.";

  /**
   * Similar to {@link ConfigKeys#CHILD_CLUSTER_URL_PREFIX} but with D2 ZK url.
   */
  public static final String CHILD_CLUSTER_D2_PREFIX = "child.cluster.d2.zkHost.";

  /**
   * Config prefix for Kafka bootstrap url in all child fabrics; parent controllers need to know the
   * Kafka url in all fabrics for native replication.
   */
  public static final String CHILD_DATA_CENTER_KAFKA_URL_PREFIX = "child.data.center.kafka.url";

  /**
   * D2 Service name for the child controllers in local datacenter
   */
  public static final String CHILD_CLUSTER_D2_SERVICE_NAME = "child.cluster.d2.service.name";

  /**
   * D2 Service name for cluster discovery
   */
  public static final String CLUSTER_DISCOVERY_D2_SERVICE = "cluster.discovery.d2.service";

  /**
   * The default source fabric used for native replication
   */
  public static final String NATIVE_REPLICATION_SOURCE_FABRIC = "native.replication.source.fabric";

  /**
   * The default source fabric used for native replication for batch only stores.
   */
  public static final String NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_BATCH_ONLY_STORES =
      "native.replication.source.fabric.as.default.for.batch.only.stores";

  /**
   * The default source fabric used for native replication for hybrid stores.
   */
  public static final String NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_HYBRID_STORES =
      "native.replication.source.fabric.as.default.for.hybrid.stores";

  /**
   * The default source fabric used for native replication for incremental push stores.
   */
  public static final String NATIVE_REPLICATION_SOURCE_FABRIC_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORES =
      "native.replication.source.fabric.as.default.for.incremental.push.stores";
  /**
   * The highest priority source fabric selection config, specified in parent controller.
   */
  public static final String EMERGENCY_SOURCE_REGION = "emergency.source.region";

  // go/inclusivecode deprecated(alias="child.cluster.allowlist")
  @Deprecated
  public static final String CHILD_CLUSTER_WHITELIST = "child.cluster.whitelist";

  /**
   * Only required when controller.parent.mode=true
   * This is a comma-separated allowlist of cluster names used in the keys with the child.cluster.url prefix.
   *
   * Example, if we have the following child.cluster.url keys:
   *
   * child.cluster.url.cluster1=...
   * child.cluster.url.cluster2=...
   * child.cluster.url.cluster3=...
   *
   * And we want to use all three cluster, then we set
   *
   * child.cluster.allowlist=cluster1,cluster2,cluster3
   *
   * If we only want to use clusters 1 and 3 we can set
   *
   * child.cluster.allowlist=cluster1,cluster3
   *
   */
  public static final String CHILD_CLUSTER_ALLOWLIST = "child.cluster.allowlist";

  // go/inclusivecode deprecated(alias="native.replication.fabric.allowlist")
  @Deprecated
  public static final String NATIVE_REPLICATION_FABRIC_WHITELIST = "native.replication.fabric.whitelist";

  /**
   * Previously {@link #CHILD_CLUSTER_ALLOWLIST} is used to also represent the allowlist of source fabrics
   * for native replication; however, the final migration plan decides that a Kafka cluster in parent fabric
   * can also be the source fabric, so the below config is introduced to represent all potential source
   * fabrics for native replication.
   */
  public static final String NATIVE_REPLICATION_FABRIC_ALLOWLIST = "native.replication.fabric.allowlist";
  /**
   * A list of potential parent fabrics. Logically, there is only one parent fabric; during native replication
   * migration, there will be two Kafka clusters in parent fabric, so we need two fabric names to represent
   * the two different Kafka cluster url.
   *
   * TODO: deprecate this config after native replication migration is complete.
   */
  public static final String PARENT_KAFKA_CLUSTER_FABRIC_LIST = "parent.kafka.cluster.fabric.list";

  /**
   * Whether A/A is enabled on the controller. When it is true, all A/A required config (e.g.
   * {@link #ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST}) must be set.
   */
  public static final String ACTIVE_ACTIVE_ENABLED_ON_CONTROLLER = "active.active.enabled.on.controller";

  /**
   * A list of fabrics that are source(s) of the active active real time replication. When active-active replication
   * is enabled on the controller {@link #ACTIVE_ACTIVE_ENABLED_ON_CONTROLLER} is true, this list should contain fabrics
   * where the Venice server should consume from when it accepts the TS (TopicSwitch) message.
   * Example value of this config: "dc-0,dc-1,dc-2".
   */
  public static final String ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST = "active.active.real.time.source.fabric.list";

  /**
   * When the parent controller receives an admin write operation, it replicates that message to the admin kafka stream.
   * After replication the parent controller consumes the message from the stream and processes it there.  This is the
   * timeout for waiting until that consumption happens.
   * */
  public static final String PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS =
      "parent.controller.waiting.time.for.consumption.ms";

  /**
   * If there is a failure in consuming from the admin topic, skip the message after retrying for this many minutes
   * Default 5 days
   */
  public static final String ADMIN_CONSUMPTION_TIMEOUT_MINUTES = "admin.consumption.timeout.minute";

  /**
   * The maximum time allowed for worker threads to execute admin messages in one cycle. A cycle is the processing of
   * delegated admin messages by some number of worker thread(s) defined by {@code ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE}.
   * Each worker thread will try to empty the queue for a store before moving on to process admin messages for another
   * store. The cycle is completed either by finishing all delegated admin messages or timing out with this config.
   * TODO: Note that the timeout is for all stores in the cycle and not individual stores. Meaning that some stores may starve.
   */
  public static final String ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS = "admin.consumption.cycle.timeout.ms";

  /**
   * The maximum number of threads allowed in the pool for executing admin messages.
   */
  public static final String ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE =
      "admin.consumption.max.worker.thread.pool.size";

  /**
   * This factor is used to estimate potential push size. VPJ reducer multiplies it
   * with total record size and compares it with store storage quota
   * TODO: it will be moved to Store metadata if we allow stores have various storage engine types.
   */
  public static final String STORAGE_ENGINE_OVERHEAD_RATIO = "storage.engine.overhead.ratio";

  // go/inclusivecode deprecated(alias="enable.offline.push.ssl.allowlist")
  @Deprecated
  public static final String ENABLE_OFFLINE_PUSH_SSL_WHITELIST = "enable.offline.push.ssl.whitelist";
  /**
   * The switcher to enable/disable the allowlist of ssl offline pushes. If we disable the allowlist here, depends on
   * the config "SSL_TO_KAFKA", all pushes will be secured by SSL or none of pushes will be secured by SSL.
   */
  public static final String ENABLE_OFFLINE_PUSH_SSL_ALLOWLIST = "enable.offline.push.ssl.allowlist";

  // go/inclusivecode deprecated(alias="enable.hybrid.push.ssl.allowlist")
  @Deprecated
  public static final String ENABLE_HYBRID_PUSH_SSL_WHITELIST = "enable.hybrid.push.ssl.whitelist";
  /**
   * The switcher to enable/disable the allowlist of ssl hybrid pushes including both batch and near-line pushes for
   * that store. If we disable the allowlist here, depends on the config "SSL_TO_KAFKA", all pushes will be secured by
   * SSL or none of pushes will be secured by SSL.
   */
  public static final String ENABLE_HYBRID_PUSH_SSL_ALLOWLIST = "enable.hybrid.push.ssl.allowlist";

  // go/inclusivecode deprecated(alias="push.ssl.allowlist")
  @Deprecated
  public static final String PUSH_SSL_WHITELIST = "push.ssl.whitelist";

  /**
   * Allowlist of stores which are allowed to push data with SSL.
   */
  public static final String PUSH_SSL_ALLOWLIST = "push.ssl.allowlist";

  /**
   * Whether to block storage requests on the non-ssl port.  Will still allow metadata requests on the non-ssl port
   * and will log storage requests on the non-ssl port even if set to false;
   */
  public static final String ENFORCE_SECURE_ROUTER = "router.enforce.ssl";

  public static final String HELIX_REBALANCE_ALG = "helix.rebalance.alg";

  /**
   * The replication factor to set for admin topics.
   */
  public static final String ADMIN_TOPIC_REPLICATION_FACTOR = "admin.topic.replication.factor";

  public static final String SERVER_DISK_FULL_THRESHOLD = "disk.full.threshold";

  /**
   * If a request is slower than this, it will be reported as tardy in the router metrics
   */
  public static final String ROUTER_SINGLEGET_TARDY_LATENCY_MS = "router.singleget.tardy.latency.ms";
  public static final String ROUTER_MULTIGET_TARDY_LATENCY_MS = "router.multiget.tardy.latency.ms";
  public static final String ROUTER_COMPUTE_TARDY_LATENCY_MS = "router.compute.tardy.latency.ms";

  public static final String ROUTER_ENABLE_READ_THROTTLING = "router.enable.read.throttling";

  /**
   * This config is for {@literal LeakedCompletableFutureCleanupService}.
   * Polling interval.
   */
  public static final String ROUTER_LEAKED_FUTURE_CLEANUP_POLL_INTERVAL_MS =
      "router.leaked.future.cleanup.poll.interval.ms";
  /**
   * This config is for {@literal LeakedCompletableFutureCleanupService}.
   * If the CompletableFuture stays in current service beyonds the configured threshold,
   * {@literal LeakedCompletableFutureCleanupService} will complete it exceptionally.
   */
  public static final String ROUTER_LEAKED_FUTURE_CLEANUP_THRESHOLD_MS = "router.leaked.future.cleanup.threshold.ms";

  /**
   * The name of the cluster that the internal store for storing push job details records belongs to.
   */
  public static final String PUSH_JOB_STATUS_STORE_CLUSTER_NAME = "controller.push.job.status.store.cluster.name";

  /**
   * The job tracker identifier as part of a map reduce job id.
   */
  public static final String PUSH_JOB_MAP_REDUCE_JT_ID = "push.job.map.reduce.jt.id";

  /**
   * The job identifier as part of a map reduce job id.
   */
  public static final String PUSH_JOB_MAP_REDUCE_JOB_ID = "push.job.map.reduce.job.id";

  /**
   * Flag to indicate whether to perform add version and start of ingestion via the admin protocol.
   */
  public static final String CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL = "controller.add.version.via.admin.protocol";

  public static final String CONTROLLER_EARLY_DELETE_BACKUP_ENABLED = "controller.early.delete.backup.enabled";

  /**
   * Flag to indicate which push monitor controller will pick up for an upcoming push
   */
  public static final String PUSH_MONITOR_TYPE = "push.monitor.type";

  /**
   * Flag to enable the participant message store setup and write operations to the store.
   */
  public static final String PARTICIPANT_MESSAGE_STORE_ENABLED = "participant.message.store.enabled";

  /**
   * The name of the cluster that should host the special stores used to serve system schemas.
   */
  public static final String CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME = "controller.system.schema.cluster.name";

  /**
   * The name of the cluster that should host the special stores used to serve system schemas.
   * This config is same as {@link #CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME}, since this config will be used
   * in various Venice Components, so we remove the `controller` prefix to avoid confusion.
   * TODO: deprecate {@link #CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME}
   */
  public static final String SYSTEM_SCHEMA_CLUSTER_NAME = "system.schema.cluster.name";

  /**
   * Flag to enable the controller to send kill push job helix messages to the storage node upon consuming kill push job
   * admin messages.
   */
  public static final String ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED = "admin.helix.messaging.channel.enabled";

  /**
   * Minimum delay between each cycle where the storage node polls the participant message store to see if any of its
   * ongoing push job has been killed.
   */
  public static final String PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS = "participant.message.consumption.delay.ms";

  public static final String ROUTER_STATEFUL_HEALTHCHECK_ENABLED = "router.stateful.healthcheck.enabled";

  /**
  * Maximum number of pending router request per storage node after which router concludes that host to be unhealthy
  * and stops sending further request to it..
  */
  public static final String ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE =
      "router.unhealthy.pending.connection.threshold.per.host";

  /**
   * This is the threshold for pending request queue depth per storage node after which router resumes sending requests once a storage node
   * which was previously marked unhealthy due to high ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE
   */
  public static final String ROUTER_PENDING_CONNECTION_RESUME_THRESHOLD_PER_ROUTE =
      "router.pending.connection.resume.threshold.per.host";

  /**
   * Enables HttpAsyncClient allocation per storage node.
   */
  public static final String ROUTER_PER_NODE_CLIENT_ENABLED = "router.per.node.client.enabled";

  public static final String ROUTER_PER_NODE_CLIENT_THREAD_COUNT = "router.per.node.client.thread.count";

  /**
   * Whether to support http/2 inbound request.
   * When this feature is enabled, the Router will support both http/1.1 and http/2.
   */
  public static final String ROUTER_HTTP2_INBOUND_ENABLED = "router.http2.inbound.enabled";

  /**
   * Indicates the maximum number of concurrent streams that the sender will allow.  This limit is
   * directional: it applies to the number of streams that the sender permits the receiver to create.
   */
  public static final String ROUTER_HTTP2_MAX_CONCURRENT_STREAMS = "router.http2.max.concurrent.streams";

  /**
   * Indicates the size of the largest frame payload that the sender is willing to receive, in octets.
   */
  public static final String ROUTER_HTTP2_MAX_FRAME_SIZE = "router.http2.max.frame.size";

  /**
   * Indicates the sender's initial window size (in octets) for stream-level flow control.
   */
  public static final String ROUTER_HTTP2_INITIAL_WINDOW_SIZE = "router.http2.initial.window.size";

  /**
   * Allows the sender to inform the remote endpoint of the maximum size of the header compression
   * table used to decode header blocks, in octets.
   */
  public static final String ROUTER_HTTP2_HEADER_TABLE_SIZE = "router.http2.header.table.size";

  /**
   * This advisory setting informs a peer of the maximum size of header list that the sender is
   * prepared to accept, in octets.  The value is based on the uncompressed size of header fields,
   * including the length of the name and value in octets plus an overhead of 32 octets for each
   * header field.
   */
  public static final String ROUTER_HTTP2_MAX_HEADER_LIST_SIZE = "router.http2.max.header.list.size";

  /**
   * In Leader/Follower state transition model, in order to avoid split brain problem (multiple leaders) as much as possible,
   * the newly promoted leader should keep checking whether there is any new messages from the old leader in the version
   * topic, and wait for some time (5 minutes by default) after the last message consumed before switching to leader role
   * and potential starts producing to the version topic. Basically, the wait time could help us avoid the scenario that
   * more than one replica is producing to the version topic.
   */
  public static final String SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS =
      "server.promotion.to.leader.replica.delay.seconds";

  /**
   * The system store, such as replica status related requires fast leadership fail over to avoid the stable info in
   * system store, which could affect the request routing in the read path.
   * Since we do have a way to correct the unordered data if it really happens, such as produce a full snapshot
   * periodically, but the freshness is very important.
   */
  public static final String SERVER_SYSTEM_STORE_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS =
      "server.system.store.promotion.to.leader.replica.delay.seconds";

  /**
   * Whether to support http/2 inbound request.
   * When this feature is enabled, the server will support both http/1.1 and http/2.
   */
  public static final String SERVER_HTTP2_INBOUND_ENABLED = "server.http2.inbound.enabled";

  /**
   * Indicates the maximum number of concurrent streams that the sender will allow.  This limit is
   * directional: it applies to the number of streams that the sender permits the receiver to create.
   */
  public static final String SERVER_HTTP2_MAX_CONCURRENT_STREAMS = "server.http2.max.concurrent.streams";

  /**
   * Indicates the size of the largest frame payload that the sender is willing to receive, in octets.
   */
  public static final String SERVER_HTTP2_MAX_FRAME_SIZE = "server.http2.max.frame.size";

  /**
   * Indicates the sender's initial window size (in octets) for stream-level flow control.
   */
  public static final String SERVER_HTTP2_INITIAL_WINDOW_SIZE = "server.http2.initial.window.size";

  /**
   * Allows the sender to inform the remote endpoint of the maximum size of the header compression
   * table used to decode header blocks, in octets.
   */
  public static final String SERVER_HTTP2_HEADER_TABLE_SIZE = "server.http2.header.table.size";

  /**
   * This advisory setting informs a peer of the maximum size of header list that the sender is
   * prepared to accept, in octets.  The value is based on the uncompressed size of header fields,
   * including the length of the name and value in octets plus an overhead of 32 octets for each
   * header field.
   */
  public static final String SERVER_HTTP2_MAX_HEADER_LIST_SIZE = "server.http2.max.header.list.size";

  /**
   * This config defines whether SSL is enabled in controller.
   */
  public static final String CONTROLLER_SSL_ENABLED = "controller.ssl.enabled";

  /**
   * Flag to indicate if the controller cluster leader will be amongst one of the local Helix as a library controllers
   * or a Helix as a service controller running remotely.
   */
  public static final String CONTROLLER_CLUSTER_LEADER_HAAS = "controller.cluster.leader.haas.enabled";

  /**
   * The super cluster name for HAAS. This config is required if HAAS is enabled for the creation of helix clusters.
   */
  public static final String CONTROLLER_HAAS_SUPER_CLUSTER_NAME = "controller.haas.super.cluster.name";

  /**
   * Whether to enable batch push (including GF job) from Admin in Child Controller.
   * In theory, we should disable batch push in Child Controller no matter what, but the fact is that today there are
   * many tests, which are doing batch pushes to an individual cluster setup (only Child Controller), so disabling batch push from Admin
   * in Child Controller will require a lot of refactoring.
   * So the current strategy is to enable it by default, but disable it in EI and PROD.
   */
  public static final String CONTROLLER_ENABLE_BATCH_PUSH_FROM_ADMIN_IN_CHILD =
      "controller.enable.batch.push.from.admin.in.child";

  /**
   * A config that turns the key/value profiling stats on and off. This config can be placed in both Router and SNs and it
   * is off by default. When switching it on, We will emit a fine grained histogram that reflects the distribution of
   * key and value size. Since this will be run in the critical read path and it will emit additional ~20 stats, please
   * be cautious when turning it on.
   */
  public static final String KEY_VALUE_PROFILING_ENABLED = "key.value.profiling.enabled";

  /*
   * Flag to indicate if venice clusters' leader will be amongst one of the local Helix as a library controllers
   * or a Helix as a service controller running remotely.
   */
  public static final String VENICE_STORAGE_CLUSTER_LEADER_HAAS = "venice.cluster.leader.haas.enabled";

  /**
   * A config specifies which partitioning scheme should be used by VenicePushJob.
   */
  public static final String PARTITIONER_CLASS = "partitioner.class";
  /**
   * A configs of over-partitioning factor
   * number of Kafka partitions in each partition
   */
  public static final String AMPLIFICATION_FACTOR = "amplification.factor";

  /**
   * A unique id that can represent this instance
   */
  public static final String INSTANCE_ID = "instance.id";

  /**
   * Maximum time allowed for router to download dictionary from Storage nodes.
   */
  public static final String ROUTER_DICTIONARY_RETRIEVAL_TIME_MS = "router.dictionary.retrieval.time.ms";

  /**
   * Number of threads that the Router will use to wait for dictionary to download from storage nodes and process it.
   */
  public static final String ROUTER_DICTIONARY_PROCESSING_THREADS = "router.dictionary.processing.threads";

  /**
   * The class name to use for the {@link com.linkedin.venice.kafka.admin.KafkaAdminWrapper}.
   */
  public static final String KAFKA_ADMIN_CLASS = "kafka.admin.class";

  /**
   * Fully-qualified class name to use for Kafka write-only admin operations.
   */
  public static final String KAFKA_WRITE_ONLY_ADMIN_CLASS = "kafka.write.only.admin.class";

  /**
   * Fully-qualified class name to use for Kafka read-only admin operations.
   */
  public static final String KAFKA_READ_ONLY_ADMIN_CLASS = "kafka.read.only.admin.class";

  /**
   * A config that determines whether to use Helix customized view for hybrid store quota
   */
  public static final String HELIX_HYBRID_STORE_QUOTA_ENABLED = "helix.hybrid.store.quota.enabled";

  /**
   * A time after which a bad SSD will trigger server shutdown.
   */
  public static final String SERVER_SHUTDOWN_DISK_UNHEALTHY_TIME_MS = "server.shutdown.ssd.unhealthy.time.ms";

  /**
   * Turns on early router throttling before allocating most of the router resources.
   */
  public static final String ROUTER_EARLY_THROTTLE_ENABLED = "router.early.throttle.enabled";

  /**
   *  Disable router heart-beat job which marks hosts as unhealthy.
   */
  public static final String ROUTER_HEART_BEAT_ENABLED = "router.heart.beat.enabled";

  /**
   * HttpClient5 pool size.
   */
  public static final String ROUTER_HTTP_CLIENT5_POOL_SIZE = "router.http.client5.pool.size";

  /**
   * Total IO thread count for HttpClient5 pool.
   */
  public static final String ROUTER_HTTP_CLIENT5_TOTAL_IO_THREAD_COUNT = "router.http.client5.total.io.thread.count";

  /**
   * Whether to skip the cipher check when using Httpclient5.
   */
  public static final String ROUTER_HTTP_CLIENT5_SKIP_CIPHER_CHECK_ENABLED =
      "router.http.client5.skip.cipher.check.enabled";

  /**
   * Number of IO threads used for AHAC client.
   */
  public static final String ROUTER_HTTPASYNCCLIENT_CLIENT_POOL_THREAD_COUNT =
      "router.httpasyncclient.client.pool.io.thread.count";

  /** Maximum number of times controller will automatically reset an error partition for the current/serving version
   * to mitigate impact of transient or long running issues during re-balance or restart.
   */
  public static final String ERROR_PARTITION_AUTO_RESET_LIMIT = "error.partition.auto.reset.limit";

  /**
   * The delay between each cycle where we iterate over all applicable resources and their partition to reset error
   * partitions and collect data on the effectiveness of previous resets.
   */
  public static final String ERROR_PARTITION_PROCESSING_CYCLE_DELAY = "error.partition.processing.cycle.delay";

  /**
   * Delay between each cycle where the checker will iterate over existing topics that are yet to be truncated and poll
   * their job status until they reach terminal state to ensure version topics in parent fabric are truncated in a
   * timely manner.
   */
  public static final String TERMINAL_STATE_TOPIC_CHECK_DELAY_MS = "controller.terminal.state.topic.check.delay.ms";

  /**
   * A config for Da-Vinci clients to use system store based repositories or Zk based repositories.
   */
  public static final String CLIENT_USE_SYSTEM_STORE_REPOSITORY = "client.use.system.store.repository";

  /**
   * The refresh interval for system store repositories that rely on periodic polling.
   */
  public static final String CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS =
      "client.system.store.repository.refresh.interval.seconds";

  /**
   * Test only config used to disable parent topic truncation upon job completion. This is needed because kafka cluster
   * in test environment is shared between parent and child controllers. Truncating topic upon completion will confuse
   * child controllers in certain scenarios.
   */
  public static final String CONTROLLER_DISABLE_PARENT_TOPIC_TRUNCATION_UPON_COMPLETION =
      "controller.disable.parent.topic.truncation.upon.completion";

  /**
   * ZooKeeper address of d2 client.
   */
  public static final String D2_ZK_HOSTS_ADDRESS = "r2d2Client.zkHosts";

  /**
   * Config to control if push status store is enabled and should be initialized
   */
  public static final String PUSH_STATUS_STORE_ENABLED = "push.status.store.enabled";

  public static final String CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED =
      "controller.zk.shared.davinci.push.status.system.schema.store.auto.creation.enabled";

  /**
   * Interval for Da Vinci clients to send heartbeats.
   */
  public static final String PUSH_STATUS_STORE_HEARTBEAT_INTERVAL_IN_SECONDS =
      "push.status.store.heartbeat.interval.seconds";

  /**
   * The expiration timeout. If an instance not sending heartbeats for over the expiration
   * time, it will be considered as stale.
   */
  public static final String PUSH_STATUS_STORE_HEARTBEAT_EXPIRATION_TIME_IN_SECONDS =
      "push.status.store.heartbeat.expiration.seconds";

  /**
   * Derived schemaId for push status store write compute.
   */
  public static final String PUSH_STATUS_STORE_DERIVED_SCHEMA_ID = "push.status.store.derived.schema.id";

  /**
   * Whether to throttle SSL connections between router and client.
   */
  public static final String ROUTER_THROTTLE_CLIENT_SSL_HANDSHAKES = "router.throttle.client.ssl.handshakes";

  /**
   * The number of threads that will be used to perform SSL handshakes between clients and a router.
   */
  public static final String ROUTER_CLIENT_SSL_HANDSHAKE_THREADS = "router.client.ssl.handshake.threads";

  /**
   * The maximum number of concurrent SSL handshakes between clients and a router.
   */
  public static final String ROUTER_MAX_CONCURRENT_SSL_HANDSHAKES = "router.max.concurrent.ssl.handshakes";

  /**
   * The number of attempts of SSL handshakes between clients and a router.
   */
  public static final String ROUTER_CLIENT_SSL_HANDSHAKE_ATTEMPTS = "router.client.ssl.handshake.attempts";

  /**
   * The delay between attempts of SSL handshakes between clients and a router.
   */
  public static final String ROUTER_CLIENT_SSL_HANDSHAKE_BACKOFF_MS = "router.client.ssl.handshake.backoff.ms";

  /**
   * Lease timeout for leaving quota disabled for a router. If quota was disabled through an API, it will be reset after
   * lease expiry.
   */
  public static final String ROUTER_READ_QUOTA_THROTTLING_LEASE_TIMEOUT_MS =
      "router.read.quota.throttling.lease.timeout.ms";

  /**
   * The delay in ms between each synchronization attempt between the Venice store acls and its corresponding system
   * store acls if any synchronization is needed.
   */
  public static final String CONTROLLER_SYSTEM_STORE_ACL_SYNCHRONIZATION_DELAY_MS =
      "controller.system.store.acl.synchronization.delay.ms";

  /**
   * This config defines the sleep interval in leaked push status clean up service.
   */
  public static final String LEAKED_PUSH_STATUS_CLEAN_UP_SERVICE_SLEEP_INTERVAL_MS =
      "leaked.push.status.clean.up.service.interval.ms";

  /**
   * This config controls whether to use da-vinci based implementation of the system store repository when
   * CLIENT_USE_SYSTEM_STORE_REPOSITORY is set to true. By default the thin-client based implementation will be used.
   */
  public static final String CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY =
      "client.use.da.vinci.based.system.store.repository";

  /**
   *
   */
  public static final String CONTROLLER_DISABLE_PARENT_REQUEST_TOPIC_FOR_STREAM_PUSHES =
      "controller.disable.parent.request.topic.for.stream.pushes";

  public static final String CONTROLLER_DEFAULT_READ_QUOTA_PER_ROUTER = "controller.default.read.quota.per.router";

  /**
   * This config will specify the region name of a controller; the region name can be customized by Venice internal.
   */
  public static final String LOCAL_REGION_NAME = "local.region.name";

  /**
   * This config controls whether to make an empty push to materialize meta system store for newly created user stores
   * if possible.
   */
  public static final String CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE =
      "controller.auto.materialize.meta.system.store";

  /**
   *
   */
  public static final String CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE =
      "controller.auto.materialize.davinci.push.status.system.store";

  /**
   * This will indicate which ReplicationMetadataSchemaGenerator version to use to generate replication metadata schema.
   * This config should be set on a per-cluster level, meaning that each cluster can have its own RMD version ID.
   */
  public static final String REPLICATION_METADATA_VERSION_ID = "replication.metadata.version.id";

  /*
   * This config will specify the offset lag threshold to be used for offset lag comparison in making partition online faster.
   */
  public static final String OFFSET_LAG_DELTA_RELAX_FACTOR_FOR_FAST_ONLINE_TRANSITION_IN_RESTART =
      "offset.lag.delta.relax.factor.for.fast.online.transition.in.restart";

  /**
   * Enable offset collection for kafka topic partition from kafka consumer metrics.
   */
  public static final String SERVER_KAFKA_CONSUMER_OFFSET_COLLECTION_ENABLED =
      "server.kafka.consumer.offset.collection.enabled";

  /**
   * This indicates if server will perform the schema presence check or not.
   * By default it is set to true.
   */
  public static final String SERVER_SCHEMA_PRESENCE_CHECK_ENABLED = "server.schema.presence.check.enabled";

  /**
   * Prefix of configs to configure Jetty server in Controller.
   */
  public static final String CONTROLLER_JETTY_CONFIG_OVERRIDE_PREFIX = "controller.jetty.";

  /**
   * The number of records
   */
  public static final String ROUTER_META_STORE_SHADOW_READ_ENABLED = "router.meta.store.shadow.read.enabled";

  /**
   * Defines the key names in venice.server.kafkaClustersMap
   */
  public static final String KAFKA_CLUSTER_MAP_KEY_NAME = "name";
  public static final String KAFKA_CLUSTER_MAP_KEY_URL = "url";
  public static final String KAFKA_CLUSTER_MAP_KEY_OTHER_URLS = "otherUrls";
  public static final String KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL = "securityProtocol";

  public static final String SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING =
      "server.enable.live.config.based.kafka.throttling";

  /**
   * Enable the concurrent execution of the controllers' init routines, which are executed when a controller
   * becomes a cluster leader.
   */
  public static final String CONCURRENT_INIT_ROUTINES_ENABLED = "concurrent.init.routines.enabled";

  /**
   * A config to control graceful shutdown.
   * True: servers will flush all remain data in producers buffers and drainer queues, and persist all data including offset
   *       metadata and producer states into disk
   * False: servers will not flush any data during shutdown. After restart, servers will resume ingestion from the last checkpoint.
   */
  public static final String SERVER_INGESTION_CHECKPOINT_DURING_GRACEFUL_SHUTDOWN_ENABLED =
      "server.ingestion.checkpoint.during.graceful.shutdown.enabled";

  /**
   * A config to control which status store to use for fetching incremental push job status from the controller. This config
   * should be removed once the migration of push status to push status system store is complete.
   * True: use push system status store
   * False: use zookeeper store
   */
  public static final String USE_PUSH_STATUS_STORE_FOR_INCREMENTAL_PUSH =
      "controller.server.incremental.push.use.push.status.store";

  /**
   * A config to control whether VeniceServer will optimize the database for the backup version to
   * free up memory resources occupied.
   * TODO: explore to apply this feature to DVC as well.
   * This feature should be very useful for RocksDB plaintable to unload the mmapped memory and it will be useful for
   * RocksDB block-based format as well to evict the unused index/filters from the shared block cache.
   */
  public static final String SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_ENABLED =
      "server.optimize.database.for.backup.version.enabled";

  /**
   * A config to control the no read threshold when the database optimization should kick in.
   */
  public static final String SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_NO_READ_THRESHOLD_SECONDS =
      "server.optimize.database.for.backup.version.no.read.threshold.seconds";

  /**
   * Schedule interval for database optimization service.
   */
  public static final String SERVER_OPTIMIZE_DATABASE_SERVICE_SCHEDULE_INTERNAL_SECONDS =
      "server.optimize.database.service.schedule.internal.seconds";

  /**
   * A config that determines whether to unregister per store metrics when a store is deleted. Default is false.
   */
  public static final String UNREGISTER_METRIC_FOR_DELETED_STORE_ENABLED =
      "unregister.metric.for.deleted.store.enabled";

  /**
   * Config to enable single leader replica disabling.
   */
  public static final String FORCE_LEADER_ERROR_REPLICA_FAIL_OVER_ENABLED =
      "controller.force.leader.error.replica.fail.over.enabled";
  /**
   * A config to specify the class to use to parse identities at runtime
   */
  public static final String IDENTITY_PARSER_CLASS = "identity.parser.class";

  /**
   * Specifies a list of partitioners venice supported.
   * It contains a string of concatenated partitioner class names separated by comma.
   */
  public static final String VENICE_PARTITIONERS = "venice.partitioners";

  /**
   * Config to check whether the protocol versions used at runtime are valid in Venice backend; if not, fail fast.
   * Used by Samza jobs and Da Vinci clients. Default value should be true.
   * Turn off the config in where access to routers is not feasible.
   */
  public static final String VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION = "validate.venice.internal.schema.version";

  /**
   * Config to control the maximum number of fields per method in a fast-avro generated deserializer. Can be useful if
   * the JIT limit of 8 KB of bytecode is reached. An alternative is to use the -XX:-DontCompileHugeMethods JVM flag
   * but that can have other side effects, so it may not be preferred.
   */
  public static final String FAST_AVRO_FIELD_LIMIT_PER_METHOD = "fast.avro.field.limit.per.method";
}
