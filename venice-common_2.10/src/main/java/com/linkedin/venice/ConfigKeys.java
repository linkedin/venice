package com.linkedin.venice;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ConfigKeys {
  private ConfigKeys(){}

  // cluster specific properties
  public static final String CLUSTER_NAME = "cluster.name";
  public static final String PARTITION_NODE_ASSIGNMENT_SCHEME = "partition.node.assignment.scheme";
  public static final String ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT = "enable.kafka.consumers.offset.management";
  public static final String OFFSET_MANAGER_TYPE = "offset.manager.type";
  public static final String OFFSET_DATA_BASE_PATH = "offsets.data.base.path";
  public static final String OFFSET_MANAGER_FLUSH_INTERVAL_MS = "offset.manager.flush.interval.ms";
  public static final String OFFSET_MANAGER_LOG_FILE_MAX_BYTES = "offset.manager.log.file.max.bytes";
  public static final String ZOOKEEPER_ADDRESS = "zookeeper.address";
  public static final Set<String> clusterSpecificProperties = new HashSet<String>(Arrays
      .asList(CLUSTER_NAME, PARTITION_NODE_ASSIGNMENT_SCHEME,
          ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT, OFFSET_MANAGER_TYPE, OFFSET_DATA_BASE_PATH,
          OFFSET_MANAGER_FLUSH_INTERVAL_MS, ZOOKEEPER_ADDRESS));

  public static final String ADMIN_PORT="admin.port";

  public static final String STATUS_MESSAGE_RETRY_COUNT = "status.message.retry.count";
  public static final String STATUS_MESSAGE_RETRY_DURATION_MS = "status.message.retry.duration.ms";

  // store specific properties
  public static final String PERSISTENCE_TYPE = "persistence.type";
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_BROKER_PORT = "kafka.broker.port";
  public static final String KAFKA_CONSUMER_FETCH_BUFFER_SIZE = "kafka.consumer.fetch.buffer.size";
  public static final String KAFKA_CONSUMER_SOCKET_TIMEOUT_MS = "kafka.consumer.socket.timeout.ms";
  public static final String KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES =
      "kafka.consumer.num.metadata.refresh.retries";
  public static final String KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS = "kafka.consumer.metadata.refresh.backoff.ms";

  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  public static final String SSL_KAFKA_BOOTSTRAP_SERVERS = "ssl.kafka.bootstrap.servers";
  public static final String KAFKA_FETCH_QUOTA_BYTES_PER_SECOND = "kafka.fetch.quota.bytes.per.second";
  /**
   * How many records that one server could consume from Kafka at most in one second.
   * If the consume rate reached this quota, the consumption thread will be blocked until there is the available quota.
   */
  public static final String KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND = "kafka.fetch.quota.records.per.second";
  /**
   * The time window used by the consumption throttler. Throttler will sum the requests during the time window and
   * compare with the quota accumulated in the time window to see whether the usage exceeds quota or not.
   */
  public static final String KAFKA_FETCH_QUOTA_TIME_WINDOW_MS = "kafka.fetch.quota.time.window.ms";
  // Kafka security protocol
  public static final String KAFKA_SECURITY_PROTOCOL = "security.protocol";
  // ssl config
  public static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
  public static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
  public static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";
  public static final String SSL_KEY_PASSWORD = "ssl.key.password";
  public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
  public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  public static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
  public static final String SSL_KEYMANAGER_ALGORITHM = "ssl.keymanager.algorithm";
  public static final String SSL_TRUSTMANAGER_ALGORITHM = "ssl.trustmanager.algorithm";
  public static final String SSL_SECURE_RANDOM_IMPLEMENTATION = "ssl.secure.random.implementation";

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
  public static final String KAFKA_LOG_COMPACTION_FOR_INCREMENTAL_PUSH_STORES = "kafka.log.compaction.for.incremental.push.stores";

  /**
   * The min.isr property to be set at topic creation time. Will not modify already-existing topics.
   *
   * If unset, will use the Kafka cluster's default.
   */
  public static final String KAFKA_MIN_ISR = "kafka.min.isr";

  /**
   * The replication factor to set for real-time bufffer topics and store-version topics, at topic creation time.
   */
  public static final String KAFKA_REPLICATION_FACTOR = "kafka.replication.factor";

  /**
   * Fallback to remain compatible with the old config spelling.
   *
   * Ignored if {@value KAFKA_REPLICATION_FACTOR} is present.
   */
  @Deprecated
  public static final String KAFKA_REPLICATION_FACTOR_LEGACY_SPELLING = "kafka.replica.factor";
  public static final String KAFKA_ZK_ADDRESS = "kafka.zk.address";
  public static final String DEFAULT_READ_STRATEGY = "default.read.strategy";
  public static final String DEFAULT_OFFLINE_PUSH_STRATEGY = "default.offline.push.strategy";
  public static final String DEFAULT_ROUTING_STRATEGY = "default.routing.strategy";
  public static final String DEFAULT_REPLICA_FACTOR = "default.replica.factor";
  public static final String DEFAULT_NUMBER_OF_PARTITION = "default.partition.count";
  public static final String DEFAULT_MAX_NUMBER_OF_PARTITIONS = "default.partition.max.count";
  public static final String DEFAULT_PARTITION_SIZE = "default.partition.size";
  public static final String OFFLINE_JOB_START_TIMEOUT_MS = "offline.job.start.timeout.ms";
  public static final String DELAY_TO_REBALANCE_MS = "delay.to.rebalance.ms";
  public static final String MIN_ACTIVE_REPLICA = "min.active.replica";
  public static final String DEFAULT_STORAGE_QUOTA = "default.storage.quota";
  public static final String DEFAULT_READ_QUOTA = "default.read.quota";
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
  public static final String TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS = "topic.cleanup.sleep.interval.between.topic.list.fetch.ms";
  public static final String TOPIC_CLEANUP_DELAY_FACTOR = "topic.cleanup.delay.factor";

  public static final String ENABLE_TOPIC_REPLICATOR = "controller.enable.topic.replicator";
  public static final String ENABLE_TOPIC_REPLICATOR_SSL = "controller.enable.topic.replicator.ssl";
  // Server specific configs
  public static final String LISTENER_PORT = "listener.port";
  public static final String DATA_BASE_PATH = "data.base.path";
  public static final String AUTOCREATE_DATA_PATH = "autocreate.data.path";
  public static final String ENABLE_SERVER_WHITE_LIST = "enable.server.whitelist";
  public static final String MAX_ONLINE_OFFLINE_STATE_TRANSITION_THREAD_NUMBER = "max.state.transition.thread.number";
  public static final String MAX_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER = "max.leader.follower.state.transition.thread.number";
  public static final String STORE_WRITER_NUMBER = "store.writer.number";
  public static final String STORE_WRITER_BUFFER_MEMORY_CAPACITY = "store.writer.buffer.memory.capacity";
  public static final String STORE_WRITER_BUFFER_NOTIFY_DELTA = "store.writer.buffer.notify.delta";
  public static final String OFFSET_DATABASE_CACHE_SIZE = "offset.database.cache.size";
  public static final String SERVER_REST_SERVICE_STORAGE_THREAD_NUM = "server.rest.service.storage.thread.num";
  public static final String SERVER_NETTY_IDLE_TIME_SECONDS = "server.netty.idle.time.seconds";
  public static final String SERVER_MAX_REQUEST_SIZE = "server.max.request.size";
  public static final String SERVER_SOURCE_TOPIC_OFFSET_CHECK_INTERVAL_MS = "server.source.topic.offset.check.interval.ms";
  public static final String SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS = "server.netty.graceful.shutdown.period.seconds";
  public static final String SERVER_NETTY_WORKER_THREADS = "server.netty.worker.threads";
  public static final String SERVER_FAIR_STORAGE_EXECUTION_QUEUE = "server.fair.storage.execution.queue";
  public static final String SSL_TO_KAFKA = "ssl.to.kakfa";
  public static final String SERVER_COMPUTE_THREAD_NUM = "server.compute.thread.num";

  /**
   * Database sync per bytes for transactional mode.
   * This parameter will impact the sync frequency of database after batch push.
   * For BDB-JE transactional mode, it won't matter since BDB-JE will persist every update in the database right away;
   * For RocksDB transactional mode, it will impact the flush frequency of memtable to SST file, and normally we would
   * like to have this config to be comparable to the memtable size;
   *
   * Negative value will disable this threshold.
   */
  public static final String SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE = "server.database.sync.bytes.interval.for.transactional.mode";
  /**
   * Database sync per bytes for deferred-write mode.
   * This parameter will impact the sync frequency of database during batch push.
   * For BDB-JE deferred-write mode, it will impact the sync frequency, but BDB-JE will do auto-flush if the memory is full;
   * For RocksDB deferred-write mode, it will decide the file size of each SST file since every sync invocation will
   * generate a new SST file;
   *
   * Negative value will disable this threshold.
   */
  public static final String SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE = "server.database.sync.bytes.interval.for.deferred.write.mode";

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
  public static final String SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS = "server.partition.graceful.drop.time.in.seconds";

  /**
   * When a BDB partition is dropped, the disk space is not released immediately; a checkpoint is needed to release the disk space;
   * so a cleaner thread is spawned for the entire storage service; the cleaner thread will wake up every few hours and check
   * whether it needs to do a checkpoint; if so, clean up each store sequentially.
   */
  public static final String SERVER_LEAKED_RESOURCE_CLEAN_UP_INTERVAL_IN_MINUTES = "server.leaked.resource.clean.up.interval.in.minutes";

  /**
   * For batch-only store, enabling read-only could improve the performance greatly.
   */
  public static final String SERVER_DB_READ_ONLY_FOR_BATCH_ONLY_STORE_ENABLED = "server.db.read.only.for.batch.only.store.enabled";

  /**
   * Set to true to enable enforcement of quota by the storage node
   */
  public static final String SERVER_QUOTA_ENFORCEMENT_ENABLED = "server.quota.enforcement.enabled";

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
  public static final String SERVER_DISK_HEALTH_CHECK_INTERVAL_IN_SECONDS = "server.disk.health.check.interval.in.seconds";

  /**
   * This config is used to enable/disable the disk health check service.
   */
  public static final String SERVER_DISK_HEALTH_CHECK_SERVICE_ENABLED = "server.disk.health.check.service.enabled";

  // Router specific configs
  // TODO the config names are same as the names in application.src, some of them should be changed to keep consistent
  // TODO with controller and server.
  public static final String LISTENER_SSL_PORT = "listener.ssl.port";
  public static final String CLIENT_TIMEOUT = "client.timeout";
  public static final String HEARTBEAT_TIMEOUT =  "heartbeat.timeout";
  public static final String MAX_READ_CAPCITY = "max.read.capacity";
  public static final String SSL_TO_STORAGE_NODES = "sslToStorageNodes";
  /**
   * After this amount of time, DDS Router will retry once for the slow storage node request.
   *
   * Practically, we need to manually select the threshold (e.g. P95) for retrying based on latency metrics.
   */
  public static final String ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS = "router.long.tail.retry.for.single.get.threshold.ms";

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
  public static final String ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS = "router.long.tail.retry.for.batch.get.threshold.ms";

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
  public static final String ROUTER_SMART_LONG_TAIL_RETRY_ABORT_THRESHOLD_MS = "router.smart.long.tail.retry.abort.threshold.ms";

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
   * The max connection number per route (to one storage node);
   */
  public static final String ROUTER_MAX_OUTGOING_CONNECTION_PER_ROUTE = "router.max.outgoing.connection.per.route";
  /**
   * The max connection number in one Router to storage nodes;
   */
  public static final String ROUTER_MAX_OUTGOING_CONNECTION = "router.max.outgoing.connection";

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
   * Whether sticky routing for single-get is enabled in Router.
   * Basically, sticky routing will ensure that the requests belonging to the same partition will always go to
   * the same storage node if rebalance/deployment doesn't happen.
   * With this way, the cache efficiency will be improved a lot in storage node since each storage node only needs
   * to serve 1/3 of key space in the most scenarios.
   */
  public static final String ROUTER_ENABLE_STICKY_ROUTING_FOR_SINGLE_GET = "router.enable.sticky.routing.for.single.get";

  /**
   * Whether sticky routing for multi-get is enabled in Router.
   */
  public static final String ROUTER_ENABLE_STICKY_ROUTING_FOR_MULTI_GET = "router.enable.sticky.routing.for.multi.get";

  /**
   * Only takes effect if ROUTER_ENABLE_STICKY_ROUTING_FOR_MULTI_GET is set to false, greedy multiget means we try to
   * serve a multiget request by clustering as many partitions to as few hosts as possible.  Set this to false to
   * randomly select a replica for each partition, then group requests to the same host together.
   */
  public static final String ROUTER_GREEDY_MULTIGET = "router.greedy.multiget";
  /**
   * The buffer we will add to the per storage node read quota. E.g 0.5 means 50% extra quota.
   */
  public static final String ROUTER_PER_STORAGE_NODE_READ_QUOTA_BUFFER = "router.per.storage.node.read.quota.buffer";

  /**
   * Whether router cache is enabled or not.
   */
  public static final String ROUTER_CACHE_ENABLED = "router.cache.enabled";

  /**
   * Router cache size, and this cache is for all the stores, which enables cache feature.
   */
  public static final String ROUTER_CACHE_SIZE_IN_BYTES = "router.cache.size.in.bytes";

  /**
   * Concurrency setup for router cache, and this is must be power of 2 when using 'OFF_HEAP_CACHE'.
   */
  public static final String ROUTER_CACHE_CONCURRENCY = "router.cache.concurrency";

  /**
   * Valid cache types: 'ON_HEAP_CACHE', 'OFF_HEAP_CACHE'.
   */
  public static final String ROUTER_CACHE_TYPE = "router.cache.type";

  /**
   * Valid cache eviction algorithms: 'LRU', 'W_TINY_LFU'.
   *
   * For 'ON_HEAP_CACHE', 'LRU' is the only available cache eviction for now.
   */
  public static final String ROUTER_CACHE_EVICTION = "router.cache.eviction";

  /**
   * Max hash table size per cache segment, and it must be power of 2, and it is only useful when using 'OFF_HEAP_CACHE'.
   */
  public static final String ROUTER_CACHE_HASH_TABLE_SIZE = "router.cache.hash.table.size";

  /**
   * The TTL for each entry in router cache (millisecond)
   * If 0, TTL is not enabled; other, cache TTL is enabled
   */
  public static final String ROUTER_CACHE_TTL_MILLIS = "router.cache.ttl.millis";

  /**
   * The request is still being throttled even it is a cache hit, but just with smaller weight.
   */
  public static final String ROUTER_CACHE_HIT_REQUEST_THROTTLE_WEIGHT = "router.cache.hit.request.throttle.weight";

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
   * Number of event loop; one thread for each event loop.
   */
  public static final String ROUTER_NETTY_CLIENT_EVENT_LOOP_THREADS = "router.netty.client.event.loop.threads";

  /**
   * Timeout for getting a channel from channel pool; if timeout, create a new channel
   */
  public static final String ROUTER_NETTY_CLIENT_CHANNEL_POOL_ACQUIRE_TIMEOUT_MS = "router.netty.client.channel.pool.acquire.timeout.ms";

  /**
   * Minimum connections for each host (a host is identified by InetSocketAddress)
   */
  public static final String ROUTER_NETTY_CLIENT_CHANNEL_POOL_MIN_CONNECTIONS = "router.netty.client.channel.pool.min.connections";

  /**
   * Maximum connections for each host/InetSocketAddress
   */
  public static final String ROUTER_NETTY_CLIENT_CHANNEL_POOL_MAX_CONNECTIONS = "router.netty.client.channel.pool.max.connections";

  /**
   * The maximum number of pending acquires for a channel in the channel pool.
   *
   * If the pending acquires exceed the threshold, netty client will fail the new requests without blocking and it won't
   * throw exception in the router thread.
   */
  public static final String ROUTER_NETTY_CLIENT_CHANNEL_POOL_MAX_PENDING_ACQUIRES = "router.netty.client.channel.pool.max.pending.acquires";

  /**
   * Interval between each channel health check
   */
  public static final String ROUTER_NETTY_CLIENT_CHANNEL_POOL_HEALTH_CHECK_INTERVAL_MS = "router.netty.client.channel.pool.health.check.interval.ms";

  /**
   * The maximum length of the aggregated content (response from SN to router) in bytes.
   *
   * If the length of the aggregated content exceeds this value, an exception will be thrown in router and the channel that
   * receives this response will be closed.
   */
  public static final String ROUTER_NETTY_CLIENT_MAX_AGGREGATED_OBJECT_LENGTH = "router.netty.client.max.aggregated.object.length";

  /**
   * Netty graceful shutdown period considering the following factors:
   * 1. D2 de-announcement could take some time;
   * 2. Client could take some  time to receive/apply the zk update event from D2 server about router shutdown;
   * 3. Router needs some time to handle already-received client requests;
   */
  public static final String ROUTER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS = "router.netty.graceful.shutdown.period.seconds";

  public static final String ROUTER_CLIENT_DECOMPRESSION_ENABLED = "router.client.decompression.enabled";

  /**
   * Venice uses a helix cluster to assign controllers to each named venice cluster.  This is the number of controllers
   * assigned to each venice cluster.  Should normally be 3; one master controller and 2 standby controllers.
   * */
  public static final String CONTROLLER_CLUSTER_REPLICA = "controller.cluster.replica";

  /** The interval, in ms, between each polling iteration of the TopicMonitor */
  public static final String TOPIC_MONITOR_POLL_INTERVAL_MS = "topic.monitor.poll.interval.ms";
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
   * A value greater than zero is recommended for Mirror Maker stability.
   *
   * N.B.: A known limitation of this preservation setting is that during store deletion, if a topic has been
   * leaked recently due to an aborted push, then there is an edge case where that topic may leak forever.
   * This leak does not happen if the latest store-versions are successful pushes, rather than failed ones.
   * Furthermore, if a store with the same name is ever re-created, then the clean up routine would resume
   * and clean up the older leaky topics successfully. This edge case is deemed a small enough concern for
   * now, though it could be addressed with a more significant redesign of the replication pipeline.
   *
   * @see
   */
  public static final String MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE = "min.number.of.unused.kafka.topics.to.preserve";

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
  public static final String PARENT_CONTROLLER_MAX_ERRORED_TOPIC_NUM_TO_KEEP = "parent.controller.max.errored.topic.num.to.keep";

  /**
   * Only required when controller.parent.mode=true
   * This prefix specifies the location of every child cluster that is being fed by this parent cluster.
   * The format for key/value would be like "key=child.cluster.url.ei-ltx1, value=url1;url2;url3"
   * the cluster name should be human readable, ex: ei-ltx1
   * the url should be of the form http://host:port
   *
   * Note that every cluster name supplied must also be specified in the child.cluster.whitelist in order to be included
   * */
  public static final String CHILD_CLUSTER_URL_PREFIX = "child.cluster.url";

  /**
   * Similar to {@link ConfigKeys#CHILD_CLUSTER_URL_PREFIX} but with D2 url.
   */
  public static final String CHILD_CLUSTER_D2_PREFIX = "child.cluster.d2.zkHost";

  public static final String CHILD_CLUSTER_D2_SERVICE_NAME = "child.cluster.d2.service.name";

  /**
   * Only required when controller.parent.mode=true
   * This is a comma-separated whitelist of cluster names used in the keys with the child.cluster.url prefix.
   *
   * Example, if we have the following child.cluster.url keys:
   *
   * child.cluster.url.cluster1=...
   * child.cluster.url.cluster2=...
   * child.cluster.url.cluster3=...
   *
   * And we want to use all three cluster, then we set
   *
   * child.cluster.whitelist=cluster1,cluster2,cluster3
   *
   * If we only want to use clusters 1 and 3 we can set
   *
   * child.cluster.whitelist=cluster1,cluster3
   *
   */
  public static final String CHILD_CLUSTER_WHITELIST = "child.cluster.whitelist";

  /**
   * When the parent controller receives an admin write operation, it replicates that message to the admin kafka stream.
   * After replication the parent controller consumes the message from the stream and processes it there.  This is the
   * timeout for waiting until that consumption happens.
   * */
  public static final String PARENT_CONTROLLER_WAITING_TIME_FOR_CONSUMPTION_MS = "parent.controller.waiting.time.for.consumption.ms";

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
  public static final String ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE = "admin.consumption.max.worker.thread.pool.size";

  /**
   * This factor is used to estimate potential push size. H2V reducer multiplies it
   * with total record size and compares it with store storage quota
   * TODO: it will be moved to Store metadata if we allow stores have various storage engine types.
   */
  public static final String STORAGE_ENGINE_OVERHEAD_RATIO = "storage.engine.overhead.ratio";

  /**
   * Env variable for setting keystore when running Venice with quickstart.
   */
  public static final String KEYSTORE_ENV = "VENICE_KEYSTORE";

  /**
   * The switcher to enable/disable the whitelist of ssl offline pushes. If we disable the whitelist here, depends on
   * the config "SSL_TO_KAFKA", all pushes will be secured by SSL or none of pushes will be secured by SSL.
   */
  public static final String ENABLE_OFFLINE_PUSH_SSL_WHITELIST = "enable.offline.push.ssl.whitelist";
  /**
   * The switcher to enable/disable the whitelist of ssl hybrid pushes including both batch and near-line pushes for
   * that store. If we disable the whitelist here, depends on the config "SSL_TO_KAFKA", all pushes will be secured by
   * SSL or none of pushes will be secured by SSL.
   */
  public static final String ENABLE_HYBRID_PUSH_SSL_WHITELIST = "enable.hybrid.push.ssl.whitelist";

  /**
   * Whitelist of stores which are allowed to push data with SSL.
   */
  public static final String PUSH_SSL_WHITELIST = "push.ssl.whitelist";

  /**
   * Whether to block storage requests on the non-ssl port.  Will still allow metadata requests on the non-ssl port
   * and will log storage requests on the non-ssl port even if set to false;
   */
  public static final String ENFORCE_SECURE_ROUTER = "router.enforce.ssl";

  public static final String HELIX_REBALANCE_ALG = "helix.rebalance.alg";

  /**
   * Whether to establish SSL connection to Brooklin.
   */
  public static final String BROOKLIN_SSL_ENABLED  = "brooklin.ssl.enabled";

  /**
   * What replication factor should the admin topics have, upon creation.
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
   * Store name for the internal store for storing push job status records.
   */
  public static final String PUSH_JOB_STATUS_STORE_NAME = "controller.push.job.status.store.name";

  /**
   * The name of the cluster that the internal store for storing push job status records belongs to.
   */
  public static final String PUSH_JOB_STATUS_STORE_CLUSTER_NAME = "controller.push.job.status.store.cluster.name";

  /**
   * Value schema id for the push job status records.
   */
  public static final String PUSH_JOB_STATUS_VALUE_SCHEMA_ID = "push.job.status.value.schema.id";

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

  /**
   * Flag to indicate whether to perform add version and start of ingestion via the topic monitor.
   */
  public static final String CONTROLLER_ADD_VERSION_VIA_TOPIC_MONITOR = "controller.add.version.via.topic.monitor";

  /**
   * Flag to skip buffer replay for hybrid store.
   * For some scenario, buffer replay might not be necessary since the store version topic has already included all the data.
   */
  public static final String CONTROLLER_SKIP_BUFFER_REPLAY_FOR_HYBRID = "controller.skip.buffer.replay.for.hybrid";

  /**
   * Flag to indicate which push monitor controller will pick up for an upcoming push
   */
  public static final String PUSH_MONITOR_TYPE = "push.monitor.type";

  /**
   * Flag to
   */
  public static final String PARTICIPANT_MESSAGE_STORE_ENABLED = "participant.message.store.enabled";
}
