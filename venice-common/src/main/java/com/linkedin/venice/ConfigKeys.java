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
  public static final String STORE_NAME = "store.name";
  public static final String PERSISTENCE_TYPE = "persistence.type";
  public static final String STORAGE_REPLICATION_FACTOR = "storage.node.replicas";
  public static final String NUMBER_OF_KAFKA_PARTITIONS = "kafka.number.partitions";
  public static final String KAFKA_ZOOKEEPER_URL = "kafka.zookeeper.url";
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_BROKER_PORT = "kafka.broker.port";
  public static final String KAFKA_CONSUMER_FETCH_BUFFER_SIZE = "kafka.consumer.fetch.buffer.size";
  public static final String KAFKA_CONSUMER_SOCKET_TIMEOUT_MS = "kafka.consumer.socket.timeout.ms";
  public static final String KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES =
      "kafka.consumer.num.metadata.refresh.retries";
  public static final String KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS = "kafka.consumer.metadata.refresh.backoff.ms";

  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  public static final String SSL_KAFKA_BOOTSTRAP_SERVERS = "ssl.kafka.bootstrap.servers";
  public static final String MAX_KAFKA_FETCH_BYTES_PER_SECOND = "max.kafka.fetch.bytes.per.second";
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
  public static final String KAFKA_REPLICA_FACTOR = "kafka.replica.factor";
  public static final String KAFKA_ZK_ADDRESS = "kafka.zk.address";
  public static final String DEFAULT_PERSISTENCE_TYPE = "default.persistence.type";
  public static final String DEFAULT_READ_STRATEGY = "default.read.strategy";
  public static final String DEFAULT_OFFLINE_PUSH_STRATEGY = "default.offline.push.strategy";
  public static final String DEFAULT_ROUTING_STRATEGY = "default.routing.strategy";
  public static final String DEFAULT_REPLICA_FACTOR = "default.replica.factor";
  public static final String DEFAULT_NUMBER_OF_PARTITION = "default.partition.count";
  public static final String DEFAULT_MAX_NUMBER_OF_PARTITIONS = "default.partition.max.count";
  public static final String DEFAULT_PARTITION_SIZE = "default.partition.size";
  public static final String OFFLINE_JOB_START_TIMEOUT_MS = "offline.job.start.timeout.ms";
  public static final String ENABLE_TOPIC_DELETION_FOR_UNCOMPLETED_JOB = "enable.topic.deletion.for.uncompleted.job";
  public static final String MIN_REQUIRED_ONLINE_REPLICA_TO_STOP_SERVER = "min.required.online.replica.to.stop.server";
  public static final String DELAY_TO_REBALANCE_MS = "delay.to.rebalance.ms";
  public static final String MIN_ACTIVE_REPLICA = "min.active.replica";
  public static final String DEFAULT_STORAGE_QUOTA = "default.storage.quota";
  public static final String DEFAULT_READ_QUOTA = "default.read.quota";
  public static final String CLUSTER_TO_D2 = "cluster.to.d2";
  public static final String HELIX_SEND_MESSAGE_TIMEOUT_MS = "helix.send.message.timeout.ms";
  public static final String ADMIN_CONSUMPTION_RETRY_DELAY_MS = "admin.consumption.retry.delay.ms";
  public static final String REFRESH_ATTEMPTS_FOR_ZK_RECONNECT = "refresh.attempts.for.zk.reconnect";
  public static final String REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS = "refresh.interval.for.zk.reconnect.ms";

  // Controller specific configs
  public static final String CONTROLLER_CLUSTER_ZK_ADDRESSS = "controller.cluster.zk.address";
  /** Cluster name for all parent controllers */
  public static final String CONTROLLER_CLUSTER = "controller.cluster.name";
  /**
   * The retention policy for the topic, whose corresponding job fails.
   * This config is used to reduce the Kafka disk footprint for unused topics.
   * Once we come up with a good way to delete unused topics without crashing Kafka MM,
   * this config and related logic could be removed.
   */
  public static final String FAILED_JOB_TOPIC_RETENTION_MS = "failed.job.topic.retention.ms";

  public static final String ENABLE_TOPIC_REPLICATOR = "controller.enable.topic.replicator";

  // Server specific configs
  public static final String LISTENER_PORT = "listener.port";
  public static final String DATA_BASE_PATH = "data.base.path";
  public static final String AUTOCREATE_DATA_PATH = "autocreate.data.path";
  public static final String ENABLE_SERVER_WHITE_LIST = "enable.server.whitelist";
  public static final String MAX_STATE_TRANSITION_THREAD_NUMBER = "max.state.transition.thread.number";
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
  public static final String SSL_TO_KAFKA = "ssl.to.kakfa";


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
   * Concurrency setup for router cache
   */
  public static final String ROUTER_CACHE_CONCURRENCY = "router.cache.concurrency";

  /**
   * The request is still being throttled even it is a cache hit, but just with smaller weight.
   */
  public static final String ROUTER_CACHE_HIT_REQUEST_THROTTLE_WEIGHT = "router.cache.hit.request.throttle.weight";

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

  /** Whether current controller is parent or not */
  public static final String CONTROLLER_PARENT_MODE = "controller.parent.mode";
  /**
   * Whether enable topic deletion after job finishes in parent controller
   */
  public static final String PARENT_CONTROLLER_ENABLE_TOPIC_DELETION = "parent.controller.enable.topic.deletion";

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
  public static final String ADMIN_CONSUMPTION_TIMEOUT_MINUTES = "admin.consumption.timeout.minutes";

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
}
