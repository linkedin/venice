package com.linkedin.venice.config;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.store.bdb.BdbServerConfig;
import com.linkedin.venice.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.queues.FairBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.ConfigKeys.*;

/**
 * class that maintains config very specific to a Venice server
 */
public class VeniceServerConfig extends VeniceClusterConfig {

  private final int listenerPort;
  private final BdbServerConfig bdbServerConfig;
  private final RocksDBServerConfig rocksDBServerConfig;
  private final boolean enableServerWhiteList;
  private final boolean autoCreateDataPath; // default true
  /**
   * Maximum number of thread that the thread pool would keep to run the Helix online offline state transition.
   * The thread pool would create a thread for a state transition until the number of thread equals to this number.
   */
  private final int maxOnlineOfflineStateTransitionThreadNumber;

  /**
   *  Maximum number of thread that the thread pool would keep to run the Helix leader follower state transition.
   */
  private final int maxLeaderFollowerStateTransitionThreadNumber;

  /**
   * Thread number of store writers, which will process all the incoming records from all the topics.
   */
  private final int storeWriterNumber;

  /**
   * Buffer capacity being used by each writer.
   * We need to be careful when tuning this param.
   * If the queue capacity is too small, the throughput will be impacted greatly,
   * and if it is too big, the memory usage used by buffered queue could be potentially high.
   * The overall memory usage is: {@link #storeWriterNumber} * {@link #storeWriterBufferMemoryCapacity}
   */
  private final long storeWriterBufferMemoryCapacity;

  /**
   * Considering the consumer thread could put various sizes of messages into the shared queue, the internal
   * {@link com.linkedin.venice.kafka.consumer.MemoryBoundBlockingQueue} won't notify the waiting thread (consumer thread)
   * right away when some message gets processed until the freed memory hit the follow config: {@link #storeWriterBufferNotifyDelta}.
   * The reason behind this design:
   * When the buffered queue is full, and the processing thread keeps processing small message, the bigger message won't
   * have chance to get queued into the buffer since the memory freed by the processed small message is not enough to
   * fit the bigger message.
   *
   * With this delta config, {@link com.linkedin.venice.kafka.consumer.MemoryBoundBlockingQueue} will guarantee some fairness
   * among various sizes of messages when buffered queue is full.
   *
   * When tuning this config, we need to consider the following tradeoffs:
   * 1. {@link #storeWriterBufferNotifyDelta} must be smaller than {@link #storeWriterBufferMemoryCapacity};
   * 2. If the delta is too big, it will waste some buffer space since it won't notify the waiting threads even there
   * are some memory available (less than the delta);
   * 3. If the delta is too small, the big message may not be able to get chance to be buffered when the queue is full;
   *
   */
  private final long storeWriterBufferNotifyDelta;

  /**
   * The number of threads being used to serve get requests.
   */
  private final int restServiceStorageThreadNum;

  /**
   * Idle timeout for Storage Node Netty connections.
   */
  private final int nettyIdleTimeInSeconds;

  /**
   * Max request size for request to Storage Node.
   */
  private final int maxRequestSize;

  /**
   * Time interval for offset check of topic in Hybrid Store lag measurement.
   */
  private final int topicOffsetCheckIntervalMs;

  /**
   * Graceful shutdown period.
   * Venice SN needs to explicitly do graceful shutdown since Netty's graceful shutdown logic
   * will close all the connections right away, which is not expected.
   */
  private final int nettyGracefulShutdownPeriodSeconds;

  /**
   * number of worker threads for the netty listener.  If not specified, netty uses twice cpu count.
   */
  private final int nettyWorkerThreadCount;

  /**
   * Whether to use a fair queue for resource isolation in the storage execution handler.  Defaults to true
   */
  private final boolean fairStorageExecutionQueue;

  private final long databaseSyncBytesIntervalForTransactionalMode;

  private final long databaseSyncBytesIntervalForDeferredWriteMode;

  private final double diskFullThreshold;

  private final int partitionGracefulDropDelaySeconds;

  private final long storageLeakedResourceCleanUpIntervalInMS;

  private final boolean readOnlyForBatchOnlyStoreEnabled;

  private final boolean quotaEnforcementEnabled;

  private final long nodeCapacityInRcu;

  private final int kafkaMaxPollRecords;

  private final int kafkaPollRetryTimes;

  private final int kafkaPollRetryBackoffMs;

  /**
   * The number of threads being used to serve compute request.
   */
  private final int serverComputeThreadNum;

  private final long diskHealthCheckIntervalInMS;

  private final boolean diskHealthCheckServiceEnabled;

  private final boolean computeFastAvroEnabled;

  private final long participantMessageConsumptionDelayMs;

  public VeniceServerConfig(VeniceProperties serverProperties) throws ConfigurationException {
    super(serverProperties);
    listenerPort = serverProperties.getInt(LISTENER_PORT);
    dataBasePath = serverProperties.getString(DATA_BASE_PATH);
    autoCreateDataPath = Boolean.valueOf(serverProperties.getString(AUTOCREATE_DATA_PATH, "true"));
    bdbServerConfig = new BdbServerConfig(serverProperties);
    rocksDBServerConfig = new RocksDBServerConfig(serverProperties);
    enableServerWhiteList = serverProperties.getBoolean(ENABLE_SERVER_WHITE_LIST, false);
    maxOnlineOfflineStateTransitionThreadNumber = serverProperties.getInt(MAX_ONLINE_OFFLINE_STATE_TRANSITION_THREAD_NUMBER, 100);
    maxLeaderFollowerStateTransitionThreadNumber = serverProperties.getInt(MAX_LEADER_FOLLOWER_STATE_TRANSITION_THREAD_NUMBER, 20);
    storeWriterNumber = serverProperties.getInt(STORE_WRITER_NUMBER, 8);
    storeWriterBufferMemoryCapacity = serverProperties.getSizeInBytes(STORE_WRITER_BUFFER_MEMORY_CAPACITY, 25 * 1024 * 1024); // 25MB
    storeWriterBufferNotifyDelta = serverProperties.getSizeInBytes(STORE_WRITER_BUFFER_NOTIFY_DELTA, 5 * 1024 * 1024); // 5MB
    restServiceStorageThreadNum = serverProperties.getInt(SERVER_REST_SERVICE_STORAGE_THREAD_NUM, 16);
    serverComputeThreadNum = serverProperties.getInt(SERVER_COMPUTE_THREAD_NUM, 16);
    nettyIdleTimeInSeconds = serverProperties.getInt(SERVER_NETTY_IDLE_TIME_SECONDS, (int) TimeUnit.HOURS.toSeconds(3)); // 3 hours
    maxRequestSize = (int)serverProperties.getSizeInBytes(SERVER_MAX_REQUEST_SIZE, 256 * 1024); // 256KB
    topicOffsetCheckIntervalMs = serverProperties.getInt(SERVER_SOURCE_TOPIC_OFFSET_CHECK_INTERVAL_MS, (int) TimeUnit.SECONDS.toMillis(60));
    nettyGracefulShutdownPeriodSeconds = serverProperties.getInt(SERVER_NETTY_GRACEFUL_SHUTDOWN_PERIOD_SECONDS, 30); //30 seconds
    nettyWorkerThreadCount = serverProperties.getInt(SERVER_NETTY_WORKER_THREADS, 0);
    /**
     * {@link com.linkedin.venice.utils.queues.FairBlockingQueue} could cause non-deterministic behavior during test.
     * Disable it by default for now.
     *
     * In the test of feature store user case, when we did a rolling bounce of storage nodes, the high latency happened
     * to one or two storage nodes randomly. And when we restarted the node with high latency, the high latency could
     * disappear, but other nodes could start high latency.
     * After switching to {@link java.util.concurrent.LinkedBlockingQueue}, this issue never happened.
     *
     * TODO: figure out the issue with {@link com.linkedin.venice.utils.queues.FairBlockingQueue}.
     */
    fairStorageExecutionQueue = serverProperties.getBoolean(SERVER_FAIR_STORAGE_EXECUTION_QUEUE, false);
    databaseSyncBytesIntervalForTransactionalMode = serverProperties.getSizeInBytes(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 32 * 1024 * 1024); // 32MB
    databaseSyncBytesIntervalForDeferredWriteMode = serverProperties.getSizeInBytes(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, 60 * 1024 * 1024); // 60MB
    diskFullThreshold = serverProperties.getDouble(SERVER_DISK_FULL_THRESHOLD, 0.90);
    partitionGracefulDropDelaySeconds = serverProperties.getInt(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 30); // 30 seconds
    storageLeakedResourceCleanUpIntervalInMS = TimeUnit.MINUTES.toMillis(serverProperties.getLong(SERVER_LEAKED_RESOURCE_CLEAN_UP_INTERVAL_IN_MINUTES, 6 * 60)); // 6 hours by default
    readOnlyForBatchOnlyStoreEnabled = serverProperties.getBoolean(SERVER_DB_READ_ONLY_FOR_BATCH_ONLY_STORE_ENABLED, true);
    quotaEnforcementEnabled = serverProperties.getBoolean(SERVER_QUOTA_ENFORCEMENT_ENABLED, false);
    //June 2018, venice-6 nodes were hitting ~20k keys per second. August 2018, no cluster has nodes above 3.5k keys per second
    nodeCapacityInRcu = serverProperties.getLong(SERVER_NODE_CAPACITY_RCU, 50000);
    kafkaMaxPollRecords = serverProperties.getInt(SERVER_KAFKA_MAX_POLL_RECORDS, 100);
    kafkaPollRetryTimes = serverProperties.getInt(SERVER_KAFKA_POLL_RETRY_TIMES, 100);
    kafkaPollRetryBackoffMs = serverProperties.getInt(SERVER_KAFKA_POLL_RETRY_BACKOFF_MS, 0);
    diskHealthCheckIntervalInMS = TimeUnit.SECONDS.toMillis(serverProperties.getLong(SERVER_DISK_HEALTH_CHECK_INTERVAL_IN_SECONDS, 60)); // 1 minute by default
    diskHealthCheckServiceEnabled = serverProperties.getBoolean(SERVER_DISK_HEALTH_CHECK_SERVICE_ENABLED, true);
    computeFastAvroEnabled = serverProperties.getBoolean(SERVER_COMPUTE_FAST_AVRO_ENABLED, false);
    participantMessageConsumptionDelayMs = serverProperties.getLong(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, 5000);
  }

  public int getListenerPort() {
    return listenerPort;
  }


  /**
   * Get base path of Venice storage data.
   *
   * @return Base path of persisted Venice database files.
   */
  public String getDataBasePath() {
    return this.dataBasePath;
  }

  public boolean isAutoCreateDataPath(){
    return autoCreateDataPath;
  }

  public BdbServerConfig getBdbServerConfig() {
    return this.bdbServerConfig;
  }

  public RocksDBServerConfig getRocksDBServerConfig() {
    return rocksDBServerConfig;
  }

  public boolean isServerWhiteLIstEnabled() {
    return enableServerWhiteList;
  }

  public int getMaxOnlineOfflineStateTransitionThreadNumber() {
    return maxOnlineOfflineStateTransitionThreadNumber;
  }

  public int getMaxLeaderFollowerStateTransitionThreadNumber() {
    return maxLeaderFollowerStateTransitionThreadNumber;
  }

  public int getStoreWriterNumber() {
    return this.storeWriterNumber;
  }

  public long getStoreWriterBufferMemoryCapacity() {
    return this.storeWriterBufferMemoryCapacity;
  }

  public long getStoreWriterBufferNotifyDelta() {
    return this.storeWriterBufferNotifyDelta;
  }

  public int getRestServiceStorageThreadNum() {
    return restServiceStorageThreadNum;
  }

  public int getServerComputeThreadNum() {
    return serverComputeThreadNum;
  }

  public int getNettyIdleTimeInSeconds() {
    return nettyIdleTimeInSeconds;
  }

  public int getMaxRequestSize() {
    return maxRequestSize;
  }

  public int getTopicOffsetCheckIntervalMs() {
    return topicOffsetCheckIntervalMs;
  }

  public int getNettyGracefulShutdownPeriodSeconds() {
    return nettyGracefulShutdownPeriodSeconds;
  }

  public int getNettyWorkerThreadCount() {
    return nettyWorkerThreadCount;
  }

  public boolean isFairStorageExecutionQueue() {
    return fairStorageExecutionQueue;
  }
  public long getDatabaseSyncBytesIntervalForTransactionalMode() {
    return databaseSyncBytesIntervalForTransactionalMode;
  }

  public long getDatabaseSyncBytesIntervalForDeferredWriteMode() {
    return databaseSyncBytesIntervalForDeferredWriteMode;
  }

  public double getDiskFullThreshold(){
    return diskFullThreshold;
  }

  public int getPartitionGracefulDropDelaySeconds() {
    return partitionGracefulDropDelaySeconds;
  }

  public long getStorageLeakedResourceCleanUpIntervalInMS() {
    return storageLeakedResourceCleanUpIntervalInMS;
  }

  public boolean isReadOnlyForBatchOnlyStoreEnabled() {
    return readOnlyForBatchOnlyStoreEnabled;
  }

  public boolean isQuotaEnforcementDisabled() {
    return !quotaEnforcementEnabled;
  }

  public long getNodeCapacityInRcu(){
    return nodeCapacityInRcu;
  }

  public int getKafkaMaxPollRecords() {
    return kafkaMaxPollRecords;
  }

  public int getKafkaPollRetryTimes() {
    return kafkaPollRetryTimes;
  }

  public int getKafkaPollRetryBackoffMs() {
    return kafkaPollRetryBackoffMs;
  }

  public long getDiskHealthCheckIntervalInMS() {
    return diskHealthCheckIntervalInMS;
  }

  public boolean isDiskHealthCheckServiceEnabled() {
    return diskHealthCheckServiceEnabled;
  }

  public BlockingQueue<Runnable> getExecutionQueue() {
    if (isFairStorageExecutionQueue()) {
      return new FairBlockingQueue<>();
    } else {
      return new LinkedBlockingQueue<>();
    }
  }

  public boolean isComputeFastAvroEnabled() {
    return computeFastAvroEnabled;
  }

  public long getParticipantMessageConsumptionDelayMs() { return participantMessageConsumptionDelayMs; }
}
