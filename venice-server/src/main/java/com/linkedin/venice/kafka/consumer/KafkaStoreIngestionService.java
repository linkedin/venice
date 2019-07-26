package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.LeaderFollowerParticipantModel;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.stats.ParticipantStoreConsumptionStats;
import com.linkedin.venice.storage.MetadataRetriever;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedDIVStats;
import com.linkedin.venice.stats.StoreBufferServiceStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriterFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import io.tehuti.metrics.MetricsRepository;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * Manages Kafka topics and partitions that need to be consumed for the stores on this node.
 *
 * Launches {@link StoreIngestionTask} for each store version to consume and process messages.
 *
 * Uses the "new" Kafka Consumer.
 */
public class KafkaStoreIngestionService extends AbstractVeniceService implements StoreIngestionService, MetadataRetriever {
  private static final String GROUP_ID_FORMAT = "%s_%s";

  private static final Logger logger = Logger.getLogger(KafkaStoreIngestionService.class);

  private static final long WAIT_THREAD_TO_STOP_TIMEOUT_MS = 10000;

  private final VeniceConfigLoader veniceConfigLoader;

  private final Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>();
  private final StorageMetadataService storageMetadataService;

  private final ReadOnlyStoreRepository metadataRepo;

  private final AggStoreIngestionStats ingestionStats;

  /**
   * Store buffer service to persist data into local bdb for all the stores.
   */
  private final StoreBufferService storeBufferService;

  /**
   * A repository mapping each Kafka Topic to it corresponding Ingestion task responsible
   * for consuming messages and making changes to the local store accordingly.
   */
  private final Map<String, StoreIngestionTask> topicNameToIngestionTaskMap;
  private final Optional<SchemaReader> schemaReader;

  private ExecutorService consumerExecutorService;

  // Need to make sure that the service has started before start running KafkaConsumptionTask.
  private final AtomicBoolean isRunning;

  private ParticipantStoreConsumptionTask participantStoreConsumptionTask = null;

  private Thread participantStoreConsumptionTaskThread = null;

  private StoreIngestionTaskFactory ingestionTaskFactory;

  public KafkaStoreIngestionService(StoreRepository storeRepository,
                                    VeniceConfigLoader veniceConfigLoader,
                                    StorageMetadataService storageMetadataService,
                                    ReadOnlyStoreRepository metadataRepo,
                                    ReadOnlySchemaRepository schemaRepo,
                                    MetricsRepository metricsRepository,
                                    Optional<SchemaReader> schemaReader,
                                    Optional<ClientConfig> clientConfig) {
    this.storageMetadataService = storageMetadataService;
    this.metadataRepo = metadataRepo;

    this.topicNameToIngestionTaskMap = new ConcurrentHashMap<>();
    this.isRunning = new AtomicBoolean(false);

    this.veniceConfigLoader = veniceConfigLoader;

    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(veniceConfigLoader.getVeniceClusterConfig().getClusterProperties().toProperties());

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();
    VeniceServerConsumerFactory veniceConsumerFactory = new VeniceServerConsumerFactory(serverConfig);
    EventThrottler consumptionBandwidthThrottler =
        new EventThrottler(serverConfig.getKafkaFetchQuotaBytesPerSecond(), serverConfig.getKafkaFetchQuotaTimeWindow(),
            "Kafka_consumption_bandwidth", false, EventThrottler.BLOCK_STRATEGY);
    EventThrottler consumptionRecordsCountThrottler = new EventThrottler(serverConfig.getKafkaFetchQuotaRecordPerSecond(),
        serverConfig.getKafkaFetchQuotaTimeWindow(), "kafka_consumption_records_count", false, EventThrottler.BLOCK_STRATEGY);
    TopicManager topicManager = new TopicManager(serverConfig.getKafkaZkAddress(), veniceConsumerFactory);

    VeniceNotifier notifier = new LogNotifier();
    this.notifiers.add(notifier);

    this.ingestionStats = new AggStoreIngestionStats(metricsRepository);
    AggVersionedDIVStats versionedDIVStats = new AggVersionedDIVStats(metricsRepository, metadataRepo);
    this.storeBufferService = new StoreBufferService(
        serverConfig.getStoreWriterNumber(),
        serverConfig.getStoreWriterBufferMemoryCapacity(),
        serverConfig.getStoreWriterBufferNotifyDelta());
    this.schemaReader = schemaReader;
    /**
     * Collect metrics for {@link #storeBufferService}.
     * Since all the metrics will be collected passively, there is no need to
     * keep the reference of this {@link StoreBufferServiceStats} variable.
     */
    new StoreBufferServiceStats(metricsRepository, this.storeBufferService);
    if (clientConfig.isPresent()) {
      String clusterName = veniceConfigLoader.getVeniceClusterConfig().getClusterName();
      participantStoreConsumptionTask = new ParticipantStoreConsumptionTask(clusterName, this,
          new ParticipantStoreConsumptionStats(metricsRepository, clusterName),
          ClientConfig.cloneConfig(clientConfig.get()), serverConfig.getParticipantMessageConsumptionDelayMs());
      ThreadFactory threadFactory = new DaemonThreadFactory(ParticipantStoreConsumptionTask.class.getSimpleName());
      participantStoreConsumptionTaskThread = threadFactory.newThread(participantStoreConsumptionTask);
    } else {
      logger.info("Unable to start participant store consumption task because client config is not provided, jobs "
          + "may not be killed if admin helix messaging channel is disabled");
    }

    /**
     * Use the same diskUsage instance for all ingestion tasks; so that all the ingestion tasks can update the same
     * remaining disk space state to provide a more accurate alert.
     */
    DiskUsage diskUsage = new DiskUsage(
        veniceConfigLoader.getVeniceServerConfig().getDataBasePath(),
        veniceConfigLoader.getVeniceServerConfig().getDiskFullThreshold());

    ingestionTaskFactory = StoreIngestionTaskFactory.builder()
        .setVeniceWriterFactory(veniceWriterFactory)
        .setVeniceConsumerFactory(veniceConsumerFactory)
        .setStoreRepository(storeRepository)
        .setStorageMetadataService(storageMetadataService)
        .setNotifiersQueue(notifiers)
        .setBandwidthThrottler(consumptionBandwidthThrottler)
        .setRecordsThrottler(consumptionRecordsCountThrottler)
        .setSchemaRepository(schemaRepo)
        .setTopicManager(topicManager)
        .setStoreIngestionStats(ingestionStats)
        .setVersionedDIVStats(versionedDIVStats)
        .setStoreBufferService(storeBufferService)
        .setServerConfig(serverConfig)
        .setDiskUsage(diskUsage)
        .build();
  }

  /**
   * Starts the Kafka consumption tasks for already subscribed partitions.
   */
  @Override
  public boolean startInner() {
    logger.info("Enabling consumerExecutorService and kafka consumer tasks ");
    consumerExecutorService = Executors.newCachedThreadPool();
    topicNameToIngestionTaskMap.values().forEach(consumerExecutorService::submit);
    isRunning.set(true);

    storeBufferService.start();
    if (participantStoreConsumptionTaskThread != null) {
      participantStoreConsumptionTaskThread.start();
    }
    // Although the StoreConsumptionTasks are now running in their own threads, there is no async
    // process that needs to finish before the KafkaStoreIngestionService can be considered
    // started, so we are done with the start up process.

    return true;
  }

  private StoreIngestionTask getConsumerTask(VeniceStoreConfig veniceStoreConfig, boolean isLeaderFollowerModel) {
    String storeName = Version.parseStoreFromKafkaTopicName(veniceStoreConfig.getStoreName());
    int storeVersion = Version.parseVersionFromKafkaTopicName(veniceStoreConfig.getStoreName());
    Store store = metadataRepo.getStore(storeName);
    Optional<Version> version = store.getVersion(storeVersion);
    if (!version.isPresent()) {
      // In theory, the version should exist since the corresponding store ingestion is ready to start.
      // The issue could be caused by race condition that the in-memory metadata hasn't been refreshed yet,
      // So here will refresh that store explicitly once.
      store = metadataRepo.refreshOneStore(storeName);
      version = store.getVersion(storeVersion);
      if (!version.isPresent()) {
        throw new VeniceException("Version: " + storeVersion + " doesn't exist in store: " + storeName);
      }
    }
    final Store finalStore = store;
    BooleanSupplier isStoreVersionCurrent = () -> finalStore.getCurrentVersion() == storeVersion;
    Optional<HybridStoreConfig> hybridStoreConfig = Optional.ofNullable(store.getHybridStoreConfig());

    boolean bufferReplayEnabledForHybrid = version.get().isBufferReplayEnabledForHybrid();

    Properties kafkaProperties = getKafkaConsumerProperties(veniceStoreConfig);
    if (schemaReader.isPresent()) {
      kafkaProperties.put(InternalAvroSpecificSerializer.VENICE_SCHEMA_READER_CONFIG, schemaReader.get());
    }

    return ingestionTaskFactory.getNewIngestionTask(isLeaderFollowerModel, kafkaProperties, isStoreVersionCurrent,
        hybridStoreConfig, store.isIncrementalPushEnabled(), veniceStoreConfig, bufferReplayEnabledForHybrid);
  }

  /**
   * Stops all the Kafka consumption tasks.
   * Closes all the Kafka clients.
   */
  @Override
  public void stopInner() throws Exception {
    logger.info("Shutting down Kafka consumer service");
    isRunning.set(false);

    IOUtils.closeQuietly(participantStoreConsumptionTask);
    if (participantStoreConsumptionTaskThread != null) {
      participantStoreConsumptionTaskThread.join(WAIT_THREAD_TO_STOP_TIMEOUT_MS);
      if (participantStoreConsumptionTaskThread.isAlive()) {
        participantStoreConsumptionTaskThread.interrupt();
      }
    }

    topicNameToIngestionTaskMap.values().forEach(StoreIngestionTask::close);

    if (consumerExecutorService != null) {
      consumerExecutorService.shutdown();

      try {
        consumerExecutorService.awaitTermination(30, TimeUnit.SECONDS);
      } catch(InterruptedException e) {
        logger.info("Error shutting down consumer service", e);
      }
    }

    if (null != storeBufferService) {
      storeBufferService.stop();
    }

    for(VeniceNotifier notifier: notifiers ) {
      notifier.close();
    }

    schemaReader.ifPresent(sr -> sr.close());
    logger.info("Shut down complete");
  }

  /**
   * Starts consuming messages from Kafka Partition corresponding to Venice Partition.
   * Subscribes to partition if required.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public synchronized void startConsumption(VeniceStoreConfig veniceStore, int partitionId, boolean isLeaderFollowerModel) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask == null || !consumerTask.isRunning()) {
      consumerTask = getConsumerTask(veniceStore, isLeaderFollowerModel);
      topicNameToIngestionTaskMap.put(topic, consumerTask);
      if(!isRunning.get()) {
        logger.info("Ignoring Start consumption message as service is stopping. Topic " + topic + " Partition " + partitionId);
        return;
      }
      consumerExecutorService.submit(consumerTask);
    }

    /**
     * Since Venice metric is store-level and it would have multiply topics tasks exist in the same time.
     * Only the task with largest version would emit it stats. That being said, relying on the {@link #metadataRepo}
     * to get the max version may be unreliable, since the information in this object is not guaranteed
     * to be up to date. As a sanity check, we will also look at the version in the topic name, and
     * pick whichever number is highest as the max version number.
     */
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int maxVersionNumberFromTopicName = Version.parseVersionFromKafkaTopicName(topic);
    int maxVersionNumberFromMetadataRepo = getStoreMaximumVersionNumber(storeName);
    if (maxVersionNumberFromTopicName > maxVersionNumberFromMetadataRepo) {
      logger.warn("Got stale info from metadataRepo. maxVersionNumberFromTopicName: " + maxVersionNumberFromTopicName +
          ", maxVersionNumberFromMetadataRepo: " + maxVersionNumberFromMetadataRepo +
          ". Will rely on the topic name's version.");
    }
    int maxVersionNumber = Math.max(maxVersionNumberFromMetadataRepo, maxVersionNumberFromTopicName);
    updateStatsEmission(topicNameToIngestionTaskMap, storeName, maxVersionNumber);

    consumerTask.subscribePartition(topic, partitionId);
    logger.info("Started Consuming - Kafka Partition: " + topic + "-" + partitionId + ".");
  }

  @Override
  public synchronized void promoteToLeader(VeniceStoreConfig veniceStoreConfig, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    String topic = veniceStoreConfig.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.promoteToLeader(topic, partitionId, checker);
    } else {
      logger.warn("Ignoring standby to leader transition message for Topic " + topic + " Partition " + partitionId);
    }
  }

  @Override
  public synchronized void demoteToStandby(VeniceStoreConfig veniceStoreConfig, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    String topic = veniceStoreConfig.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.demoteToStandby(topic, partitionId, checker);
    } else {
      logger.warn("Ignoring leader to standby transition message for Topic " + topic + " Partition " + partitionId);
    }
  }

  void updateStatsEmission(Map<String, StoreIngestionTask> taskMap, String storeName, int maximumVersion) {
    taskMap.forEach((topicName, task) -> {
      if (Version.parseStoreFromKafkaTopicName(topicName).equals(storeName)) {
        if (Version.parseVersionFromKafkaTopicName(topicName) < maximumVersion) {
          task.disableMetricsEmission();
        } else {
          task.enableMetricsEmission();
          ingestionStats.updateStoreConsumptionTask(storeName, task);
        }
      }
    });
  }

  private int getStoreMaximumVersionNumber(String storeName) {
    Store store = metadataRepo.getStore(storeName);
    if (store == null) {
      throw new VeniceException("Could not find store " + storeName + " info in ZK");
    }

    int maxVersionNumber = store.getLargestUsedVersionNumber();
    if (maxVersionNumber == 0) {
      throw new VeniceException("No version has been created yet for store " + storeName);
    }

    return maxVersionNumber;
  }

  /**
   * Stops consuming messages from Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public synchronized void stopConsumption(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.unSubscribePartition(topic, partitionId);
    } else {
      logger.warn("Ignoring stop consumption message for Topic " + topic + " Partition " + partitionId);
    }
  }

  /**
   * Resets Offset to beginning for Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public void resetConsumptionOffset(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.resetPartitionConsumptionOffset(topic, partitionId);
    } else {
      logger.info("There is no active task for Topic " + topic + " Partition " + partitionId
          +" Using offset manager directly");
      storageMetadataService.clearOffset(topic, partitionId);
    }
    logger.info("Offset reset to beginning - Kafka Partition: " + topic + "-" + partitionId + ".");
  }

  @Override
  public synchronized void killConsumptionTask(String topicName) {
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topicName);
    if (consumerTask != null && consumerTask.isRunning()) {
      consumerTask.kill();
      topicNameToIngestionTaskMap.remove(topicName);
      logger.info("Killed consumption task for Topic " + topicName);
    } else {
      logger.warn("Ignoring kill signal for Topic " + topicName);
    }
  }

  @Override
  public void addNotifier(VeniceNotifier notifier) {
    notifiers.add(notifier);
  }

  @Override
  public synchronized boolean containsRunningConsumption(VeniceStoreConfig veniceStore) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if (consumerTask != null && consumerTask.isRunning()) {
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean isPartitionConsuming(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    return consumerTask != null && consumerTask.isRunning() && consumerTask.isPartitionConsuming(partitionId);
  }

  @Override
  public Set<String> getIngestingTopics() {
    return topicNameToIngestionTaskMap.keySet();
  }

  /**
   * @return Group Id for kafka consumer.
   */
  private static String getGroupId(String topic) {
    return String.format(GROUP_ID_FORMAT, topic, Utils.getHostName());
  }

  /**
   * @return Properties Kafka properties corresponding to the venice store.
   */
  private static Properties getKafkaConsumerProperties(VeniceStoreConfig storeConfig) {
    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, storeConfig.getKafkaBootstrapServers());
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Venice is persisting offset in local offset db.
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    String groupId = getGroupId(storeConfig.getStoreName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    /**
     * Temporarily we are going to use group_id as client_id as well since it is unique in cluster level.
     * With unique client_id, it will be easier for us to check Kafka consumer related metrics through JMX.
     * TODO: Kafka is throttling based on client_id, need to investigate whether we should use Kafka throttling or not.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
    kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        KafkaKeySerializer.class.getName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OptimizedKafkaValueSerializer.class.getName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
        String.valueOf(storeConfig.getKafkaFetchMinSizePerSecond()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
        String.valueOf(storeConfig.getKafkaFetchMaxSizePerSecond()));
    /**
     * The following setting is used to control the maximum number of records to returned in one poll request.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(storeConfig.getKafkaMaxPollRecords()));
    kafkaConsumerProperties
        .setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(storeConfig.getKafkaFetchMaxTimeMS()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        String.valueOf(storeConfig.getKafkaFetchPartitionMaxSizePerSecond()));
    kafkaConsumerProperties.setProperty(ApacheKafkaConsumer.CONSUMER_POLL_RETRY_TIMES_CONFIG,
        String.valueOf(storeConfig.getKafkaPollRetryTimes()));
    kafkaConsumerProperties.setProperty(ApacheKafkaConsumer.CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG,
        String.valueOf(storeConfig.getKafkaPollRetryBackoffMs()));
    return kafkaConsumerProperties;
  }

  @Override
  public Optional<Long> getOffset(String topicName, int partitionId) {
    StoreIngestionTask ingestionTask = topicNameToIngestionTaskMap.get(topicName);
    if (null == ingestionTask) {
      return Optional.empty();
    }
    return ingestionTask.getCurrentOffset(partitionId);
  }

  @Override
  public boolean isStoreVersionChunked(String topicName) {
    return storageMetadataService.isStoreVersionChunked(topicName);
  }

  @Override
  public CompressionStrategy getStoreVersionCompressionStrategy(String topicName) {
    return storageMetadataService.getStoreVersionCompressionStrategy(topicName);
  }
}
