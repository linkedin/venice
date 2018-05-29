package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.storage.MetadataRetriever;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.AggBdbStorageEngineStats;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedDIVStats;
import com.linkedin.venice.stats.StoreBufferServiceStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.bdb.BdbStorageEngine;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.Utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import io.tehuti.metrics.MetricsRepository;
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

  private final StoreRepository storeRepository;
  private final VeniceConfigLoader veniceConfigLoader;

  private final Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>();
  private final StorageMetadataService storageMetadataService;
  private final TopicManager topicManager;

  private final ReadOnlyStoreRepository metadataRepo;
  private final ReadOnlySchemaRepository schemaRepo;

  private final AggStoreIngestionStats ingestionStats;
  private final AggBdbStorageEngineStats storageEngineStats;
  private final AggVersionedDIVStats versionedDIVStats;

  /**
   * Store buffer service to persist data into local bdb for all the stores.
   */
  private final StoreBufferService storeBufferService;

  private final VeniceServerConsumerFactory veniceConsumerFactory;

  /**
   * A repository mapping each Kafka Topic to it corresponding Ingestion task responsible
   * for consuming messages and making changes to the local store accordingly.
   */
  private final Map<String, StoreIngestionTask> topicNameToIngestionTaskMap;
  private final EventThrottler consumptionBandwidthThrottler;
  private final EventThrottler consumptionRecordsCountThrottler;

  private ExecutorService consumerExecutorService;

  // Need to make sure that the service has started before start running KafkaConsumptionTask.
  private final AtomicBoolean isRunning;

  public KafkaStoreIngestionService(StoreRepository storeRepository,
                                    VeniceConfigLoader veniceConfigLoader,
                                    StorageMetadataService storageMetadataService,
                                    ReadOnlyStoreRepository metadataRepo,
                                    ReadOnlySchemaRepository schemaRepo,
                                    MetricsRepository metricsRepository) {
    this.storeRepository = storeRepository;
    this.storageMetadataService = storageMetadataService;
    this.metadataRepo = metadataRepo;
    this.schemaRepo = schemaRepo;

    this.topicNameToIngestionTaskMap = new ConcurrentHashMap<>();
    this.isRunning = new AtomicBoolean(false);

    this.veniceConfigLoader = veniceConfigLoader;

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();
    veniceConsumerFactory = new VeniceServerConsumerFactory(serverConfig);
    this.consumptionBandwidthThrottler =
        new EventThrottler(serverConfig.getKafkaFetchQuotaBytesPerSecond(), serverConfig.getKafkaFetchQuotaTimeWindow(),
            "Kafka_consumption_bandwidth", false, EventThrottler.BLOCK_STRATEGY);
    this.consumptionRecordsCountThrottler = new EventThrottler(serverConfig.getKafkaFetchQuotaRecordPerSecond(),
        serverConfig.getKafkaFetchQuotaTimeWindow(), "kafka_consumption_records_count", false, EventThrottler.BLOCK_STRATEGY);
    this.topicManager = new TopicManager(serverConfig.getKafkaZkAddress(), veniceConsumerFactory);

    VeniceNotifier notifier = new LogNotifier();
    this.notifiers.add(notifier);

    this.ingestionStats = new AggStoreIngestionStats(metricsRepository);
    this.storageEngineStats = new AggBdbStorageEngineStats(metricsRepository);
    this.versionedDIVStats = new AggVersionedDIVStats(metricsRepository, metadataRepo);
    this.storeBufferService = new StoreBufferService(
        serverConfig.getStoreWriterNumber(),
        serverConfig.getStoreWriterBufferMemoryCapacity(),
        serverConfig.getStoreWriterBufferNotifyDelta());
    /**
     * Collect metrics for {@link #storeBufferService}.
     * Since all the metrics will be collected passively, there is no need to
     * keep the reference of this {@link StoreBufferServiceStats} variable.
     */
    new StoreBufferServiceStats(metricsRepository, this.storeBufferService);
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

    // Although the StoreConsumptionTasks are now running in their own threads, there is no async
    // process that needs to finish before the KafkaStoreIngestionService can be considered
    // started, so we are done with the start up process.

    return true;
  }

  private StoreIngestionTask getConsumerTask(VeniceStoreConfig veniceStore) {
    String storeName = Version.parseStoreFromKafkaTopicName(veniceStore.getStoreName());
    int storeVersion = Version.parseVersionFromKafkaTopicName(veniceStore.getStoreName());
    Store store = metadataRepo.getStore(storeName);
    BooleanSupplier isStoreVersionCurrent = () -> store.getCurrentVersion() == storeVersion;
    Optional<HybridStoreConfig> hybridStoreConfig = Optional.ofNullable(store.getHybridStoreConfig());
    DiskUsage diskUsage = new DiskUsage(
        veniceConfigLoader.getVeniceServerConfig().getDataBasePath(),
        veniceConfigLoader.getVeniceServerConfig().getDiskFullThreshold());

    return new StoreIngestionTask(veniceConsumerFactory, getKafkaConsumerProperties(veniceStore), storeRepository,
        storageMetadataService, notifiers, consumptionBandwidthThrottler, consumptionRecordsCountThrottler,
        veniceStore.getStoreName(), schemaRepo, topicManager, ingestionStats, versionedDIVStats, storeBufferService,
        isStoreVersionCurrent, hybridStoreConfig, veniceStore.getSourceTopicOffsetCheckIntervalMs(),
        veniceStore.getKafkaReadCycleDelayMs(), veniceStore.getKafkaEmptyPollSleepMs(),
        veniceStore.getDatabaseSyncBytesIntervalForTransactionalMode(),
        veniceStore.getDatabaseSyncBytesIntervalForDeferredWriteMode(), diskUsage);

  }

  /**
   * Stops all the Kafka consumption tasks.
   * Closes all the Kafka clients.
   */
  @Override
  public void stopInner() throws Exception {
    logger.info("Shutting down Kafka consumer service");
    isRunning.set(false);

    topicNameToIngestionTaskMap.values().forEach(StoreIngestionTask::close);

    if (consumerExecutorService != null) {
      consumerExecutorService.shutdown();

      try {
        consumerExecutorService.awaitTermination(5, TimeUnit.SECONDS);
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
    logger.info("Shut down complete");
  }

  /**
   * Starts consuming messages from Kafka Partition corresponding to Venice Partition.
   * Subscribes to partition if required.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public synchronized void startConsumption(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask == null || !consumerTask.isRunning()) {
      consumerTask = getConsumerTask(veniceStore);
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

  void updateStatsEmission(Map<String, StoreIngestionTask> taskMap, String storeName, int maximumVersion) {
    taskMap.forEach((topicName, task) -> {
      if (Version.parseStoreFromKafkaTopicName(topicName).equals(storeName)) {
        if (Version.parseVersionFromKafkaTopicName(topicName) < maximumVersion) {
          task.disableMetricsEmission();
        } else {
          task.enableMetricsEmission();
          AbstractStorageEngine engine = storeRepository.getLocalStorageEngine(topicName);
          if (engine instanceof BdbStorageEngine) {
            storageEngineStats.setBdbEnvironment(storeName, ((BdbStorageEngine) engine).getBdbEnvironment());
          }

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
  public synchronized void killConsumptionTask(VeniceStoreConfig veniceStore) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if (consumerTask != null && consumerTask.isRunning()) {
      consumerTask.kill();
      topicNameToIngestionTaskMap.remove(topic);
      logger.info("Killed consumption task for Topic " + topic);
    } else {
      logger.warn("Ignoring kill signal for Topic " + topic);
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
    kafkaConsumerProperties
        .setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(storeConfig.getKafkaFetchMaxTimeMS()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        String.valueOf(storeConfig.getKafkaFetchPartitionMaxSizePerSecond()));
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
