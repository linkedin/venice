package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * Manages Kafka topics and partitions that need to be consumed for the stores on this node.
 *
 * Launches KafkaPerStoreConsumptionTask for each store to consume and process messages.
 *
 * Uses the "new" Kafka Consumer.
 */
public class KafkaConsumerPerStoreService extends AbstractVeniceService implements KafkaConsumerService {
  private static final String GROUP_ID_FORMAT = "%s_%s";

  private static final Logger logger = Logger.getLogger(KafkaConsumerPerStoreService.class);

  private final StoreRepository storeRepository;
  private final VeniceConfigLoader veniceConfigLoader;

  private final Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>();
  private final OffsetManager offsetManager;
  private final TopicManager topicManager;

  private final ReadOnlySchemaRepository schemaRepo;

  /**
   * A repository mapping each Kafka Topic to it corresponding Consumption task responsible
   * for consuming messages and making changes to the local store accordingly.
   */
  private final Map<String, StoreConsumptionTask> topicNameToConsumptionTaskMap;
  private final EventThrottler throttler;

  private ExecutorService consumerExecutorService;

  // Need to make sure that the service has started before start running KafkaConsumptionTask.
  private final AtomicBoolean isRunning;

  public KafkaConsumerPerStoreService(StoreRepository storeRepository,
                                      VeniceConfigLoader veniceConfigLoader,
                                      OffsetManager offsetManager,
                                      ReadOnlySchemaRepository schemaRepo) {
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;
    this.schemaRepo = schemaRepo;

    this.topicNameToConsumptionTaskMap = Collections.synchronizedMap(new HashMap<>());
    isRunning = new AtomicBoolean(false);

    this.veniceConfigLoader = veniceConfigLoader;

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();

    long maxKafkaFetchBytesPerSecond = serverConfig.getMaxKafkaFetchBytesPerSecond();
    throttler = new EventThrottler(maxKafkaFetchBytesPerSecond);
    topicManager = new TopicManager(veniceConfigLoader.getVeniceClusterConfig().getKafkaZkAddress());

    VeniceNotifier notifier = new LogNotifier();
    notifiers.add(notifier);
  }

  /**
   * Starts the Kafka consumption tasks for already subscribed partitions.
   */
  @Override
  public boolean startInner() {
    logger.info("Enabling consumerExecutorService and kafka consumer tasks ");
    consumerExecutorService = Executors.newCachedThreadPool(new DaemonThreadFactory("venice-consumer"));
    topicNameToConsumptionTaskMap.values().forEach(consumerExecutorService::submit);
    isRunning.set(true);

    // Although the StoreConsumptionTasks are now running in their own threads, there is no async
    // process that needs to finish before the KafkaConsumerPerStoreService can be considered
    // started, so we are done with the start up process.
    return true;
  }

  private StoreConsumptionTask getConsumerTask(VeniceStoreConfig veniceStore) {
    return new StoreConsumptionTask(new VeniceConsumerFactory(), getKafkaConsumerProperties(veniceStore), storeRepository,
            offsetManager , notifiers, throttler , veniceStore.getStoreName(), schemaRepo, topicManager);
  }

  /**
   * Stops all the Kafka consumption tasks.
   * Closes all the Kafka clients.
   */
  @Override
  public void stopInner() {
    logger.info("Shutting down Kafka consumer service");
    isRunning.set(false);

    topicNameToConsumptionTaskMap.values().forEach(StoreConsumptionTask::close);

    if (consumerExecutorService != null) {
      consumerExecutorService.shutdown();

      try {
        consumerExecutorService.awaitTermination(5, TimeUnit.SECONDS);
      } catch(InterruptedException e) {
        logger.info("Error shutting down consumer service ", e);
      }
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
    StoreConsumptionTask consumerTask = topicNameToConsumptionTaskMap.get(topic);
    if(consumerTask == null || !consumerTask.isRunning()) {
      consumerTask = getConsumerTask(veniceStore);
      topicNameToConsumptionTaskMap.put(topic, consumerTask);
      if(!isRunning.get()) {
        logger.info("Ignoring Start consumption message as service is stopping. Topic " + topic + " Partition " + partitionId);
        return;
      }
      consumerExecutorService.submit(consumerTask);
    }
    consumerTask.subscribePartition(topic, partitionId);
    logger.info("Started Consuming - Kafka Partition: " + topic + "-" + partitionId + ".");
  }

  /**
   * Stops consuming messages from Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public synchronized void stopConsumption(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreConsumptionTask consumerTask = topicNameToConsumptionTaskMap.get(topic);
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
    StoreConsumptionTask consumerTask = topicNameToConsumptionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.resetPartitionConsumptionOffset(topic, partitionId);
    } else {
      logger.info("There is no active task for Topic " + topic + " Partition " + partitionId
          +" Using offset manager directly");
      offsetManager.clearOffset(topic, partitionId);
    }
    logger.info("Offset reset to beginning - Kafka Partition: " + topic + "-" + partitionId + ".");
  }

  @Override
  public synchronized void killConsumptionTask(VeniceStoreConfig veniceStore) {
    String topic = veniceStore.getStoreName();
    StoreConsumptionTask consumerTask = topicNameToConsumptionTaskMap.get(topic);
    if (consumerTask != null && consumerTask.isRunning()) {
      consumerTask.kill();
      topicNameToConsumptionTaskMap.remove(topic);
      logger.info("Killed consumption task for Topic " + topic);
    } else {
      logger.warn("Ignoring kill signal for Topic " + topic);
    }
  }

  @Override
  public List<StoreConsumptionTask> getRunningConsumptionTasksByStore(String storeName) {
    return topicNameToConsumptionTaskMap.entrySet().stream()
      .filter(entry -> Version.parseStoreFromKafkaTopicName(entry.getKey()).equals(storeName)
        && entry.getValue().isRunning())
      .map(Map.Entry::getValue)
      .collect(Collectors.toList());
  }

  @Override
  public void addNotifier(VeniceNotifier notifier) {
    notifiers.add(notifier);
  }

  @Override
  public synchronized boolean containsRunningConsumption(VeniceStoreConfig veniceStore) {
    String topic = veniceStore.getStoreName();
    StoreConsumptionTask consumerTask = topicNameToConsumptionTaskMap.get(topic);
    if (consumerTask != null && consumerTask.isRunning()) {
      return true;
    }
    return false;
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
    kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaValueSerializer.class.getName());
    return kafkaConsumerProperties;
  }

}
