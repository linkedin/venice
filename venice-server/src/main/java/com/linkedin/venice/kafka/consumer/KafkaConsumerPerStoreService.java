package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.notifier.KafkaNotifier;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.partitioner.PartitionZeroPartitioner;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KafkaValueSerializer;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

  // FIXME: Get rid of hard-coded topic name
  private static final String ACK_PARTITION_CONSUMPTION_KAFKA_TOPIC = "venice-partition-consumption-acknowledgement-1";
  private static final String VENICE_SERVICE_NAME = "kafka-consumer-service";
  private static final String GROUP_ID_FORMAT = "%s_%s_%d";

  private static final Logger logger = Logger.getLogger(KafkaConsumerPerStoreService.class.getName());

  private final StoreRepository storeRepository;
  private final VeniceConfigLoader veniceConfigLoader;

  private final Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>();
  private final OffsetManager offsetManager;

  /**
   * A repository mapping each Kafka Topic to it corresponding Consumption task responsible
   * for consuming messages and making changes to the local store accordingly.
   */
  private final Map<String, StoreConsumptionTask> topicNameToKafkaMessageConsumptionTaskMap;

  private final int nodeId;

  private ExecutorService consumerExecutorService;

  // Need to make sure that the service has started before start running KafkaConsumptionTask.
  private final AtomicBoolean isRunning;

  public KafkaConsumerPerStoreService(StoreRepository storeRepository, VeniceConfigLoader veniceConfigLoader, OffsetManager offsetManager) {
    super(VENICE_SERVICE_NAME);
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;

    this.topicNameToKafkaMessageConsumptionTaskMap = Collections.synchronizedMap(new HashMap<>());
    isRunning = new AtomicBoolean(false);

    this.veniceConfigLoader = veniceConfigLoader;

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();
    nodeId = serverConfig.getNodeId();

    VeniceNotifier notifier = null;
    // initialize internal kafka producer for acknowledging consumption (if enabled)
    if (serverConfig.isEnableConsumptionAcksForAzkabanJobs()) {
      Properties ackKafkaProps = getAcksKafkaProducerProperties(serverConfig);
      notifier = new KafkaNotifier(ACK_PARTITION_CONSUMPTION_KAFKA_TOPIC , ackKafkaProps , nodeId);
    } else {
      notifier = new LogNotifier();
    }
    notifiers.add(notifier);
  }

  /**
   * Initializes internal kafka producer for acknowledging kafka message consumption (if enabled)
   *
   * TODO: Get rid of this function
   */
  private static Properties getAcksKafkaProducerProperties(VeniceServerConfig veniceServerConfig) {
    Properties properties = new Properties();
    // TODO: Use constants from Kafka code (these are the old producer's config anyway...)
    properties.setProperty("metadata.broker.list", veniceServerConfig.getKafkaConsumptionAcksBrokerUrl());
    properties.setProperty("request.required.acks", "1");
    properties.setProperty("producer.type", "sync");
    properties.setProperty("partitioner.class", PartitionZeroPartitioner.class.getName());
    return properties;
  }

  /**
   * Starts the Kafka consumption tasks for already subscribed partitions.
   */
  @Override
  public void startInner() {
    logger.info("Enabling consumerExecutorService and kafka consumer tasks on node: " + nodeId);
    consumerExecutorService = Executors.newCachedThreadPool(new DaemonThreadFactory("venice-consumer"));
    topicNameToKafkaMessageConsumptionTaskMap.values().forEach(consumerExecutorService::submit);
    isRunning.set(true);
    logger.info("Kafka consumer tasks started.");
  }

  private StoreConsumptionTask getConsumerTask(VeniceStoreConfig veniceStore) {
    return new StoreConsumptionTask(new VeniceConsumerFactory(), getKafkaConsumerProperties(veniceStore), storeRepository,
            offsetManager , notifiers, nodeId, veniceStore.getStoreName());
  }

  /**
   * Stops all the Kafka consumption tasks.
   * Closes all the Kafka clients.
   */
  @Override
  public void stopInner() {
    logger.info("Shutting down Kafka consumer service for node: " + nodeId);
    isRunning.set(false);

    topicNameToKafkaMessageConsumptionTaskMap.values().forEach(StoreConsumptionTask::close);

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
    StoreConsumptionTask consumerTask = topicNameToKafkaMessageConsumptionTaskMap.get(topic);
    if(consumerTask == null || !consumerTask.isRunning()) {
      consumerTask = getConsumerTask(veniceStore);
      topicNameToKafkaMessageConsumptionTaskMap.put(topic, consumerTask);
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
    StoreConsumptionTask consumerTask = topicNameToKafkaMessageConsumptionTaskMap.get(topic);
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
    StoreConsumptionTask consumerTask = topicNameToKafkaMessageConsumptionTaskMap.get(topic);
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
  public void addNotifier(VeniceNotifier notifier) {
    notifiers.add(notifier);
  }

  /**
   * @return Group Id for kafka consumer.
   */
  private static String getGroupId(String topic, int nodeId) {
    return String.format(GROUP_ID_FORMAT, topic, Utils.getHostName(), nodeId);
  }

  /**
   * @return Properties Kafka properties corresponding to the venice store.
   */
  private static Properties getKafkaConsumerProperties(VeniceStoreConfig storeConfig) {
    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, storeConfig.getKafkaBootstrapServers());
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        String.valueOf(storeConfig.kafkaEnableAutoOffsetCommit()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        String.valueOf(storeConfig.getKafkaAutoCommitIntervalMs()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
        getGroupId(storeConfig.getStoreName(), storeConfig.getNodeId()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        KafkaKeySerializer.class.getName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaValueSerializer.class.getName());
    return kafkaConsumerProperties;
  }

}
