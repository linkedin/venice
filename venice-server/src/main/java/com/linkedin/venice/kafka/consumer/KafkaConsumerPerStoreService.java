package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.partitioner.PartitionZeroPartitioner;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KafkaValueSerializer;
import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  private static final String ACK_PARTITION_CONSUMPTION_KAFKA_TOPIC = "venice-partition-consumption-acknowledgement-1";
  private static final String VENICE_SERVICE_NAME = "kafka-consumer-service";
  private static final String GROUP_ID_FORMAT = "%s_%s_%d";

  private static final Logger logger = Logger.getLogger(KafkaConsumerPerStoreService.class.getName());

  private final StoreRepository storeRepository;
  private final VeniceConfigService veniceConfigService;

  private final VeniceNotifier notifier;

  /**
   * A repository mapping each Kafka Topic to it corresponding Consumption task responsible
   * for consuming messages and making changes to the local store accordingly.
   */
  private final Map<String, StoreConsumptionTask> topicNameToKafkaMessageConsumptionTaskMap;

  private final int nodeId;

  private ExecutorService consumerExecutorService;

  // Need to make sure that the service has started before start running KafkaConsumptionTask.
  private final AtomicBoolean canRunTasks;

  public KafkaConsumerPerStoreService(StoreRepository storeRepository, VeniceConfigService veniceConfigService) {
    super(VENICE_SERVICE_NAME);
    this.storeRepository = storeRepository;

    this.topicNameToKafkaMessageConsumptionTaskMap = Collections.synchronizedMap(new HashMap<>());
    canRunTasks = new AtomicBoolean(false);

    this.veniceConfigService = veniceConfigService;

    VeniceServerConfig serverConfig = veniceConfigService.getVeniceServerConfig();
    nodeId = serverConfig.getNodeId();

    // initialize internal kafka producer for acknowledging consumption (if enabled)
    if (serverConfig.isEnableConsumptionAcksForAzkabanJobs()) {
      Properties ackKafkaProps = getAcksKafkaProducerProperties(serverConfig);
      notifier = new KafkaNotifier(ACK_PARTITION_CONSUMPTION_KAFKA_TOPIC , ackKafkaProps , nodeId);
    } else {
      notifier = null;
    }
  }

  /**
   * Initializes internal kafka producer for acknowledging kafka message consumption (if enabled)
   */
  private static Properties getAcksKafkaProducerProperties(VeniceServerConfig veniceServerConfig) {
    Properties properties = new Properties();
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
    consumerExecutorService = Executors.newCachedThreadPool();
    topicNameToKafkaMessageConsumptionTaskMap.values().forEach(consumerExecutorService::submit);
    canRunTasks.set(true);
    logger.info("Kafka consumer tasks started.");
  }

  /**
   * Function to start kafka message consumption for all stores according to the PartitionNodeAssignment.
   * Ideally, should only be used when NOT using Helix.
   * @param partitionNodeAssignmentRepository
   */
  public void consumeForPartitionNodeAssignmentRepository(PartitionNodeAssignmentRepository partitionNodeAssignmentRepository) {
    for (VeniceStoreConfig storeConfig : veniceConfigService.getAllStoreConfigs().values()) {
      String topic = storeConfig.getStoreName();
      Set<Integer> currentTopicPartitions = partitionNodeAssignmentRepository.getLogicalPartitionIds(topic, nodeId);
      for(Integer partitionId : currentTopicPartitions) {
        startConsumption(storeConfig, partitionId);
      }
    }
  }

  private StoreConsumptionTask getConsumerTask(VeniceStoreConfig veniceStore) {
    return new StoreConsumptionTask(getKafkaConsumerProperties(veniceStore), storeRepository,
            notifier, nodeId, veniceStore.getStoreName());
  }

  /**
   * Stops all the Kafka consumption tasks.
   * Closes all the Kafka clients.
   */
  @Override
  public void stopInner() {
    logger.info("Shutting down Kafka consumer service for node: " + nodeId);
    canRunTasks.set(false);
    if (consumerExecutorService != null) {
      consumerExecutorService.shutdown();
    }
    topicNameToKafkaMessageConsumptionTaskMap.values().forEach(StoreConsumptionTask::stop);
    if(notifier != null) {
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
  public void startConsumption(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreConsumptionTask consumerTask = topicNameToKafkaMessageConsumptionTaskMap.get(topic);
    if(consumerTask == null || !consumerTask.isRunning()) {
      consumerTask = getConsumerTask(veniceStore);
      topicNameToKafkaMessageConsumptionTaskMap.put(topic, consumerTask);
      if(canRunTasks.get()) {
        consumerExecutorService.submit(consumerTask);
      }
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
  public void stopConsumption(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreConsumptionTask consumerTask = topicNameToKafkaMessageConsumptionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.unsubscribePartition(topic, partitionId);
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
    }
    logger.info("Offset reset to beginning - Kafka Partition: " + topic + "-" + partitionId + ".");
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
