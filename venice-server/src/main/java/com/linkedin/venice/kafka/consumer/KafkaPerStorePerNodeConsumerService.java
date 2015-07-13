package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.partitioner.PartitionZeroPartitioner;
import com.linkedin.venice.serialization.Avro.AzkabanJobAvroAckRecordGenerator;
import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;


/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * Acts as the running Kafka interface to Venice. "Manages the consumption of Kafka partitions for each kafka topic
 * consumed by this node". Creates a Kafka Consumer for each Venice Store on this node.
 *
 * Uses the "new" Kafka Consumer.
 */
public class KafkaPerStorePerNodeConsumerService extends AbstractVeniceService {

  private static final String ACK_PARTITION_CONSUMPTION_KAFKA_TOPIC = "venice-partition-consumption-acknowledgement-1";
  private static final String VENICE_SERVICE_NAME = "kafka-consumer-service";

  private static final Logger logger = Logger.getLogger(KafkaPerStorePerNodeConsumerService.class.getName());

  private final boolean enableKafkaAutoCommit;

  private final StoreRepository storeRepository;
  private final VeniceConfigService veniceConfigService;
  private final PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;
  private final VeniceServerConfig veniceServerConfig;
  private KafkaProducer<byte[], byte[]> ackPartitionConsumptionProducer;
  private AzkabanJobAvroAckRecordGenerator ackRecordGenerator;

  /**
   * A repository of kafka topic to their corresponding partitions and the kafka consumer tasks. This may be used in
   * future for monitoring purposes. etc.
   * TODO: Make this a concurrent map if atomicity is needed in future
   */
  private final Map<String, KafkaPerStorePerNodeConsumerTask> topicNameToKafkaConsumerTaskMap;
  private final Map<String, Set<Integer>> topicNameToKafkaPartitionsMap;

  private ExecutorService consumerExecutorService;

  public KafkaPerStorePerNodeConsumerService(StoreRepository storeRepository, VeniceConfigService veniceConfigService,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepository) {
    super(VENICE_SERVICE_NAME);
    this.storeRepository = storeRepository;
    this.veniceConfigService = veniceConfigService;
    this.veniceServerConfig = veniceConfigService.getVeniceServerConfig();
    this.partitionNodeAssignmentRepository = partitionNodeAssignmentRepository;

    this.enableKafkaAutoCommit = veniceConfigService.getVeniceClusterConfig().isEnableKafkaConsumersOffsetManagement();

    this.topicNameToKafkaConsumerTaskMap = Collections.synchronizedMap(new HashMap<>());
    this.topicNameToKafkaPartitionsMap = Collections.synchronizedMap(new HashMap<>());

    // initialize internal kafka producer for acknowledging consumption (if enabled)
    if (veniceServerConfig.isEnableConsumptionAcksForAzkabanJobs()) {
      initializeAcksKafkaProducer();
    } else {
      ackPartitionConsumptionProducer = null;
    }
  }

  /**
   * Initializes internal kafka producer for acknowledging kafka message consumption (if enabled)
   */
  private void initializeAcksKafkaProducer() {
    Properties properties = new Properties();
    properties.setProperty("metadata.broker.list", veniceServerConfig.getKafkaConsumptionAcksBrokerUrl());
    properties.setProperty("request.required.acks", "1");
    properties.setProperty("producer.type", "sync");
    properties.setProperty("partitioner.class", PartitionZeroPartitioner.class.getName());

    ackPartitionConsumptionProducer = new KafkaProducer<byte[], byte[]>(properties);
    ackRecordGenerator = new AzkabanJobAvroAckRecordGenerator(ACK_PARTITION_CONSUMPTION_KAFKA_TOPIC);
  }

  @Override
  public void startInner() {
    logger.info("Starting all kafka consumer tasks on node: " + veniceServerConfig.getNodeId());
    consumerExecutorService = Executors.newCachedThreadPool();
    for (VeniceStoreConfig storeConfig : veniceConfigService.getAllStoreConfigs().values()) {
      registerKafkaConsumers(storeConfig);
    }
    logger.info("All kafka consumer tasks started.");
  }

  /**
   * Creates Kafka Consumers required by the Venice store according to the VeniceStoreConfig.
   *
   * @param storeConfig configs for the Venice store
   */
  public void registerKafkaConsumers(VeniceStoreConfig storeConfig) {
    /**
     * TODO: Make sure that admin service or any other service when registering Kafka consumers ensures that
     * there are no active consumers for the topic/partition.
     */
    String topic = storeConfig.getStoreName();
    Set<Integer> currentTopicPartitions = partitionNodeAssignmentRepository
        .getLogicalPartitionIds(topic, veniceServerConfig.getNodeId());
    topicNameToKafkaPartitionsMap.put(topic, currentTopicPartitions);
    KafkaPerStorePerNodeConsumerTask currentTopicConsumerTask = getConsumerTask(storeConfig, currentTopicPartitions);
    consumerExecutorService.submit(currentTopicConsumerTask);
    this.topicNameToKafkaConsumerTaskMap.put(topic, currentTopicConsumerTask);
  }

  private KafkaPerStorePerNodeConsumerTask getConsumerTask(VeniceStoreConfig storeConfig,
      Set<Integer> currentTopicPartitions) {
    return new KafkaPerStorePerNodeConsumerTask(storeConfig, storeRepository.getLocalStorageEngine(storeConfig.getStoreName()),
        currentTopicPartitions, ackPartitionConsumptionProducer, ackRecordGenerator, enableKafkaAutoCommit);
  }

  @Override
  public void stopInner() {
    logger.info("Shutting down Kafka consumer service for node: " + veniceServerConfig.getNodeId());
    if (consumerExecutorService != null) {
      consumerExecutorService.shutdown();
    }
    logger.info("Shut down complete");
  }

  /**
   * Unsubscribe Kafka Consumption for the specified topic-partition.
   * @param storeConfig Venice Store Config for which the partition that needs to be unsubscribed.
   * @param partition Partition Id that needs to be unsubscribed.
   * @throws VeniceException If the topic or the topic-partition was not subscribed.
   */
  public void unsubscribeKafkaPartition (VeniceStoreConfig storeConfig, int partition) throws VeniceException {
    String topic = storeConfig.getStoreName();
    if (!topicNameToKafkaConsumerTaskMap.containsKey(topic)) {
      throw new VeniceException("Kafka Topic: " + topic + " not subscribed.");
    }
    Set<Integer> partitionSet = topicNameToKafkaPartitionsMap.get(topic);

    if (!partitionSet.contains(partition)) {
      throw new VeniceException("Kafka Partition: " + topic + "-" + partition + " not subscribed.");
    }

    KafkaPerStorePerNodeConsumerTask consumerTask = topicNameToKafkaConsumerTaskMap.get(topic);
    consumerTask.unsubscribePartition(topic, partition);
    partitionSet.remove(partition);
    logger.info("Kafka Partition: " + topic + "-" + partition + " unsubscribed.");
  }

  /**
   * Subscribe and start the Kafka Consumer Task for the specified topic-partition.
   * @param storeConfig storeConfig Venice Store Config for which the partition that needs to be subscribed.
   * @param partition Partition Id that needs to be subscribed.
   * @throws VeniceException If the topic-partition was already subscribed.
   */
  public void subscribeKafkaPartition (VeniceStoreConfig storeConfig, int partition) throws VeniceException {
    String topic = storeConfig.getStoreName();
    if (!topicNameToKafkaConsumerTaskMap.containsKey(topic)) {
      Set<Integer> partitionSet = new HashSet();
      partitionSet.add(partition);
      KafkaPerStorePerNodeConsumerTask consumerTask = getConsumerTask(storeConfig, partitionSet);
      this.topicNameToKafkaPartitionsMap.put(topic, partitionSet);
      this.topicNameToKafkaConsumerTaskMap.put(topic, consumerTask);
      consumerExecutorService.submit(consumerTask);
    } else {
      if(this.topicNameToKafkaPartitionsMap.get(topic).contains(partition)) {
        throw new VeniceException("Kafka Partition: " + topic + "-" + partition + " already subscribed.");
      }
      this.topicNameToKafkaConsumerTaskMap.get(topic).subscribePartition(topic, partition);
    }
    logger.info("Kafka Partition: " + topic + "-" + partition + " subscribed.");
  }

}
