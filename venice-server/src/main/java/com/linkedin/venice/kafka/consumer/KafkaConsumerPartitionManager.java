package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.server.VeniceConfig;
import com.linkedin.venice.storage.VeniceStorageNode;

import org.apache.log4j.Logger;

import java.util.List;


/**
 * Singleton class which acts as the running Kafka interface for Venice
 * Manages the creation of new Kafka partitions for storage nodes
 * Stores topic related information such as broker, port, etc.
 */
public class KafkaConsumerPartitionManager {

  static final Logger logger = Logger.getLogger(KafkaConsumerPartitionManager.class.getName());

  private String topic;
  private List<String> brokers;
  private int kafkaPort;
  private static KafkaConsumerPartitionManager manager = null;
  
  private static final String DEFAULT_TOPIC = "default_topic";

  private KafkaConsumerPartitionManager(String topic, List<String> brokers, int port) {
    this.topic = topic;
    this.brokers = brokers;
    this.kafkaPort = port;
  }

  /**
   * Returns an instance of the partition manager
   * */
  public static KafkaConsumerPartitionManager getInstance() throws KafkaConsumerException {
    if (null == manager) {
      throw new KafkaConsumerException("Kafka Manager has not yet been initialized");
    }
    return manager;
  }
  
  public static void initialize(VeniceConfig veniceConfig) {
    manager = new KafkaConsumerPartitionManager(DEFAULT_TOPIC, veniceConfig.getBrokerList(), veniceConfig.getKafkaBrokerPort());
  }


    /**
     * Initializes the Kafka Partition Manager with the provided variables
     *
     * @param topic - The name of the Kafka topic being consumed from
     * @param veniceConfig -  All configs for Venice Server
     */
  public static void initialize(String topic, VeniceConfig veniceConfig) {
    manager = new KafkaConsumerPartitionManager(topic, veniceConfig.getBrokerList(), veniceConfig.getKafkaBrokerPort());
  }

  /**
   * Given a partition id, returns a consumer task that is tied to that specific partition
   * @param node - An instance of a VeniceStorageNode, which the consumer task will write to
   * @param partition - The Kafka partitionId to which this consumer task is attached to
   * @return SimpleKafkaConsumerTask object
   * */
  public SimpleKafkaConsumerTask getConsumerTask(VeniceStorageNode node, int partition) {
	  SimpleKafkaConsumerConfig kafkaConfig = new SimpleKafkaConsumerConfig();
      kafkaConfig.setSeedBrokers(this.brokers);
    return new SimpleKafkaConsumerTask(kafkaConfig, node, topic, partition, kafkaPort);
  }

  /**
   * Safely shuts down all services on the partition manager
   * */
  public void shutdown() {

  }
}
