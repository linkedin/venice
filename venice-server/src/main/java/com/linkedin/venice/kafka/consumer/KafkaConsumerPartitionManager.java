package com.linkedin.venice.kafka.consumer;

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

  private KafkaConsumerPartitionManager(String topic, List<String> brokers, int port) {

    this.topic = topic;
    this.brokers = brokers;
    this.kafkaPort = port;

  }

  /**
   * Returns an instance of the partition manager
   * */
  public static KafkaConsumerPartitionManager getInstance() throws VeniceKafkaConsumerException {

    if (null == manager) {
      throw new VeniceKafkaConsumerException("Kafka Manager has not yet been initialized");
    }

    return manager;

  }

  /**
   * Initializes the Kafka Partition Manager with the provided variables
   * @param topic - The name of the Kafka topic being consumed from
   * @param brokers - A list of hosts that the Kafka topic lives on
   * @param port - The port number on the host to read from
   * */
  public static void initialize(String topic, List<String> brokers, int port) {
    manager = new KafkaConsumerPartitionManager(topic, brokers, port);
  }

  /**
   * Given a partition id, returns a consumer task that is tied to that specific partition
   * */
  public SimpleKafkaConsumerTask getConsumerTask(VeniceStorageNode node, int partition) {
    return new SimpleKafkaConsumerTask(node, topic, partition, brokers, kafkaPort);
  }

  /**
   * Safely shuts down all services on the partition manager
   * */
  public void shutdown() {

  }

}
