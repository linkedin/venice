package com.linkedin.venice.kafka.consumer;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An implementation of the Kafka Simple Consumer API.
 * Consumes messages from Kafka partitions.
 */
public class SimpleKafkaConsumer {

  static final Logger logger = Logger.getLogger(SimpleKafkaConsumer.class.getName());

  public static final int MAX_READS = 100000000;

  private ExecutorService executor;
  private String topic;

  public SimpleKafkaConsumer(String topic) {

    this.topic = topic;

  }

  /**
   * Class which creates threads for concurrent Kafka consumption.
   * @param numThreads - Number of threads to use in Kafka consumption per partition
   * @param partition - Id of the Kafka Partition to consume from
   * @param brokers - A list of possible Kafka brokers
   * @param port - The port number to listen to
   * */
  public void run(int numThreads, int partition, List<String> brokers, int port) {

    // launch all the threads
    executor = Executors.newFixedThreadPool(numThreads);

    // create an object to consume the messages
    for (int i = 0; i < numThreads; i++) {
      executor.submit(new SimpleKafkaConsumerTask(MAX_READS, topic, partition, brokers, port, i));
    }

  }

  /**
   * Cleans up the Simple Consumer Service.
   * */
  public void shutdown() {

    logger.error("Shutting down consumer service...");

    if (executor != null) {
      executor.shutdown();
    }

  }


}
