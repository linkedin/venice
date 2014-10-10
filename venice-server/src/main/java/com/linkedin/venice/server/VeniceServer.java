package com.linkedin.venice.server;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPartitionManager;
import com.linkedin.venice.kafka.consumer.VeniceKafkaConsumerException;
import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.storage.VeniceStorageManager;
import org.apache.log4j.Logger;

/**
 * Primary Venice Server Main class.
 */
public class VeniceServer {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceServer.class.getName());

  private static final String DEFAULT_TOPIC = "default_topic";
  private static VeniceStorageManager storeManager;

  public static void main(String args[]) {

    try {
      GlobalConfiguration.initializeFromFile(args[0]);
    } catch (Exception e) {
      logger.error(e.getMessage());
      System.exit(1);
    }

    initializeKakfaConsumer();
    initializeStorage();

  }

  private static void initializeKakfaConsumer() {

    // Start the service which provides partition connections to Kafka
    KafkaConsumerPartitionManager.initialize(DEFAULT_TOPIC,
        GlobalConfiguration.getBrokerList(), GlobalConfiguration.getKafkaBrokerPort());

  }

  private static void initializeStorage() {

    // initialize the storage engine, start n nodes and p partitions.
    storeManager = new VeniceStorageManager();

    try {

      // start nodes
      for (int n = 0; n < GlobalConfiguration.getNumStorageNodes(); n++) {
        storeManager.registerNewNode(n);
      }

      // start partitions
      for (int p = 0; p < GlobalConfiguration.getNumKafkaPartitions(); p++) {
        storeManager.registerNewPartition(p);
      }

    } catch (VeniceStorageException e) {

      logger.error("Could not properly initialize the storage instance.");
      e.printStackTrace();
      shutdown();

    } catch (VeniceKafkaConsumerException e) {

      logger.error("Could not properly initialize Venice Kafka instance.");
      e.printStackTrace();
      shutdown();

    }

  }

  public Object readValue(String key) {

    Object toReturn = null;

    try {

      toReturn = storeManager.readValue(key);

    } catch (VeniceStorageException e) {

      // TODO: Implement a way to recover from missing nodes or partitions here
      logger.error("Could not execute command on Venice Store.");
      e.printStackTrace();

    }

    return toReturn;

  }

  /**
   * Method which closes VeniceServer, shuts down its resources, and exits the JVM.
   * */
  public static void shutdown() {

    System.exit(1);

  }

}
