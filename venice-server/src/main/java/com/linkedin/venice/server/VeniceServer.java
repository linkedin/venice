package com.linkedin.venice.server;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.SimpleKafkaConsumer;
import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.storage.VeniceStorageManager;
import kafka.consumer.SimpleConsumer;
import org.apache.log4j.Logger;

/**
 * Primary Venice Server class. In the future, this will become the main class for the Server component.
 */
public class VeniceServer {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceServer.class.getName());

  private static final String DEFAULT_TOPIC = "default_topic";
  private static VeniceStorageManager storeManager;
  private static SimpleKafkaConsumer consumer;

  public static void main(String args[]) {

    GlobalConfiguration.initialize("");

    initializeStorage();
    initializeKakfaConsumer();

  }

  private static void initializeKakfaConsumer() {

    consumer = new SimpleKafkaConsumer(DEFAULT_TOPIC);

    // start one consumer for each partition
    for (int i = 0; i < GlobalConfiguration.getNumKafkaPartitions(); i++) {
      consumer.run(GlobalConfiguration.getNumThreadsPerPartition(), i,
          GlobalConfiguration.getBrokerList(), GlobalConfiguration.getKafkaBrokerPort());
    }

  }

  private static void initializeStorage() {

    // initialize the storage engine, start n nodes and p partitions.
    storeManager = VeniceStorageManager.getInstance();

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

    if (consumer != null) {
      consumer.shutdown();
    }

    System.exit(1);

  }

}
