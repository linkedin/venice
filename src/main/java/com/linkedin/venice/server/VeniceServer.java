package com.linkedin.venice.server;

import com.linkedin.venice.Venice;
import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.HighKafkaConsumer;
import com.linkedin.venice.kafka.consumer.SimpleKafkaConsumer;
import com.linkedin.venice.storage.VeniceStorageManager;
import org.apache.log4j.Logger;

/**
 * Primary Venice Server class. In the future, this will become the main class for the Server component.
 */
public class VeniceServer {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceServer.class.getName());

  private VeniceStorageManager storeManager;

  // TODO: Remove this code if high level consumer is to be deprecated
  private static final boolean USE_HIGH_CONSUMER = false;

  public VeniceServer() {

    initializeStorage();
    initializeKakfaConsumer();

  }

  private void initializeKakfaConsumer() {

    try {

      // optional use of high level consumer or simple consumer
      if (VeniceServer.USE_HIGH_CONSUMER) {

        HighKafkaConsumer highConsumer = new HighKafkaConsumer(GlobalConfiguration.getZookeeperURL(),
            "sample_group", Venice.DEFAULT_TOPIC);
        highConsumer.run(1);

      } else {

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(Venice.DEFAULT_TOPIC);

        // start one consumer for each partition
        for (int i = 0; i < GlobalConfiguration.getNumKafkaPartitions(); i++) {
          consumer.run(GlobalConfiguration.getNumThreadsPerPartition(), i,
              GlobalConfiguration.getBrokerList(), GlobalConfiguration.getKafkaBrokerPort());
        }

      }

    } catch (Exception e) {
      logger.error("Consumer failure: " + e);
      e.printStackTrace();
    }

  }

  private void initializeStorage() {

    // initialize the storage engine, start n nodes and p partitions.
    storeManager = VeniceStorageManager.getInstance();

    // start nodes
    for (int n = 0; n < GlobalConfiguration.getNumStorageNodes(); n++) {
      storeManager.registerNewNode(n);
    }

    // start partitions
    for (int p = 0; p < GlobalConfiguration.getNumKafkaPartitions(); p++) {
      storeManager.registerNewPartition(p);
    }

  }

  public Object readValue(String key) {
    return storeManager.readValue(key);
  }

}
