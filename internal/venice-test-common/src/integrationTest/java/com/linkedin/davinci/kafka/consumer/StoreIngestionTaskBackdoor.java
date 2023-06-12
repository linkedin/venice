package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.integration.utils.VeniceServerWrapper;


public class StoreIngestionTaskBackdoor {
  /**
   * Test utility to manipulate the internal state of the {@link StoreIngestionTask}. Do not use in production code.
   */
  public static void setPurgeTransientRecordBuffer(VeniceServerWrapper serverWrapper, String topicName, boolean value) {
    serverWrapper.getVeniceServer()
        .getKafkaStoreIngestionService()
        .getStoreIngestionTask(topicName)
        .setPurgeTransientRecordBuffer(value);
  }
}
