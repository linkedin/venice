package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;


public class VeniceServerConsumerJavaBasedFactory extends VeniceServerConsumerFactory {
  public VeniceServerConsumerJavaBasedFactory(VeniceServerConfig serverConfig) {
    super(serverConfig);
  }

  @Override
  protected String getKafkaAdminClass() {
    return KafkaAdminClient.class.getName();
  }
}
