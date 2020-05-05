package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;


public class VeniceAdminToolConsumerFactory extends KafkaClientFactory {
  private final VeniceProperties veniceProperties;

  public VeniceAdminToolConsumerFactory(VeniceProperties veniceProperties) {
    this.veniceProperties = veniceProperties;
  }

  @Override
  public Properties setupSSL(Properties properties) {
    properties.putAll(veniceProperties.toProperties());
    try {
      SSLConfig sslConfig = new SSLConfig(veniceProperties);
      properties.putAll(sslConfig.getKafkaSSLConfig());
      properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, veniceProperties.getString(ConfigKeys.KAFKA_SECURITY_PROTOCOL));
    } catch (UndefinedPropertyException e) {
      // No SSL for you.
    }
    return properties;
  }

  @Override
  protected String getKafkaAdminClass() {
    throw new VeniceException("getKafkaAdminClass not supported in " + this.getClass().getSimpleName());
  }

  @Override
  protected String getKafkaZkAddress() {
    throw new VeniceException("getKafkaZkAddress not supported in " + this.getClass().getSimpleName());
  }
}
