package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.VeniceKafkaConsumerFactory;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static com.linkedin.venice.hadoop.VenicePushJob.*;


public class KafkaInputUtils {

  public static KafkaClientFactory getConsumerFactory(JobConf config) {
    Properties sslProperties = null;
    if (config.get(SSL_CONFIGURATOR_CLASS_CONFIG) != null) {
      SSLConfigurator configurator = SSLConfigurator.getSSLConfigurator(config.get(SSL_CONFIGURATOR_CLASS_CONFIG));
      try {
        sslProperties = configurator.setupSSLConfig(HadoopUtils.getProps(config), UserCredentialsFactory.getHadoopUserCredentials());
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential", e);
      }
    }
    Properties consumerFactoryProperties = new Properties(sslProperties);
    consumerFactoryProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.get(KAFKA_INPUT_BROKER_URL));
    /**
     * This is used to bypass the check in {@link VeniceKafkaConsumerFactory#getKafkaZkAddress}.
     */
    consumerFactoryProperties.setProperty(ConfigKeys.KAFKA_ZK_ADDRESS, "fake_zk_address");
    return new VeniceKafkaConsumerFactory(new VeniceProperties(consumerFactoryProperties));
  }

  public static Properties getConsumerProperties() {
    Properties clonedProperties = new Properties();
    clonedProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    clonedProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OptimizedKafkaValueSerializer.class);
    return clonedProperties;
  }

}
