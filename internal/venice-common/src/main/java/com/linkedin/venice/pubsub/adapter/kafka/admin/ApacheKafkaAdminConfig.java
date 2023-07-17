package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerConfig;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheKafkaAdminConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaAdminConfig.class);

  private final Properties adminProperties;

  public ApacheKafkaAdminConfig(VeniceProperties veniceProperties) {
    Properties properties = veniceProperties.toProperties();

    if (veniceProperties.containsKey(ConfigKeys.PUB_SUB_COMPONENTS_USAGE)) {
      String pubSubComponentsUsage = veniceProperties.getString(ConfigKeys.PUB_SUB_COMPONENTS_USAGE);
      if (pubSubComponentsUsage.equals(PubSubClientsFactory.PUB_SUB_CLIENT_USAGE_FOR_CONTROLLER)) {
        properties = ApacheKafkaConsumerConfig.preparePubSubSSLPropertiesForController(veniceProperties);
      } else if (pubSubComponentsUsage.equals(PubSubClientsFactory.PUB_SUB_CLIENT_USAGE_FOR_SERVER)) {
        properties = ApacheKafkaConsumerConfig.preparePubSubSSLPropertiesForServer(veniceProperties);
      }
    }

    VeniceProperties postVeniceProperties = new VeniceProperties(properties);
    this.adminProperties =
        postVeniceProperties.clipAndFilterNamespace(ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX).toProperties();
    // Setup ssl config if needed.
    if (KafkaSSLUtils.validateAndCopyKafkaSSLConfig(postVeniceProperties, this.adminProperties)) {
      LOGGER.info("Will initialize an SSL Kafka admin client");
    } else {
      LOGGER.info("Will initialize a non-SSL Kafka admin client");
    }
    adminProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    if (!adminProperties.contains(ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC)) {
      adminProperties.put(
          ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC,
          DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS);
    }
  }

  public Properties getAdminProperties() {
    return adminProperties;
  }
}
