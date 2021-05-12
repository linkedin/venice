package com.linkedin.venice.kafka;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.admin.InstrumentedKafkaAdmin;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.admin.ScalaAdminUtils;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.utils.ReflectUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigConstants.*;


public abstract class KafkaClientFactory {
  private static final Logger LOGGER = Logger.getLogger(KafkaClientFactory.class);

  public KafkaConsumerWrapper getConsumer(Properties props) {
    return new ApacheKafkaConsumer(setupSSL(props));
  }

  public <K, V> KafkaConsumer<K, V> getKafkaConsumer(Properties properties){
    return new KafkaConsumer<>(setupSSL(properties));
  }

  public KafkaAdminWrapper getKafkaAdminClient(Optional<MetricsRepository> optionalMetricsRepository) {
    KafkaAdminWrapper adminWrapper = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(getKafkaAdminClass()),
        new Class[0],
        new Object[0]
    );
    Properties properties = setupSSL(new Properties());
    properties.setProperty(ConfigKeys.KAFKA_ZK_ADDRESS, getKafkaZkAddress());
    if (!properties.contains(ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFG_MAX_RETRY_TIME_SEC)) {
      properties.put(ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFG_MAX_RETRY_TIME_SEC, DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS);
    }
    adminWrapper.initialize(properties);
    if (optionalMetricsRepository.isPresent()) {
      // Use Kafka bootstrap server to identify which Kafka admin client stats it is
      String kafkaAdminStatsName = "KafkaAdminStats_" + getKafkaBootstrapServers();
      adminWrapper = new InstrumentedKafkaAdmin(adminWrapper, optionalMetricsRepository.get(), kafkaAdminStatsName);
      LOGGER.info("Created instrumented topic manager for Kafka cluster with bootstrap server " + getKafkaBootstrapServers());
    } else {
      LOGGER.info("Created non-instrumented topic manager for Kafka cluster with bootstrap server " + getKafkaBootstrapServers());
    }
    return adminWrapper;
  }

  /**
   * Setup essential ssl related configuration by putting all ssl properties of this factory into the given properties.
   */
  public abstract Properties setupSSL(Properties properties);

  abstract protected String getKafkaAdminClass();

  abstract protected String getKafkaZkAddress();

  abstract protected String getKafkaBootstrapServers();

  abstract protected KafkaClientFactory clone(String kafkaBootstrapServers, String kafkaZkAddress);
}
