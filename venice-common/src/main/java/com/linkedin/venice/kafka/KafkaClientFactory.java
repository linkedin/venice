package com.linkedin.venice.kafka;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.admin.InstrumentedKafkaAdmin;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.ReflectUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigConstants.*;


public abstract class KafkaClientFactory {
  private static final Logger LOGGER = Logger.getLogger(KafkaClientFactory.class);

  public KafkaConsumerWrapper getConsumer(Properties props) {
    return new ApacheKafkaConsumer(setupSSL(props), false);
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
      String kafkaAdminStatsName = "KafkaAdminStats_" + TehutiUtils.fixMalformedMetricName(getKafkaBootstrapServers());
      adminWrapper = new InstrumentedKafkaAdmin(adminWrapper, optionalMetricsRepository.get(), kafkaAdminStatsName);
      LOGGER.info("Created instrumented topic manager for Kafka cluster with bootstrap server " + getKafkaBootstrapServers());
    } else {
      LOGGER.info("Created non-instrumented topic manager for Kafka cluster with bootstrap server " + getKafkaBootstrapServers());
    }
    return adminWrapper;
  }

  public static Properties getKafkaRawBytesConsumerProps() {
    Properties props = new Properties();
    //This is a temporary fix for the issue described here
    //https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
    //In our case "org.apache.kafka.common.serialization.ByteArrayDeserializer" class can not be found
    //because class loader has no venice-common in class path. This can be only reproduced on JDK11
    //Trying to avoid class loading via Kafka's ConfigDef class
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return props;
  }

  public static Properties getKafkaRecordConsumerProps() {
    Properties props = new Properties();
    //This is a temporary fix for the issue described here
    //https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
    //In our case "org.apache.kafka.common.serialization.ByteArrayDeserializer" class can not be found
    //because class loader has no venice-common in class path. This can be only reproduced on JDK11
    //Trying to avoid class loading via Kafka's ConfigDef class
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OptimizedKafkaValueSerializer.class);
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return props;
  }

  /**
   * Setup essential ssl related configuration by putting all ssl properties of this factory into the given properties.
   */
  public abstract Properties setupSSL(Properties properties);

  abstract protected String getKafkaAdminClass();

  abstract protected String getKafkaZkAddress();

  public abstract String getKafkaBootstrapServers();

  abstract protected KafkaClientFactory clone(String kafkaBootstrapServers, String kafkaZkAddress);
}
