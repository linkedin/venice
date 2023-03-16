package com.linkedin.venice.kafka;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.admin.InstrumentedKafkaAdmin;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import com.linkedin.venice.pubsub.factory.MetricsParameters;
import com.linkedin.venice.pubsub.factory.PubSubClientFactory;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A factory that creates Kafka clients, specifically Kafka consumer and Kafka admin client.
 */
public class KafkaClientFactory implements PubSubClientFactory {
  private static final Logger LOGGER = LogManager.getLogger(KafkaClientFactory.class);

  private final Optional<MetricsParameters> metricsParameters;

  protected VeniceProperties veniceProperties; // TODO: change it after we remove KafkaConsumerFactoryImpl
  protected final Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader;

  public KafkaClientFactory(VeniceProperties veniceProperties) {
    this(Optional.empty(), Optional.empty());
    this.veniceProperties = veniceProperties;
  }

  protected KafkaClientFactory(
      @Nonnull Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader,
      Optional<MetricsParameters> metricsParameters) {
    Validate.notNull(kafkaMessageEnvelopeSchemaReader);
    this.kafkaMessageEnvelopeSchemaReader = kafkaMessageEnvelopeSchemaReader;
    this.metricsParameters = metricsParameters;
  }

  public Optional<MetricsParameters> getMetricsParameters() {
    return metricsParameters;
  }

  @Override
  public PubSubConsumer getConsumer(Properties props, PubSubMessageDeserializer pubSubMessageDeserializer) {
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    if (pubSubMessageDeserializer instanceof KafkaPubSubMessageDeserializer) {
      Properties propertiesWithSSL = setupSSL(props);
      return new ApacheKafkaConsumer(
          getKafkaConsumer(propertiesWithSSL),
          new VeniceProperties(props),
          isKafkaConsumerOffsetCollectionEnabled(),
          (KafkaPubSubMessageDeserializer) pubSubMessageDeserializer);
    } else {
      throw new VeniceException("Only support " + KafkaPubSubMessageDeserializer.class);
    }
  }

  private <K, V> Consumer<K, V> getKafkaConsumer(Properties properties) {
    Properties propertiesWithSSL = setupSSL(properties);
    return new KafkaConsumer<>(propertiesWithSSL);
  }

  @Override
  public KafkaAdminWrapper getWriteOnlyPubSubAdmin(
      Optional<MetricsRepository> optionalMetricsRepository,
      PubSubTopicRepository pubSubTopicRepository) {
    return createAdminClient(
        getWriteOnlyAdminClass(),
        optionalMetricsRepository,
        "WriteOnlyKafkaAdminStats",
        pubSubTopicRepository);
  }

  @Override
  public KafkaAdminWrapper getReadOnlyPubSubAdmin(
      Optional<MetricsRepository> optionalMetricsRepository,
      PubSubTopicRepository pubSubTopicRepository) {
    return createAdminClient(
        getReadOnlyAdminClass(),
        optionalMetricsRepository,
        "ReadOnlyKafkaAdminStats",
        pubSubTopicRepository);
  }

  @Override
  public KafkaAdminWrapper getPubSubAdmin(
      Optional<MetricsRepository> optionalMetricsRepository,
      PubSubTopicRepository pubSubTopicRepository) {
    return createAdminClient(getKafkaAdminClass(), optionalMetricsRepository, "KafkaAdminStats", pubSubTopicRepository);
  }

  private KafkaAdminWrapper createAdminClient(
      String kafkaAdminClientClass,
      Optional<MetricsRepository> optionalMetricsRepository,
      String statsNamePrefix,
      PubSubTopicRepository pubSubTopicRepository) {
    KafkaAdminWrapper adminWrapper =
        ReflectUtils.callConstructor(ReflectUtils.loadClass(kafkaAdminClientClass), new Class[0], new Object[0]);
    Properties properties = setupSSL(new Properties());
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    properties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    if (!properties.contains(ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC)) {
      properties.put(
          ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC,
          DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS);
    }
    adminWrapper.initialize(properties, pubSubTopicRepository);
    final String kafkaBootstrapServers = getPubSubBootstrapServers();

    if (optionalMetricsRepository.isPresent()) {
      // Use Kafka bootstrap server to identify which Kafka admin client stats it is
      final String kafkaAdminStatsName =
          String.format("%s_%s_%s", statsNamePrefix, kafkaAdminClientClass, kafkaBootstrapServers);
      adminWrapper = new InstrumentedKafkaAdmin(adminWrapper, optionalMetricsRepository.get(), kafkaAdminStatsName);
      LOGGER.info(
          "Created instrumented Kafka admin client of class {} for Kafka cluster with bootstrap "
              + "server {} and has stats name prefix {}",
          kafkaAdminClientClass,
          kafkaBootstrapServers,
          statsNamePrefix);
    } else {
      LOGGER.info(
          "Created non-instrumented Kafka admin client of class {} for Kafka cluster with bootstrap server {}",
          kafkaAdminClientClass,
          kafkaBootstrapServers);
    }
    return adminWrapper;
  }

  protected boolean isKafkaConsumerOffsetCollectionEnabled() {
    return false;
  }

  /**
   * Setup essential ssl related configuration by putting all ssl properties of this factory into the given properties.
   */
  @Override
  public Properties setupSSL(Properties properties) {
    properties.putAll(veniceProperties.toProperties());
    try {
      SSLConfig sslConfig = new SSLConfig(veniceProperties);
      properties.putAll(sslConfig.getKafkaSSLConfig());
      properties.setProperty(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          veniceProperties.getString(ConfigKeys.KAFKA_SECURITY_PROTOCOL));
    } catch (UndefinedPropertyException e) {
      LOGGER.warn("SSL properties are missing, Kafka consumer will not be able to consume if SSL is required.");
    }
    return properties;
  }

  protected String getKafkaAdminClass() {
    return KafkaAdminClient.class.getName();
  }

  /**
   * Get the class name of an admin client that is used for "write-only" tasks such as create topics, update topic configs,
   * etc. "Write-only" means that it only modifies the Kafka cluster state and does not read it.
   *
   * @return Fully-qualified name name. For example: "com.linkedin.venice.kafka.admin.KafkaAdminClient"
   */
  protected String getWriteOnlyAdminClass() {
    return getKafkaAdminClass();
  }

  /**
   * Get the class name of an admin client that is used for "read-only" tasks such as check topic existence, get topic configs,
   * etc. "Read-only" means that it only reads/get the topic metadata from a Kafka cluster and does not modify any of it.
   *
   * @return Fully-qualified name name. For example: "com.linkedin.venice.kafka.admin.KafkaAdminClient"
   */

  protected String getReadOnlyAdminClass() {
    return getKafkaAdminClass();
  }

  @Override
  public String getPubSubBootstrapServers() {
    return veniceProperties.getString(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
  }

  @Override
  public KafkaClientFactory clone(String kafkaBootstrapServers, Optional<MetricsParameters> metricsParameters) {
    Properties clonedProperties = this.veniceProperties.toProperties();
    clonedProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    return new KafkaClientFactory(new VeniceProperties(clonedProperties));
  }
}
