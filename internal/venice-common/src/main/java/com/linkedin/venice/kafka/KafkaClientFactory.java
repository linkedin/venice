package com.linkedin.venice.kafka;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS;
import static com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer.VENICE_SCHEMA_READER_CONFIG;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.admin.InstrumentedKafkaAdmin;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A factory that creates Kafka clients, specifically Kafka consumer and Kafka admin client.
 */
public abstract class KafkaClientFactory {
  private static final Logger LOGGER = LogManager.getLogger(KafkaClientFactory.class);

  protected final Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader;

  private final Optional<MetricsParameters> metricsParameters;

  protected KafkaClientFactory() {
    this(Optional.empty(), Optional.empty());
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

  public PubSubConsumer getConsumer(Properties props, KafkaPubSubMessageDeserializer kafkaPubSubMessageDeserializer) {
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    Properties propertiesWithSSL = setupSSL(props);
    return new ApacheKafkaConsumer(
        getKafkaConsumer(propertiesWithSSL),
        new VeniceProperties(props),
        isKafkaConsumerOffsetCollectionEnabled(),
        kafkaPubSubMessageDeserializer);
  }

  public Consumer<byte[], byte[]> getRawBytesKafkaConsumer() {
    Properties props = getConsumerProps();
    // This is a temporary fix for the issue described here
    // https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
    // In our case "org.apache.kafka.common.serialization.ByteArrayDeserializer" class can not be found
    // because class loader has no venice-common in class path. This can be only reproduced on JDK11
    // Trying to avoid class loading via Kafka's ConfigDef class
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);

    return getKafkaConsumer(props);
  }

  @Deprecated
  public Consumer<KafkaKey, KafkaMessageEnvelope> getRecordKafkaConsumer() {
    Properties props = getConsumerProps();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OptimizedKafkaValueSerializer.class);
    if (kafkaMessageEnvelopeSchemaReader.isPresent()) {
      props.put(VENICE_SCHEMA_READER_CONFIG, kafkaMessageEnvelopeSchemaReader.get());
    }

    return getKafkaConsumer(props);
  }

  private <K, V> Consumer<K, V> getKafkaConsumer(Properties properties) {
    Properties propertiesWithSSL = setupSSL(properties);
    return new KafkaConsumer<>(propertiesWithSSL);
  }

  public KafkaAdminWrapper getWriteOnlyKafkaAdmin(Optional<MetricsRepository> optionalMetricsRepository) {
    return createAdminClient(getWriteOnlyAdminClass(), optionalMetricsRepository, "WriteOnlyKafkaAdminStats");
  }

  public KafkaAdminWrapper getReadOnlyKafkaAdmin(Optional<MetricsRepository> optionalMetricsRepository) {
    return createAdminClient(getReadOnlyAdminClass(), optionalMetricsRepository, "ReadOnlyKafkaAdminStats");
  }

  public KafkaAdminWrapper getKafkaAdminClient(Optional<MetricsRepository> optionalMetricsRepository) {
    return createAdminClient(getKafkaAdminClass(), optionalMetricsRepository, "KafkaAdminStats");
  }

  private KafkaAdminWrapper createAdminClient(
      String kafkaAdminClientClass,
      Optional<MetricsRepository> optionalMetricsRepository,
      String statsNamePrefix) {
    KafkaAdminWrapper adminWrapper =
        ReflectUtils.callConstructor(ReflectUtils.loadClass(kafkaAdminClientClass), new Class[0], new Object[0]);
    Properties properties = setupSSL(new Properties());
    if (!properties.contains(ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC)) {
      properties.put(
          ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC,
          DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS);
    }
    adminWrapper.initialize(properties);
    final String kafkaBootstrapServers = getKafkaBootstrapServers();

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

  /**
   * N.B. These props are only used by {@link #getRecordKafkaConsumer()} and {@link #getRawBytesKafkaConsumer()}
   *      which in turn are used by {@link com.linkedin.venice.kafka.partitionoffset.PartitionOffsetFetcherImpl}
   *      and {@link TopicManager}, and therefore only for metadata operations. The data path is using
   *      {@link #getConsumer(Properties)} and is thus more flexible in terms of configurability.
   */
  private Properties getConsumerProps() {
    Properties props = new Properties();
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return props;
  }

  protected boolean isKafkaConsumerOffsetCollectionEnabled() {
    return false;
  }

  /**
   * Setup essential ssl related configuration by putting all ssl properties of this factory into the given properties.
   */
  public abstract Properties setupSSL(Properties properties);

  abstract protected String getKafkaAdminClass();

  /**
   * Get the class name of an admin client that is used for "write-only" tasks such as create topics, update topic configs,
   * etc. "Write-only" means that it only modifies the Kafka cluster state and does not read it.
   *
   * @return Fully-qualified name name. For example: "com.linkedin.venice.kafka.admin.KafkaAdminClient"
   */
  abstract protected String getWriteOnlyAdminClass();

  /**
   * Get the class name of an admin client that is used for "read-only" tasks such as check topic existence, get topic configs,
   * etc. "Read-only" means that it only reads/get the topic metadata from a Kafka cluster and does not modify any of it.
   *
   * @return Fully-qualified name name. For example: "com.linkedin.venice.kafka.admin.KafkaAdminClient"
   */
  abstract protected String getReadOnlyAdminClass();

  public abstract String getKafkaBootstrapServers();

  abstract protected KafkaClientFactory clone(
      String kafkaBootstrapServers,
      Optional<MetricsParameters> metricsParameters);

  public static class MetricsParameters {
    final String uniqueName;
    final MetricsRepository metricsRepository;

    public MetricsParameters(String uniqueMetricNamePrefix, MetricsRepository metricsRepository) {
      this.uniqueName = uniqueMetricNamePrefix;
      this.metricsRepository = metricsRepository;
    }

    public MetricsParameters(
        Class kafkaFactoryClass,
        Class usingClass,
        String kafkaBootstrapUrl,
        MetricsRepository metricsRepository) {
      this(
          kafkaFactoryClass.getSimpleName() + "_used_by_" + usingClass + "_for_" + kafkaBootstrapUrl,
          metricsRepository);
    }
  }
}
