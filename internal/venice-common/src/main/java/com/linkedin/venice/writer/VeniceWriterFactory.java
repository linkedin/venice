package com.linkedin.venice.writer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_OVER_SSL;
import static com.linkedin.venice.ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterConcurrentDelegator;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterDelegator;
import com.linkedin.venice.stats.VeniceWriterStats;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Factory used to create {@link VeniceWriter}.
 */
public class VeniceWriterFactory {
  private static final Logger LOGGER = LogManager.getLogger(VeniceWriterFactory.class);
  private final VeniceProperties veniceProperties;
  private final PubSubProducerAdapterFactory producerAdapterFactory;
  private final MetricsRepository metricsRepository;
  private final String defaultBrokerAddress;
  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  public VeniceWriterFactory(Properties properties) {
    this(properties, null, null, null);
  }

  @Deprecated
  public VeniceWriterFactory(Properties properties, MetricsRepository metricsRepository) {
    this(properties, null, metricsRepository, null);
  }

  public VeniceWriterFactory(
      Properties properties,
      PubSubProducerAdapterFactory producerAdapterFactory,
      MetricsRepository metricsRepository,
      PubSubPositionTypeRegistry pubSubPositionTypeRegistry) {
    this.metricsRepository = metricsRepository;
    this.veniceProperties = new VeniceProperties(properties);
    this.defaultBrokerAddress = lookupBrokerAddress(veniceProperties);
    if (metricsRepository != null) {
      new VeniceWriterStats(metricsRepository);
    }
    // For now, if VeniceWriterFactory caller does not pass PubSubProducerAdapterFactory, use Kafka factory as default.
    // Eventually we'll force VeniceWriterFactory creators to inject PubSubProducerAdapterFactory.
    this.producerAdapterFactory = Objects.requireNonNull(
        producerAdapterFactory != null
            ? producerAdapterFactory
            : PubSubClientsFactory.createProducerFactory(veniceProperties),
        "PubSubProducerAdapterFactory cannot be null when creating VeniceWriterFactory");
    this.pubSubPositionTypeRegistry = Objects.requireNonNull(
        pubSubPositionTypeRegistry != null
            ? pubSubPositionTypeRegistry
            : PubSubPositionTypeRegistry.getRegistryFromPropertiesOrDefault(veniceProperties),
        "PubSubPositionTypeRegistry cannot be null when creating VeniceWriterFactory");
    LOGGER.info(
        "### MARKER ### Creating VeniceWriterFactory with properties: {} defaultBrokerAddress: {} pubSubPositionTypeRegistry: {} pubSubProducerAdapterFactory: {}",
        this.veniceProperties,
        this.defaultBrokerAddress,
        this.pubSubPositionTypeRegistry,
        this.producerAdapterFactory);
  }

  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(VeniceWriterOptions options) {
    PubSubProducerAdapter producerAdapter = buildPubSubProducerAdapter(options);
    return new VeniceWriter<>(options, veniceProperties, producerAdapter);
  }

  public <K, V, U> ComplexVeniceWriter<K, V, U> createComplexVeniceWriter(VeniceWriterOptions options) {
    PubSubProducerAdapter producerAdapter = buildPubSubProducerAdapter(options);
    return new ComplexVeniceWriter<>(options, veniceProperties, producerAdapter);
  }

  private PubSubProducerAdapter buildPubSubProducerAdapter(VeniceWriterOptions options) {
    VeniceProperties props = veniceProperties;
    String targetBrokerAddress = options.getBrokerAddress() != null ? options.getBrokerAddress() : defaultBrokerAddress;
    Objects.requireNonNull(
        targetBrokerAddress,
        "Broker address is required to create a VeniceWriter. Please provide it in the options.");
    PubSubProducerAdapterContext.Builder producerContext =
        new PubSubProducerAdapterContext.Builder().setVeniceProperties(props)
            .setProducerName(options.getTopicName())
            .setBrokerAddress(targetBrokerAddress)
            .setMetricsRepository(metricsRepository)
            .setPubSubMessageSerializer(options.getPubSubMessageSerializer())
            .setProducerCompressionEnabled(options.isProducerCompressionEnabled())
            .setPubSubPositionTypeRegistry(pubSubPositionTypeRegistry);

    Supplier<PubSubProducerAdapter> producerAdapterSupplier =
        () -> producerAdapterFactory.create(producerContext.build());
    int producerThreadCnt = options.getProducerThreadCount();
    if (producerThreadCnt > 1) {
      return new PubSubProducerAdapterConcurrentDelegator(
          options.getTopicName(),
          producerThreadCnt,
          options.getProducerQueueSize(),
          producerAdapterSupplier);
    }
    int producerCnt = options.getProducerCount();
    if (producerCnt > 1) {
      List<PubSubProducerAdapter> producers = new ArrayList<>(producerCnt);
      for (int i = 0; i < producerCnt; ++i) {
        producers.add(producerAdapterSupplier.get());
      }
      return new PubSubProducerAdapterDelegator(producers);
    }
    return producerAdapterSupplier.get();
  }

  @VisibleForTesting
  PubSubProducerAdapterFactory getProducerAdapterFactory() {
    return producerAdapterFactory;
  }

  /**
   * Retrieves the broker address from the provided Venice properties.
   *
   * This method centralizes the broker address lookup logic, ensuring that a single broker address
   * is passed to the producer context. By doing so, we eliminate the need for individual producer
   * adapters to handle name resolution. The long-term goal is to require all callers to explicitly
   * provide the broker address when configuring Venice writer options.
   *
   * The lookup follows these steps:
   * 1. If the `PUBSUB_BROKER_ADDRESS` property is set, it is returned immediately.
   * 2. If SSL to Kafka is enabled (determined by `SSL_TO_KAFKA_LEGACY` or `KAFKA_OVER_SSL`),
   *    the method verifies that `SSL_KAFKA_BOOTSTRAP_SERVERS` is set and returns it.
   * 3. Otherwise, the method ensures that `KAFKA_BOOTSTRAP_SERVERS` is defined and returns it.
   *
   * @param veniceProperties The properties containing broker configuration details.
   * @return The resolved broker address.
   * @throws VeniceException if required broker address properties are missing.
   */
  private String lookupBrokerAddress(VeniceProperties veniceProperties) {
    if (veniceProperties.containsKey(ConfigKeys.PUBSUB_BROKER_ADDRESS)) {
      return veniceProperties.getString(ConfigKeys.PUBSUB_BROKER_ADDRESS);
    }
    if (Boolean.parseBoolean(veniceProperties.getStringWithAlternative(SSL_TO_KAFKA_LEGACY, KAFKA_OVER_SSL, "false"))) {
      checkProperty(veniceProperties, SSL_KAFKA_BOOTSTRAP_SERVERS);
      return veniceProperties.getString(SSL_KAFKA_BOOTSTRAP_SERVERS);
    }
    checkProperty(veniceProperties, KAFKA_BOOTSTRAP_SERVERS);
    return veniceProperties.getString(KAFKA_BOOTSTRAP_SERVERS);
  }

  private static void checkProperty(VeniceProperties properties, String key) {
    if (!properties.containsKey(key)) {
      throw new VeniceException(
          "Invalid properties for Kafka producer factory. Required property: " + key + " is missing.");
    }
  }
}
