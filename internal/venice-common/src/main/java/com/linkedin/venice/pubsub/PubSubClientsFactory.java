package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper around pub-sub producer, consumer, and admin adapter factories
 *
 * This will be passed as one of the arguments to the component which depends on the pub-sub APIs.
 */
public class PubSubClientsFactory {
  private static final Logger LOGGER = LogManager.getLogger(PubSubClientsFactory.class);

  /**
   * The hard default for {@link com.linkedin.venice.ConfigKeys#PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED}
   * when it is not provided in the properties: {@code false}, i.e. fail fast. A missing factory-class
   * config then raises an exception instead of silently defaulting to Apache Kafka, which surfaces
   * misconfiguration early (important for non-Kafka deployments).
   * <p>
   * This default can be overridden by the same-named system property. Production never sets it, so it
   * stays fail-fast; the test JVM sets it to {@code true} (see the root {@code build.gradle} test
   * configuration) so the large body of existing tests keep resolving to the Apache Kafka adapters
   * without each having to configure the factory classes explicitly.
   */
  public static final boolean DEFAULT_KAFKA_FALLBACK_ENABLED = false;

  private enum FactoryType {
    PRODUCER, CONSUMER, ADMIN
  }

  private final PubSubProducerAdapterFactory producerAdapterFactory;
  private final PubSubConsumerAdapterFactory consumerAdapterFactory;
  private final PubSubAdminAdapterFactory adminAdapterFactory;

  public PubSubClientsFactory(
      PubSubProducerAdapterFactory producerAdapterFactory,
      PubSubConsumerAdapterFactory consumerAdapterFactory,
      PubSubAdminAdapterFactory adminAdapterFactory) {
    this.producerAdapterFactory = producerAdapterFactory;
    this.consumerAdapterFactory = consumerAdapterFactory;
    this.adminAdapterFactory = adminAdapterFactory;
  }

  public PubSubClientsFactory(VeniceProperties properties) {
    this(createProducerFactory(properties), createConsumerFactory(properties), createAdminFactory(properties));
  }

  public PubSubProducerAdapterFactory getProducerAdapterFactory() {
    return producerAdapterFactory;
  }

  public PubSubConsumerAdapterFactory getConsumerAdapterFactory() {
    return consumerAdapterFactory;
  }

  public PubSubAdminAdapterFactory getAdminAdapterFactory() {
    return adminAdapterFactory;
  }

  public static PubSubProducerAdapterFactory<PubSubProducerAdapter> createProducerFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS,
        PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS,
        ApacheKafkaProducerAdapterFactory.class.getName(),
        FactoryType.PRODUCER);
  }

  public static PubSubConsumerAdapterFactory<PubSubConsumerAdapter> createConsumerFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        ApacheKafkaConsumerAdapterFactory.class.getName(),
        FactoryType.CONSUMER);
  }

  public static PubSubAdminAdapterFactory<PubSubAdminAdapter> createAdminFactory(VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS,
        PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS,
        ApacheKafkaAdminAdapterFactory.class.getName(),
        FactoryType.ADMIN);
  }

  public static PubSubAdminAdapterFactory<PubSubAdminAdapter> createSourceOfTruthAdminFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS,
        PUB_SUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS,
        ApacheKafkaAdminAdapterFactory.class.getName(),
        FactoryType.ADMIN);
  }

  private static <T> T createFactory(
      VeniceProperties properties,
      String preferredConfigKey,
      String alternateConfigKey,
      String defaultClassName,
      FactoryType factoryType) {
    String className;
    if (properties.containsKey(preferredConfigKey) || properties.containsKey(alternateConfigKey)) {
      className = properties.getStringWithAlternative(preferredConfigKey, alternateConfigKey);
      LOGGER.debug("Creating pub-sub {} adapter factory instance for class: {}", factoryType, className);
    } else {
      boolean kafkaFallbackEnabled =
          properties.getBoolean(PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED, isKafkaFallbackEnabledByDefault());
      if (!kafkaFallbackEnabled) {
        throw new VeniceException(
            String.format(
                "PubSub %s adapter factory class is not configured. Set '%s' (or the legacy '%s') to the "
                    + "fully-qualified factory class name. Implicit fallback to the Apache Kafka adapter factory "
                    + "('%s') is disabled; set '%s=true' to re-enable it.",
                factoryType,
                preferredConfigKey,
                alternateConfigKey,
                defaultClassName,
                PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED));
      }
      className = defaultClassName;
      LOGGER.debug("Creating pub-sub {} adapter factory instance with default class: {}", factoryType, className);
    }

    return createInstance(className);
  }

  /**
   * Resolves the default for {@link com.linkedin.venice.ConfigKeys#PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED}
   * when the supplied properties do not set it. Returns {@link #DEFAULT_KAFKA_FALLBACK_ENABLED} (fail
   * fast) unless overridden by the same-named system property. Production leaves the system property
   * unset; the test JVM sets it to {@code true} so existing tests keep resolving to the Apache Kafka
   * adapters without configuring the factory classes explicitly.
   */
  private static boolean isKafkaFallbackEnabledByDefault() {
    return Boolean.parseBoolean(
        System.getProperty(
            PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED,
            Boolean.toString(DEFAULT_KAFKA_FALLBACK_ENABLED)));
  }

  public static <T> T createInstance(String className) {
    try {
      return (T) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new VeniceException("Failed to create instance of class: " + className, e);
    }
  }
}
