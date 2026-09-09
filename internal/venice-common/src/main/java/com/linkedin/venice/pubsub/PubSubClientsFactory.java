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
   * By default the adapter factories do NOT fall back to Apache Kafka when their factory-class config
   * is missing; callers must configure the factory classes explicitly so that misconfiguration fails
   * fast. Set {@link com.linkedin.venice.ConfigKeys#PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED} to
   * {@code true} to restore the legacy implicit-Kafka behavior.
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
    String className = properties.getStringWithAlternative(preferredConfigKey, alternateConfigKey, null);
    if (className == null || className.trim().isEmpty()) {
      // A missing or blank value is treated as "not configured": either fall back to the Apache Kafka
      // adapter factory (when explicitly enabled) or fail fast with a message naming the missing key.
      boolean kafkaFallbackEnabled =
          properties.getBoolean(PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED, DEFAULT_KAFKA_FALLBACK_ENABLED);
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
    } else {
      LOGGER.debug("Creating pub-sub {} adapter factory instance for class: {}", factoryType, className);
    }

    return createInstance(className);
  }

  public static <T> T createInstance(String className) {
    try {
      return (T) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new VeniceException("Failed to create instance of class: " + className, e);
    }
  }
}
