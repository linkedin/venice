package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS;

import com.linkedin.venice.exceptions.VeniceException;
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
        FactoryType.PRODUCER);
  }

  public static PubSubConsumerAdapterFactory<PubSubConsumerAdapter> createConsumerFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        FactoryType.CONSUMER);
  }

  public static PubSubAdminAdapterFactory<PubSubAdminAdapter> createAdminFactory(VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS,
        PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS,
        FactoryType.ADMIN);
  }

  public static PubSubAdminAdapterFactory<PubSubAdminAdapter> createSourceOfTruthAdminFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS,
        PUB_SUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS,
        FactoryType.ADMIN);
  }

  private static <T> T createFactory(
      VeniceProperties properties,
      String preferredConfigKey,
      String alternateConfigKey,
      FactoryType factoryType) {
    String className;
    if (properties.containsKey(preferredConfigKey) || properties.containsKey(alternateConfigKey)) {
      className = properties.getStringWithAlternative(preferredConfigKey, alternateConfigKey);
      LOGGER.debug("Creating pub-sub {} adapter factory instance for class: {}", factoryType, className);
    } else {
      // No implicit fallback to a default (e.g. Apache Kafka) adapter factory. The factory class must be
      // provided at runtime, either in the properties above or as a JVM system property. If neither
      // provides it, fail fast so the misconfiguration surfaces immediately.
      className = System.getProperty(preferredConfigKey, System.getProperty(alternateConfigKey));
      if (className == null) {
        throw new VeniceException(
            String.format(
                "PubSub %s adapter factory class is not configured. Provide '%s' (or the legacy '%s') in the "
                    + "properties or as a JVM system property.",
                factoryType,
                preferredConfigKey,
                alternateConfigKey));
      }
      LOGGER.debug(
          "Creating pub-sub {} adapter factory instance for class supplied via system property: {}",
          factoryType,
          className);
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
