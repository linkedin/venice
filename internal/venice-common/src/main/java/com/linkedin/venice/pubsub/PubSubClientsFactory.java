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
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper around pub-sub producer, consumer, and admin adapter factories
 *
 * This will be passed as one of the arguments to the component which depends on the pub-sub APIs.
 */
public class PubSubClientsFactory {
  private static final Logger LOGGER = LogManager.getLogger(PubSubClientsFactory.class);

  private final PubSubProducerAdapterFactory<? extends PubSubProducerAdapter> producerAdapterFactory;
  private final PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> consumerAdapterFactory;
  private final PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> adminAdapterFactory;

  public PubSubClientsFactory(
      PubSubProducerAdapterFactory<? extends PubSubProducerAdapter> producerAdapterFactory,
      PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> consumerAdapterFactory,
      PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> adminAdapterFactory) {
    this.producerAdapterFactory = producerAdapterFactory;
    this.consumerAdapterFactory = consumerAdapterFactory;
    this.adminAdapterFactory = adminAdapterFactory;
  }

  public PubSubClientsFactory(VeniceProperties properties) {
    this(createProducerFactory(properties), createConsumerFactory(properties), createAdminFactory(properties));
  }

  public PubSubProducerAdapterFactory<? extends PubSubProducerAdapter> getProducerAdapterFactory() {
    return producerAdapterFactory;
  }

  public PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> getConsumerAdapterFactory() {
    return consumerAdapterFactory;
  }

  public PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> getAdminAdapterFactory() {
    return adminAdapterFactory;
  }

  public static PubSubProducerAdapterFactory<? extends PubSubProducerAdapter> createProducerFactory(
      Properties properties) {
    return createProducerFactory(new VeniceProperties(properties));
  }

  public static PubSubProducerAdapterFactory<? extends PubSubProducerAdapter> createProducerFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS,
        PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS,
        ApacheKafkaProducerAdapterFactory.class.getName(),
        PubSubClientType.PRODUCER);
  }

  public static PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> createConsumerFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        ApacheKafkaConsumerAdapterFactory.class.getName(),
        PubSubClientType.CONSUMER);
  }

  public static PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> createAdminFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS,
        PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS,
        ApacheKafkaAdminAdapterFactory.class.getName(),
        PubSubClientType.ADMIN);
  }

  public static PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> createSourceOfTruthAdminFactory(
      VeniceProperties veniceProperties) {
    return createFactory(
        veniceProperties,
        PUBSUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS,
        PUB_SUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS,
        ApacheKafkaAdminAdapterFactory.class.getName(),
        PubSubClientType.ADMIN);
  }

  private static <T> T createFactory(
      VeniceProperties properties,
      String preferredConfigKey,
      String alternateConfigKey,
      String defaultClassName,
      PubSubClientType pubSubClientType) {
    String className;
    if (properties.containsKey(preferredConfigKey) || properties.containsKey(alternateConfigKey)) {
      className = properties.getStringWithAlternative(preferredConfigKey, alternateConfigKey);
      LOGGER.debug("Creating pub-sub {} adapter factory instance for class: {}", pubSubClientType, className);
    } else {
      className = defaultClassName;
      LOGGER.debug("Creating pub-sub {} adapter factory instance with default class: {}", pubSubClientType, className);
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
