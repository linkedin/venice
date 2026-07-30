package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Properties;
import org.testng.annotations.Test;


public class PubSubClientsFactoryTest {
  @Test
  public void testCreateInstanceSuccess() {
    // with legacy config names
    Properties legacyProps = new Properties();
    legacyProps.put(PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS, TestPubSubProducerAdapterFactory.class.getName());
    legacyProps.put(PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS, TestPubSubConsumerAdapterFactory.class.getName());
    legacyProps.put(PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS, TestPubSubAdminAdapterFactory.class.getName());
    verifyFactoryClasses(
        legacyProps,
        TestPubSubProducerAdapterFactory.class,
        TestPubSubConsumerAdapterFactory.class,
        TestPubSubAdminAdapterFactory.class);

    // with new config names
    Properties newProps = new Properties();
    newProps.put(PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS, TestPubSubProducerAdapterFactory.class.getName());
    newProps.put(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, TestPubSubConsumerAdapterFactory.class.getName());
    newProps.put(PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS, TestPubSubAdminAdapterFactory.class.getName());
    verifyFactoryClasses(
        newProps,
        TestPubSubProducerAdapterFactory.class,
        TestPubSubConsumerAdapterFactory.class,
        TestPubSubAdminAdapterFactory.class);
  }

  /**
   * When the factory class is not present in the config, it is resolved from a JVM system property
   * (the mechanism by which the value is "provided at runtime" — see the root {@code build.gradle},
   * which sets these for the whole test suite). There is no implicit Apache Kafka default.
   */
  @Test
  public void testResolvesFactoryClassFromSystemProperty() {
    String key = PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
    String saved = System.getProperty(key);
    try {
      System.setProperty(key, TestPubSubProducerAdapterFactory.class.getName());
      PubSubProducerAdapterFactory factory =
          PubSubClientsFactory.createProducerFactory(new VeniceProperties(new Properties()));
      assertNotNull(factory);
      assertEquals(factory.getClass().getName(), TestPubSubProducerAdapterFactory.class.getName());
    } finally {
      restoreProperty(key, saved);
    }
  }

  /**
   * When the factory class is provided neither in the config nor as a system property, factory
   * creation fails fast instead of silently defaulting to the Apache Kafka adapter factories.
   */
  @Test
  public void testFailFastWhenFactoryClassNotProvided() {
    assertFailFast(
        () -> PubSubClientsFactory.createProducerFactory(new VeniceProperties(new Properties())),
        PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS,
        PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS);
    assertFailFast(
        () -> PubSubClientsFactory.createConsumerFactory(new VeniceProperties(new Properties())),
        PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS);
    assertFailFast(
        () -> PubSubClientsFactory.createAdminFactory(new VeniceProperties(new Properties())),
        PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS,
        PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS);
  }

  /**
   * An explicit factory-class config takes precedence over a system property.
   */
  @Test
  public void testExplicitConfigWinsOverSystemProperty() {
    String key = PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
    String saved = System.getProperty(key);
    try {
      System.setProperty(key, ApacheKafkaProducerAdapterFactory.class.getName());
      Properties props = new Properties();
      props.put(PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS, TestPubSubProducerAdapterFactory.class.getName());
      PubSubProducerAdapterFactory factory = PubSubClientsFactory.createProducerFactory(new VeniceProperties(props));
      assertEquals(factory.getClass().getName(), TestPubSubProducerAdapterFactory.class.getName());
    } finally {
      restoreProperty(key, saved);
    }
  }

  /**
   * Invokes {@code runnable} with the given factory-class system properties cleared, and asserts it
   * fails fast with a message naming the missing config key.
   */
  private static void assertFailFast(
      org.testng.Assert.ThrowingRunnable runnable,
      String configKey,
      String legacyConfigKey) {
    String saved = System.getProperty(configKey);
    String savedLegacy = System.getProperty(legacyConfigKey);
    try {
      System.clearProperty(configKey);
      System.clearProperty(legacyConfigKey);
      VeniceException e = expectThrows(VeniceException.class, runnable);
      assertTrue(
          e.getMessage().contains(configKey),
          "Expected fail-fast message to reference '" + configKey + "' but was: " + e.getMessage());
    } finally {
      restoreProperty(configKey, saved);
      restoreProperty(legacyConfigKey, savedLegacy);
    }
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private void verifyFactoryClasses(
      Properties props,
      Class<?> expectedProducer,
      Class<?> expectedConsumer,
      Class<?> expectedAdmin) {
    VeniceProperties veniceProps = new VeniceProperties(props);
    PubSubClientsFactory factory = new PubSubClientsFactory(veniceProps);

    PubSubProducerAdapterFactory producerFactory = factory.getProducerAdapterFactory();
    assertNotNull(producerFactory);
    assertEquals(producerFactory.getClass().getName(), expectedProducer.getName());

    PubSubConsumerAdapterFactory consumerFactory = factory.getConsumerAdapterFactory();
    assertNotNull(consumerFactory);
    assertEquals(consumerFactory.getClass().getName(), expectedConsumer.getName());

    PubSubAdminAdapterFactory adminFactory = factory.getAdminAdapterFactory();
    assertNotNull(adminFactory);
    assertEquals(adminFactory.getClass().getName(), expectedAdmin.getName());
  }

  @Test
  public void testCreateInstanceFailure() {
    String className = "com.example.bogus.NonExistentClass";
    Properties properties = new Properties();
    properties.put(PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS, className);
    Throwable t = expectThrows(
        VeniceException.class,
        () -> PubSubClientsFactory.createProducerFactory(new VeniceProperties(properties)));
    assertEquals(t.getMessage(), "Failed to create instance of class: " + className);
  }

  protected static class TestPubSubConsumerAdapterFactory extends PubSubConsumerAdapterFactory {
    @Override
    public PubSubConsumerAdapter create(PubSubConsumerAdapterContext context) {
      return null;
    }

    @Override
    public String getName() {
      return "TestPubSubConsumerAdapterFactory";
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  protected static class TestPubSubProducerAdapterFactory extends PubSubProducerAdapterFactory {
    @Override
    public PubSubProducerAdapter create(PubSubProducerAdapterContext context) {
      return null;
    }

    @Override
    public String getName() {
      return "TestPubSubProducerAdapterFactory";
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  protected static class TestPubSubAdminAdapterFactory extends PubSubAdminAdapterFactory<PubSubAdminAdapter> {
    @Override
    public PubSubAdminAdapter create(PubSubAdminAdapterContext context) {
      return null;
    }

    @Override
    public String getName() {
      return "TestPubSubAdminAdapterFactory";
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }
}
