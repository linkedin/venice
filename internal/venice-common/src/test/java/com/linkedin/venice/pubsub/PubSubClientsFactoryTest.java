package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
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
   * By default (no factory-class config and no explicit fallback flag) the factory should fail fast
   * instead of silently defaulting to the Apache Kafka adapter factories.
   */
  @Test
  public void testFailFastWhenFactoryClassMissingAndFallbackDisabled() {
    VeniceProperties emptyProps = new VeniceProperties(new Properties());

    assertFailFast(() -> PubSubClientsFactory.createProducerFactory(emptyProps), PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS);
    assertFailFast(() -> PubSubClientsFactory.createConsumerFactory(emptyProps), PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS);
    assertFailFast(() -> PubSubClientsFactory.createAdminFactory(emptyProps), PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS);
    assertFailFast(
        () -> PubSubClientsFactory.createSourceOfTruthAdminFactory(emptyProps),
        PUBSUB_SOURCE_OF_TRUTH_ADMIN_ADAPTER_FACTORY_CLASS);
    // The instance constructor eagerly builds all three factories, so it should fail fast as well.
    expectThrows(VeniceException.class, () -> new PubSubClientsFactory(emptyProps));

    // Explicitly disabling the fallback behaves the same as the default.
    Properties fallbackDisabled = new Properties();
    fallbackDisabled.put(PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED, "false");
    expectThrows(VeniceException.class, () -> new PubSubClientsFactory(new VeniceProperties(fallbackDisabled)));
  }

  /**
   * When the Kafka fallback is explicitly enabled, missing factory-class configs should resolve to the
   * Apache Kafka adapter factories (the legacy behavior).
   */
  @Test
  public void testKafkaFallbackWhenExplicitlyEnabled() {
    Properties fallbackEnabled = new Properties();
    fallbackEnabled.put(PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED, "true");
    verifyFactoryClasses(
        fallbackEnabled,
        ApacheKafkaProducerAdapterFactory.class,
        ApacheKafkaConsumerAdapterFactory.class,
        ApacheKafkaAdminAdapterFactory.class);
  }

  private static void assertFailFast(org.testng.Assert.ThrowingRunnable runnable, String expectedConfigKeyInMessage) {
    VeniceException e = expectThrows(VeniceException.class, runnable);
    assertTrue(
        e.getMessage().contains(expectedConfigKeyInMessage),
        "Expected fail-fast message to reference '" + expectedConfigKeyInMessage + "' but was: " + e.getMessage());
    assertTrue(
        e.getMessage().contains(PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED),
        "Expected fail-fast message to reference the fallback config key but was: " + e.getMessage());
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
