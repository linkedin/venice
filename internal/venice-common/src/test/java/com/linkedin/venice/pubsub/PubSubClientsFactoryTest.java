package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
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
    // default: no config provided
    verifyFactoryClasses(
        new Properties(),
        ApacheKafkaProducerAdapterFactory.class,
        ApacheKafkaConsumerAdapterFactory.class,
        ApacheKafkaAdminAdapterFactory.class);

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
