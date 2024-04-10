package com.linkedin.venice.pubsub;

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
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.annotations.Test;


public class PubSubClientsFactoryTest {
  @Test
  public void testCreateInstanceSuccess() {
    // default
    VeniceProperties veniceProps = new VeniceProperties(new Properties());
    PubSubClientsFactory factory = new PubSubClientsFactory(veniceProps);

    PubSubProducerAdapterFactory producerFactory = factory.getProducerAdapterFactory();
    assertNotNull(producerFactory);
    assertEquals(producerFactory.getClass().getName(), ApacheKafkaProducerAdapterFactory.class.getName());

    PubSubConsumerAdapterFactory consumerFactory = factory.getConsumerAdapterFactory();
    assertNotNull(consumerFactory);
    assertEquals(consumerFactory.getClass().getName(), ApacheKafkaConsumerAdapterFactory.class.getName());

    PubSubAdminAdapterFactory adminFactory = factory.getAdminAdapterFactory();
    assertNotNull(adminFactory);
    assertEquals(adminFactory.getClass().getName(), ApacheKafkaAdminAdapterFactory.class.getName());

    // with factory class name
    Properties properties = new Properties();
    properties.put(PUB_SUB_PRODUCER_ADAPTER_FACTORY_CLASS, ApacheKafkaProducerAdapterFactory.class.getName());
    properties.put(PUB_SUB_CONSUMER_ADAPTER_FACTORY_CLASS, ApacheKafkaConsumerAdapterFactory.class.getName());
    properties.put(PUB_SUB_ADMIN_ADAPTER_FACTORY_CLASS, ApacheKafkaAdminAdapterFactory.class.getName());
    veniceProps = new VeniceProperties(properties);
    factory = new PubSubClientsFactory(veniceProps);

    producerFactory = factory.getProducerAdapterFactory();
    assertNotNull(producerFactory);
    assertEquals(producerFactory.getClass().getName(), ApacheKafkaProducerAdapterFactory.class.getName());

    consumerFactory = factory.getConsumerAdapterFactory();
    assertNotNull(consumerFactory);
    assertEquals(consumerFactory.getClass().getName(), ApacheKafkaConsumerAdapterFactory.class.getName());

    adminFactory = factory.getAdminAdapterFactory();
    assertNotNull(adminFactory);
    assertEquals(adminFactory.getClass().getName(), ApacheKafkaAdminAdapterFactory.class.getName());
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
}
