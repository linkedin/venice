package com.linkedin.venice.pubsub.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.Test;


public class PubSubProducerAdapterContextTest {
  @Test
  public void testValidContextBuild() {
    PubSubProducerAdapterContext context = new PubSubProducerAdapterContext.Builder().setProducerName("test-producer")
        .setBrokerAddress("localhost:9092")
        .setVeniceProperties(VeniceProperties.empty())
        .setSecurityProtocol(PubSubSecurityProtocol.SSL)
        .setMetricsRepository(new MetricsRepository())
        .setPubSubTopicRepository(new PubSubTopicRepository())
        .setShouldValidateProducerConfigStrictly(false)
        .setPubSubMessageSerializer(PubSubMessageSerializer.DEFAULT_PUBSUB_SERIALIZER)
        .setProducerCompressionEnabled(false)
        .setCompressionType("snappy")
        .build();

    assertTrue(context.getProducerName().contains("test-producer"));
    assertEquals(context.getBrokerAddress(), "localhost:9092");
    assertEquals(context.getCompressionType(), "none");
    assertFalse(context.isProducerCompressionEnabled());
  }

  @Test
  public void testMissingBrokerAddressThrowsException() {
    Exception e = expectThrows(
        VeniceException.class,
        () -> new PubSubProducerAdapterContext.Builder().setProducerName("test-producer").build());
    assertEquals(e.getMessage(), "Broker address must be provided to create a pub-sub producer");
  }

  @Test
  public void testDefaultValuesAreSet() {
    PubSubProducerAdapterContext context =
        new PubSubProducerAdapterContext.Builder().setBrokerAddress("localhost:9092").build();

    assertNotNull(context.getVeniceProperties());
    assertNotNull(context.getPubSubMessageSerializer());
    assertEquals(context.getCompressionType(), "gzip");
    assertTrue(context.isProducerCompressionEnabled());
    assertNull(context.getMetricsRepository());
    assertNull(context.getPubSubTopicRepository());
    assertTrue(context.shouldValidateProducerConfigStrictly());
    assertNull(context.getSecurityProtocol());
    assertNotNull(context.getProducerName());
  }
}
