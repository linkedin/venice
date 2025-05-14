package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import org.testng.annotations.Test;


public class PubSubConsumerAdapterContextTest {
  @Test
  public void testPubSubConsumerAdapterContextBuilderWithAllFields() {
    VeniceProperties props = new VeniceProperties(new Properties());
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;
    PubSubConsumerAdapterContext context = new PubSubConsumerAdapterContext.Builder().setConsumerName("test-consumer")
        .setPubSubBrokerAddress("localhost:1234")
        .setVeniceProperties(props)
        .setPubSubSecurityProtocol(PubSubSecurityProtocol.SSL)
        .setMetricsRepository(new MetricsRepository())
        .setPubSubTopicRepository(new PubSubTopicRepository())
        .setIsOffsetCollectionEnabled(true)
        .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
        .setPubSubPositionTypeRegistry(registry)
        .build();

    assertNotNull(context.getConsumerName());
    assertEquals(context.getPubSubBrokerAddress(), "localhost:1234");
    assertEquals(context.getVeniceProperties(), props);
    assertEquals(context.getPubSubSecurityProtocol(), PubSubSecurityProtocol.SSL);
    assertNotNull(context.getMetricsRepository());
    assertNotNull(context.getPubSubTopicRepository());
    assertTrue(context.isOffsetCollectionEnabled());
    assertNotNull(context.getPubSubMessageDeserializer());
    assertEquals(context.getPubSubPositionTypeRegistry(), registry);
  }

  @Test
  public void testMissingVenicePropertiesThrowsException() {
    Exception exception = expectThrows(IllegalArgumentException.class, () -> {
      new PubSubConsumerAdapterContext.Builder()
          .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY)
          .build();
    });
    assertTrue(exception.getMessage().contains("Venice properties cannot be null."));
  }

  @Test
  public void testMissingRegistryThrowsException() {
    VeniceProperties props = new VeniceProperties(new Properties());
    Exception exception = expectThrows(IllegalArgumentException.class, () -> {
      new PubSubConsumerAdapterContext.Builder().setVeniceProperties(props).build();
    });
    assertTrue(exception.getMessage().contains("PubSubPositionTypeRegistry cannot be null"));
  }
}
