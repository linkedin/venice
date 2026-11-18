package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import java.util.function.Function;
import org.testng.annotations.Test;


public class PubSubConsumerAdapterContextTest {
  @Test
  public void testPubSubConsumerAdapterContextBuilderWithAllFields() {
    VeniceProperties props = new VeniceProperties(new Properties());
    PubSubPositionTypeRegistry registry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;
    Function<String, String> keyLookup = storeName -> "urn:test:key:1";
    PubSubConsumerAdapterContext context = new PubSubConsumerAdapterContext.Builder().setConsumerName("test-consumer")
        .setPubSubBrokerAddress("localhost:1234")
        .setVeniceProperties(props)
        .setPubSubSecurityProtocol(PubSubSecurityProtocol.SSL)
        .setMetricsRepository(new MetricsRepository())
        .setPubSubTopicRepository(new PubSubTopicRepository())
        .setIsOffsetCollectionEnabled(true)
        .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
        .setPubSubPositionTypeRegistry(registry)
        .setPubSubEncryptionKeyUrnLookup(keyLookup)
        .setThreadPriority(Thread.MAX_PRIORITY)
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
    assertSame(context.getPubSubEncryptionKeyUrnLookup(), keyLookup);
    assertEquals(context.getThreadPriority(), Thread.MAX_PRIORITY);
    assertTrue(context.hasThreadPriority());
  }

  @Test
  public void testThreadPriorityDefaultsToUnspecified() {
    PubSubConsumerAdapterContext.Builder builder =
        new PubSubConsumerAdapterContext.Builder().setPubSubBrokerAddress("localhost:1234")
            .setVeniceProperties(VeniceProperties.empty())
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);
    PubSubConsumerAdapterContext context = builder.build();
    assertEquals(context.getThreadPriority(), Thread.NORM_PRIORITY);
    assertFalse(context.hasThreadPriority());

    context = builder.setThreadPriority(Thread.NORM_PRIORITY).build();
    assertEquals(context.getThreadPriority(), Thread.NORM_PRIORITY);
    assertTrue(context.hasThreadPriority());
  }

  @Test
  public void testEncryptionKeyLookupIsOptional() {
    PubSubConsumerAdapterContext context =
        new PubSubConsumerAdapterContext.Builder().setPubSubBrokerAddress("localhost:1234")
            .setVeniceProperties(VeniceProperties.empty())
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY)
            .build();
    assertNull(context.getPubSubEncryptionKeyUrnLookup());
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
