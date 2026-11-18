package com.linkedin.venice.pubsub.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.function.Function;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PubSubProducerAdapterContextTest {
  @Test
  public void testValidContextBuild() {
    Function<String, String> keyLookup = storeName -> "urn:test:key:1";
    PubSubProducerAdapterContext context = new PubSubProducerAdapterContext.Builder().setProducerName("test-producer")
        .setBrokerAddress("localhost:9092")
        .setVeniceProperties(VeniceProperties.empty())
        .setSecurityProtocol(PubSubSecurityProtocol.SSL)
        .setMetricsRepository(new MetricsRepository())
        .setPubSubTopicRepository(new PubSubTopicRepository())
        .setShouldValidateProducerConfigStrictly(false)
        .setPubSubMessageSerializer(PubSubMessageSerializer.DEFAULT_PUBSUB_SERIALIZER)
        .setProducerCompressionEnabled(false)
        .setProducerEncryptionEnabled(true)
        .setCompressionType("snappy")
        .setPubSubEncryptionKeyUrnLookup(keyLookup)
        .setThreadPriority(Thread.MAX_PRIORITY)
        .build();

    assertTrue(context.getProducerName().contains("test-producer"));
    assertEquals(context.getBrokerAddress(), "localhost:9092");
    assertEquals(context.getCompressionType(), "none");
    assertFalse(context.isProducerCompressionEnabled());
    assertTrue(context.isProducerEncryptionEnabled());
    assertSame(context.getPubSubEncryptionKeyUrnLookup(), keyLookup);
    assertTrue(context.hasThreadPriority());
    assertEquals(context.getThreadPriority(), Thread.MAX_PRIORITY);
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
    assertFalse(context.isProducerEncryptionEnabled());
    assertNull(context.getMetricsRepository());
    assertNull(context.getPubSubTopicRepository());
    assertNull(context.getPubSubEncryptionKeyUrnLookup());
    assertTrue(context.shouldValidateProducerConfigStrictly());
    assertNotNull(context.getPubSubSecurityProtocol());
    assertNotNull(context.getProducerName());
    assertFalse(context.hasThreadPriority());
    assertEquals(context.getThreadPriority(), Thread.NORM_PRIORITY);
  }

  @DataProvider
  public Object[][] threadPriorities() {
    return new Object[][] { { Integer.MIN_VALUE, Thread.MIN_PRIORITY }, { -1, Thread.MIN_PRIORITY },
        { 0, Thread.MIN_PRIORITY }, { Thread.MIN_PRIORITY, Thread.MIN_PRIORITY },
        { Thread.NORM_PRIORITY, Thread.NORM_PRIORITY }, { Thread.MAX_PRIORITY, Thread.MAX_PRIORITY },
        { Thread.MAX_PRIORITY + 1, Thread.MAX_PRIORITY }, { Integer.MAX_VALUE, Thread.MAX_PRIORITY } };
  }

  @Test(dataProvider = "threadPriorities")
  public void testExplicitThreadPriorityIsClamped(int priority, int expectedPriority) {
    PubSubProducerAdapterContext context = new PubSubProducerAdapterContext.Builder().setBrokerAddress("localhost:9092")
        .setThreadPriority(priority)
        .build();
    assertTrue(context.hasThreadPriority());
    assertEquals(context.getThreadPriority(), expectedPriority);
  }
}
