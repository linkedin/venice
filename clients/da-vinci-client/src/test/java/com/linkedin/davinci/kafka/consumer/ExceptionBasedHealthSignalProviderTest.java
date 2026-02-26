package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubHealthCategory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ExceptionBasedHealthSignalProviderTest {
  private ExceptionBasedHealthSignalProvider provider;

  @BeforeMethod
  public void setUp() {
    provider = new ExceptionBasedHealthSignalProvider();
  }

  @Test
  public void testName() {
    assertEquals(provider.getName(), "exception");
  }

  @Test
  public void testInitiallyHealthy() {
    assertFalse(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.BROKER));
    assertFalse(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.METADATA_SERVICE));
  }

  @Test
  public void testSingleExceptionMarksUnhealthy() {
    provider.recordException("broker1:9092", PubSubHealthCategory.BROKER);
    assertTrue(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.BROKER));
    // Metadata service should still be healthy
    assertFalse(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.METADATA_SERVICE));
  }

  @Test
  public void testExceptionIsolatedPerBroker() {
    provider.recordException("broker1:9092", PubSubHealthCategory.BROKER);
    assertTrue(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.BROKER));
    assertFalse(provider.isUnhealthy("broker2:9092", PubSubHealthCategory.BROKER));
  }

  @Test
  public void testProbeSuccessClearsState() {
    provider.recordException("broker1:9092", PubSubHealthCategory.BROKER);
    assertTrue(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.BROKER));

    provider.onProbeSuccess("broker1:9092", PubSubHealthCategory.BROKER);
    assertFalse(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.BROKER));
  }

  @Test
  public void testProbeSuccessClearsOnlyCategoryAffected() {
    provider.recordException("broker1:9092", PubSubHealthCategory.BROKER);
    provider.recordException("broker1:9092", PubSubHealthCategory.METADATA_SERVICE);

    provider.onProbeSuccess("broker1:9092", PubSubHealthCategory.BROKER);
    assertFalse(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.BROKER));
    assertTrue(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.METADATA_SERVICE));
  }

  @Test
  public void testProbeSuccessOnUnknownAddressIsNoOp() {
    // Should not throw
    provider.onProbeSuccess("unknown:9092", PubSubHealthCategory.BROKER);
    assertFalse(provider.isUnhealthy("unknown:9092", PubSubHealthCategory.BROKER));
  }

  @Test
  public void testMultipleExceptionsForSameTarget() {
    provider.recordException("broker1:9092", PubSubHealthCategory.BROKER);
    provider.recordException("broker1:9092", PubSubHealthCategory.BROKER);
    assertTrue(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.BROKER));

    // Single probe success should clear it
    provider.onProbeSuccess("broker1:9092", PubSubHealthCategory.BROKER);
    assertFalse(provider.isUnhealthy("broker1:9092", PubSubHealthCategory.BROKER));
  }
}
