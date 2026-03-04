package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link DolStamp} class that tracks Declaration of Leadership (DoL)
 * state during STANDBY to LEADER transition.
 */
public class DolStampTest {
  private static final long TEST_LEADERSHIP_TERM = 42L;
  private static final String TEST_HOST_ID = "test-host-123";

  @Test
  public void testConstructorAndGetters() {
    long beforeCreation = System.currentTimeMillis();
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);
    long afterCreation = System.currentTimeMillis();

    assertEquals(dolStamp.getLeadershipTerm(), TEST_LEADERSHIP_TERM);
    assertEquals(dolStamp.getHostId(), TEST_HOST_ID);
    assertFalse(dolStamp.isDolProduced());
    assertFalse(dolStamp.isDolConsumed());
    assertNull(dolStamp.getDolProduceFuture());
    assertFalse(dolStamp.isDolComplete());

    // Verify produceStartTimeMs is set correctly
    long produceStartTimeMs = dolStamp.getProduceStartTimeMs();
    assertTrue(
        produceStartTimeMs >= beforeCreation && produceStartTimeMs <= afterCreation,
        "produceStartTimeMs should be between creation time bounds");
  }

  @Test
  public void testSetDolProduced() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    assertFalse(dolStamp.isDolProduced());

    dolStamp.setDolProduced(true);
    assertTrue(dolStamp.isDolProduced());

    dolStamp.setDolProduced(false);
    assertFalse(dolStamp.isDolProduced());
  }

  @Test
  public void testSetDolConsumed() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    assertFalse(dolStamp.isDolConsumed());

    dolStamp.setDolConsumed(true);
    assertTrue(dolStamp.isDolConsumed());

    dolStamp.setDolConsumed(false);
    assertFalse(dolStamp.isDolConsumed());
  }

  @Test
  public void testSetDolProduceFuture() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    assertNull(dolStamp.getDolProduceFuture());

    CompletableFuture<PubSubProduceResult> future = new CompletableFuture<>();
    dolStamp.setDolProduceFuture(future);

    assertEquals(dolStamp.getDolProduceFuture(), future);

    // Test setting to null
    dolStamp.setDolProduceFuture(null);
    assertNull(dolStamp.getDolProduceFuture());
  }

  @Test
  public void testIsDolComplete() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    // Neither produced nor consumed
    assertFalse(dolStamp.isDolComplete());

    // Only produced
    dolStamp.setDolProduced(true);
    assertFalse(dolStamp.isDolComplete());

    // Only consumed (reset produced)
    dolStamp.setDolProduced(false);
    dolStamp.setDolConsumed(true);
    assertFalse(dolStamp.isDolComplete());

    // Both produced and consumed
    dolStamp.setDolProduced(true);
    assertTrue(dolStamp.isDolComplete());
  }

  @Test
  public void testGetLatencyMs() throws InterruptedException {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    // Latency should be small immediately after creation
    long initialLatency = dolStamp.getLatencyMs();
    assertTrue(initialLatency >= 0, "Latency should be non-negative");
    assertTrue(initialLatency < 1000, "Initial latency should be less than 1 second");

    // Sleep for a short while and verify latency increases
    Thread.sleep(50);
    long laterLatency = dolStamp.getLatencyMs();
    assertTrue(laterLatency >= initialLatency, "Latency should increase over time");
  }

  @Test
  public void testToStringBasic() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    String result = dolStamp.toString();

    assertNotNull(result);
    assertTrue(result.contains("DolStamp{"), "Should contain class name");
    assertTrue(result.contains("term=" + TEST_LEADERSHIP_TERM), "Should contain leadership term");
    assertTrue(result.contains("host=" + TEST_HOST_ID), "Should contain host ID");
    assertTrue(result.contains("produced=false"), "Should contain produced status");
    assertTrue(result.contains("consumed=false"), "Should contain consumed status");
    assertTrue(result.contains("latencyMs="), "Should contain latency");
  }

  @Test
  public void testToStringWithProducedAndConsumed() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);
    dolStamp.setDolProduced(true);
    dolStamp.setDolConsumed(true);

    String result = dolStamp.toString();

    assertTrue(result.contains("produced=true"), "Should show produced=true");
    assertTrue(result.contains("consumed=true"), "Should show consumed=true");
  }

  @Test
  public void testToStringWithCompletedFuture() throws ExecutionException, InterruptedException {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    PubSubProduceResult produceResult = mock(PubSubProduceResult.class);
    PubSubPosition position = mock(PubSubPosition.class);
    when(produceResult.getPubSubPosition()).thenReturn(position);
    when(position.toString()).thenReturn("test-position-123");

    CompletableFuture<PubSubProduceResult> future = CompletableFuture.completedFuture(produceResult);
    dolStamp.setDolProduceFuture(future);

    String result = dolStamp.toString();

    assertTrue(result.contains("position="), "Should contain position when future is completed");
  }

  @Test
  public void testToStringWithIncompleteFuture() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    // Create an incomplete future
    CompletableFuture<PubSubProduceResult> future = new CompletableFuture<>();
    dolStamp.setDolProduceFuture(future);

    String result = dolStamp.toString();

    // Should not contain position since future is not complete
    assertFalse(result.contains("position="), "Should not contain position when future is incomplete");
  }

  @Test
  public void testToStringWithExceptionalFuture() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    CompletableFuture<PubSubProduceResult> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Test exception"));
    dolStamp.setDolProduceFuture(future);

    String result = dolStamp.toString();

    // Should not contain position since future completed exceptionally
    assertFalse(result.contains("position="), "Should not contain position when future completed exceptionally");
  }

  @Test
  public void testToStringWithNullFuture() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, TEST_HOST_ID);

    String result = dolStamp.toString();

    // Should not throw NPE and should not contain position
    assertFalse(result.contains("position="), "Should not contain position when future is null");
  }

  @Test
  public void testMultipleDolStampsIndependent() {
    DolStamp dolStamp1 = new DolStamp(1L, "host-1");
    DolStamp dolStamp2 = new DolStamp(2L, "host-2");

    dolStamp1.setDolProduced(true);
    dolStamp2.setDolConsumed(true);

    assertEquals(dolStamp1.getLeadershipTerm(), 1L);
    assertEquals(dolStamp2.getLeadershipTerm(), 2L);
    assertTrue(dolStamp1.isDolProduced());
    assertFalse(dolStamp1.isDolConsumed());
    assertFalse(dolStamp2.isDolProduced());
    assertTrue(dolStamp2.isDolConsumed());
  }

  @Test
  public void testDolStampWithZeroLeadershipTerm() {
    DolStamp dolStamp = new DolStamp(0L, TEST_HOST_ID);

    assertEquals(dolStamp.getLeadershipTerm(), 0L);
  }

  @Test
  public void testDolStampWithNegativeLeadershipTerm() {
    DolStamp dolStamp = new DolStamp(-1L, TEST_HOST_ID);

    assertEquals(dolStamp.getLeadershipTerm(), -1L);
  }

  @Test
  public void testDolStampWithEmptyHostId() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, "");

    assertEquals(dolStamp.getHostId(), "");
  }

  @Test
  public void testDolStampWithNullHostId() {
    DolStamp dolStamp = new DolStamp(TEST_LEADERSHIP_TERM, null);

    assertNull(dolStamp.getHostId());
  }
}
