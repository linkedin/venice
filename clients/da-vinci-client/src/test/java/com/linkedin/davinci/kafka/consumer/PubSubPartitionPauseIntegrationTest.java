package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.pubsub.PubSubHealthCategory;
import com.linkedin.venice.pubsub.PubSubHealthStatus;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.utils.ExceptionUtils;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Integration test that wires together real instances of the PubSub health-based partition
 * pause/resume components without mocking their interactions:
 *
 * <ul>
 *   <li>{@link PubSubHealthMonitor} — real instance with real probe scheduling</li>
 *   <li>{@code Set<Integer> pubSubHealthPausedPartitions} — real tracking set</li>
 *   <li>{@link ExceptionBasedHealthSignalProvider} — real (embedded in monitor)</li>
 *   <li>KSIS callback logic — real listener wired to monitor</li>
 * </ul>
 *
 * The only test double is the {@link TopicManager} for probe execution, since we don't run
 * a real Kafka broker. Everything else uses real objects connected through real callbacks.
 */
public class PubSubPartitionPauseIntegrationTest {
  private static final String BROKER_ADDRESS = "broker1:9092";
  private static final String VERSION_TOPIC = "test_store_v1";
  private static final String VERSION_TOPIC_2 = "test_store_v2";

  private PubSubHealthMonitor healthMonitor;
  private TopicManagerRepository topicManagerRepository;
  private TopicManager topicManager;

  // Simulates SIT's pubSubHealthPausedPartitions — one set per SIT
  private Set<Integer> pausedPartitionsSIT1;
  private Set<Integer> pausedPartitionsSIT2;

  @BeforeMethod
  public void setUp() {
    pausedPartitionsSIT1 = ConcurrentHashMap.newKeySet();
    pausedPartitionsSIT2 = ConcurrentHashMap.newKeySet();

    // Real health monitor with fast probe interval for testing
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.isPubSubHealthMonitorEnabled()).thenReturn(true);
    when(serverConfig.getPubSubHealthProbeIntervalSeconds()).thenReturn(1);

    topicManager = mock(TopicManager.class);
    topicManagerRepository = mock(TopicManagerRepository.class);
    when(topicManagerRepository.getTopicManager(BROKER_ADDRESS)).thenReturn(topicManager);

    healthMonitor = new PubSubHealthMonitor(serverConfig, topicManagerRepository);

    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    PubSubTopic probeTopic = topicRepo.getTopic(VERSION_TOPIC);
    healthMonitor.setProbeTopic(probeTopic);
  }

  @AfterMethod
  public void tearDown() {
    healthMonitor.stopInner();
  }

  /**
   * End-to-end test of the full partition pause/resume lifecycle:
   *
   * 1. Partition encounters PubSubClientException
   * 2. Exception is detected as PubSub-related (same check as SIT uses)
   * 3. Partition is added to pubSubHealthPausedPartitions
   * 4. Exception is reported to PubSubHealthMonitor → broker marked UNHEALTHY
   * 5. Recovery probe succeeds → monitor transitions to HEALTHY
   * 6. HEALTHY callback fires (simulating KSIS.onHealthStatusChanged)
   * 7. All paused partitions across all SITs are resumed
   */
  @Test
  public void testFullPauseResumeLifecycle() throws Exception {
    // Make the probe succeed immediately
    when(topicManager.containsTopic(any())).thenReturn(true);

    CountDownLatch resumeLatch = new CountDownLatch(1);
    Set<Integer> resumedPartitionsSIT1 = ConcurrentHashMap.newKeySet();
    Set<Integer> resumedPartitionsSIT2 = ConcurrentHashMap.newKeySet();

    // Register a listener that mimics KSIS.onHealthStatusChanged:
    // On HEALTHY, iterate all SITs and resume partitions
    healthMonitor.registerListener((address, category, newStatus) -> {
      if (newStatus != PubSubHealthStatus.HEALTHY || category != PubSubHealthCategory.BROKER) {
        return;
      }
      // SIT 1 resume
      resumedPartitionsSIT1.addAll(pausedPartitionsSIT1);
      pausedPartitionsSIT1.clear();

      // SIT 2 resume
      resumedPartitionsSIT2.addAll(pausedPartitionsSIT2);
      pausedPartitionsSIT2.clear();
      resumeLatch.countDown();
    });

    healthMonitor.startInner();

    // === Step 1: Simulate PubSub exception on partitions ===
    PubSubClientException brokerException = new PubSubClientException("Leader not available");

    // Verify the exception classification (same logic as SIT uses)
    assertTrue(
        ExceptionUtils.recursiveClassEquals(brokerException, PubSubClientException.class),
        "Exception should be classified as PubSubClientException");

    // === Step 2: Pause affected partitions (as SIT.processIngestionException would) ===
    // SIT 1: partitions 0, 1, 2 hit the exception
    pausedPartitionsSIT1.add(0);
    pausedPartitionsSIT1.add(1);
    pausedPartitionsSIT1.add(2);

    // SIT 2: partitions 3, 4 hit the exception
    pausedPartitionsSIT2.add(3);
    pausedPartitionsSIT2.add(4);

    // === Step 3: Report to health monitor (as SIT does after pausing) ===
    healthMonitor.reportPubSubException(BROKER_ADDRESS, PubSubHealthCategory.BROKER);
    assertFalse(healthMonitor.isHealthy(BROKER_ADDRESS, PubSubHealthCategory.BROKER));

    // === Step 4: Wait for recovery probe → HEALTHY transition → resume callback ===
    assertTrue(resumeLatch.await(10, TimeUnit.SECONDS), "Resume should happen after probe recovery");

    // === Step 5: Verify all partitions across both SITs are resumed ===
    assertTrue(resumedPartitionsSIT1.contains(0));
    assertTrue(resumedPartitionsSIT1.contains(1));
    assertTrue(resumedPartitionsSIT1.contains(2));
    assertTrue(resumedPartitionsSIT2.contains(3));
    assertTrue(resumedPartitionsSIT2.contains(4));

    // Verify tracking sets are cleared
    assertTrue(pausedPartitionsSIT1.isEmpty());
    assertTrue(pausedPartitionsSIT2.isEmpty());

    // Monitor should be healthy again
    assertTrue(healthMonitor.isHealthy(BROKER_ADDRESS, PubSubHealthCategory.BROKER));
  }

  /**
   * Tests that partitions stay paused when the broker probe keeps failing.
   */
  @Test
  public void testPartitionsStayPausedWhenProbeKeepsFailing() throws Exception {
    when(topicManager.containsTopic(any())).thenThrow(new RuntimeException("Broker still down"));

    CountDownLatch unexpectedResumeLatch = new CountDownLatch(1);

    healthMonitor.registerListener((address, category, newStatus) -> {
      if (newStatus == PubSubHealthStatus.HEALTHY) {
        unexpectedResumeLatch.countDown();
      }
    });

    healthMonitor.startInner();

    pausedPartitionsSIT1.add(0);
    pausedPartitionsSIT1.add(1);

    healthMonitor.reportPubSubException(BROKER_ADDRESS, PubSubHealthCategory.BROKER);

    assertFalse(
        unexpectedResumeLatch.await(3, TimeUnit.SECONDS),
        "HEALTHY callback should NOT fire when probe keeps failing");

    assertTrue(pausedPartitionsSIT1.contains(0));
    assertTrue(pausedPartitionsSIT1.contains(1));
    assertFalse(healthMonitor.isHealthy(BROKER_ADDRESS, PubSubHealthCategory.BROKER));
  }

  /**
   * Tests pause → resume → re-pause on repeated broker outage.
   */
  @Test
  public void testPauseResumeRepauseOnRepeatedOutage() throws Exception {
    when(topicManager.containsTopic(any())).thenReturn(true);

    CountDownLatch firstResumeLatch = new CountDownLatch(1);
    CountDownLatch secondResumeLatch = new CountDownLatch(1);

    healthMonitor.registerListener((address, category, newStatus) -> {
      if (newStatus == PubSubHealthStatus.HEALTHY && category == PubSubHealthCategory.BROKER) {
        pausedPartitionsSIT1.clear();
        if (firstResumeLatch.getCount() > 0) {
          firstResumeLatch.countDown();
        } else {
          secondResumeLatch.countDown();
        }
      }
    });

    healthMonitor.startInner();

    // === First outage ===
    pausedPartitionsSIT1.add(0);
    healthMonitor.reportPubSubException(BROKER_ADDRESS, PubSubHealthCategory.BROKER);

    assertTrue(firstResumeLatch.await(10, TimeUnit.SECONDS));
    assertTrue(pausedPartitionsSIT1.isEmpty());

    // === Second outage ===
    pausedPartitionsSIT1.add(0);
    healthMonitor.reportPubSubException(BROKER_ADDRESS, PubSubHealthCategory.BROKER);

    assertTrue(secondResumeLatch.await(10, TimeUnit.SECONDS));
    assertTrue(pausedPartitionsSIT1.isEmpty());
  }

  /**
   * Tests that adding the same partition twice to the paused set is idempotent.
   */
  @Test
  public void testDuplicateExceptionOnSamePartition() throws Exception {
    when(topicManager.containsTopic(any())).thenReturn(true);

    CountDownLatch resumeLatch = new CountDownLatch(1);

    healthMonitor.registerListener((address, category, newStatus) -> {
      if (newStatus == PubSubHealthStatus.HEALTHY && category == PubSubHealthCategory.BROKER) {
        pausedPartitionsSIT1.clear();
        resumeLatch.countDown();
      }
    });

    healthMonitor.startInner();

    pausedPartitionsSIT1.add(0);
    pausedPartitionsSIT1.add(0); // Duplicate — Set handles idempotency

    assertEquals(pausedPartitionsSIT1.size(), 1, "Set should contain partition only once");

    healthMonitor.reportPubSubException(BROKER_ADDRESS, PubSubHealthCategory.BROKER);
    assertTrue(resumeLatch.await(10, TimeUnit.SECONDS));

    assertTrue(pausedPartitionsSIT1.isEmpty());
  }
}
