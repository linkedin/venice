package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.pubsub.PubSubHealthCategory;
import com.linkedin.venice.pubsub.PubSubHealthChangeListener;
import com.linkedin.venice.pubsub.PubSubHealthStatus;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PubSubHealthMonitorTest {
  private VeniceServerConfig serverConfig;
  private TopicManagerRepository topicManagerRepository;
  private PubSubTopicRepository pubSubTopicRepository;
  private PubSubHealthMonitor monitor;

  @BeforeMethod
  public void setUp() {
    serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.isPubSubHealthMonitorEnabled()).thenReturn(true);
    when(serverConfig.getPubSubHealthProbeIntervalSeconds()).thenReturn(1); // Fast for tests

    topicManagerRepository = mock(TopicManagerRepository.class);
    pubSubTopicRepository = new PubSubTopicRepository();

    monitor = new PubSubHealthMonitor(serverConfig, topicManagerRepository);
  }

  @AfterMethod
  public void tearDown() {
    monitor.stopInner();
  }

  @Test
  public void testDisabledMonitorAlwaysReturnsHealthy() {
    when(serverConfig.isPubSubHealthMonitorEnabled()).thenReturn(false);
    PubSubHealthMonitor disabledMonitor = new PubSubHealthMonitor(serverConfig, topicManagerRepository);

    disabledMonitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);
    assertTrue(disabledMonitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
  }

  @Test
  public void testExceptionMarksUnhealthy() {
    monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);
    assertFalse(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
    assertEquals(monitor.getHealthStatus("broker1:9092", PubSubHealthCategory.BROKER), PubSubHealthStatus.UNHEALTHY);
  }

  @Test
  public void testUnknownTargetIsHealthy() {
    assertTrue(monitor.isHealthy("unknown:9092", PubSubHealthCategory.BROKER));
    assertEquals(monitor.getHealthStatus("unknown:9092", PubSubHealthCategory.BROKER), PubSubHealthStatus.HEALTHY);
  }

  @Test
  public void testCategoriesAreIndependent() {
    monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);
    assertFalse(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
    assertTrue(monitor.isHealthy("broker1:9092", PubSubHealthCategory.METADATA_SERVICE));
  }

  @Test
  public void testBrokersAreIndependent() {
    monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);
    assertFalse(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
    assertTrue(monitor.isHealthy("broker2:9092", PubSubHealthCategory.BROKER));
  }

  @Test
  public void testUnhealthyCount() {
    assertEquals(monitor.getUnhealthyCount(PubSubHealthCategory.BROKER), 0);

    monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);
    monitor.reportPubSubException("broker2:9092", PubSubHealthCategory.BROKER);
    assertEquals(monitor.getUnhealthyCount(PubSubHealthCategory.BROKER), 2);
    assertEquals(monitor.getUnhealthyCount(PubSubHealthCategory.METADATA_SERVICE), 0);
  }

  @Test
  public void testListenerNotifiedOnUnhealthyTransition() throws Exception {
    monitor.startInner();
    try {
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<PubSubHealthStatus> capturedStatus = new AtomicReference<>();
      AtomicReference<String> capturedAddress = new AtomicReference<>();

      monitor.registerListener((address, category, newStatus) -> {
        capturedAddress.set(address);
        capturedStatus.set(newStatus);
        latch.countDown();
      });

      monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);

      assertTrue(latch.await(5, TimeUnit.SECONDS), "Listener should be notified");
      assertEquals(capturedStatus.get(), PubSubHealthStatus.UNHEALTHY);
      assertEquals(capturedAddress.get(), "broker1:9092");
    } finally {
      monitor.stopInner();
    }
  }

  @Test
  public void testRecoveryProbeTransitionsToHealthy() throws Exception {
    // Set up a topic manager that reports the topic exists (probe succeeds)
    TopicManager topicManager = mock(TopicManager.class);
    when(topicManager.containsTopic(any())).thenReturn(true);
    when(topicManagerRepository.getTopicManager("broker1:9092")).thenReturn(topicManager);

    PubSubTopic probeTopic = pubSubTopicRepository.getTopic("test_store_v1");
    monitor.setProbeTopic(probeTopic);

    monitor.startInner();
    try {
      // Mark broker unhealthy
      monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);
      assertFalse(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));

      // Set up listener to wait for recovery
      CountDownLatch recoveryLatch = new CountDownLatch(1);
      AtomicReference<PubSubHealthStatus> capturedStatus = new AtomicReference<>();
      monitor.registerListener((address, category, newStatus) -> {
        if (newStatus == PubSubHealthStatus.HEALTHY) {
          capturedStatus.set(newStatus);
          recoveryLatch.countDown();
        }
      });

      // Wait for the probe to run and detect recovery (probe interval = 1s)
      assertTrue(recoveryLatch.await(10, TimeUnit.SECONDS), "Should recover within probe interval");
      assertEquals(capturedStatus.get(), PubSubHealthStatus.HEALTHY);
      assertTrue(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
    } finally {
      monitor.stopInner();
    }
  }

  @Test
  public void testRecoveryProbeFailureKeepsUnhealthy() throws Exception {
    // Set up a topic manager that throws (probe fails)
    TopicManager topicManager = mock(TopicManager.class);
    when(topicManager.containsTopic(any())).thenThrow(new RuntimeException("Broker still down"));
    when(topicManagerRepository.getTopicManager("broker1:9092")).thenReturn(topicManager);

    PubSubTopic probeTopic = pubSubTopicRepository.getTopic("test_store_v1");
    monitor.setProbeTopic(probeTopic);

    monitor.startInner();
    try {
      monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);

      // Wait a few probe intervals
      Thread.sleep(3000);

      // Should still be unhealthy
      assertFalse(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
    } finally {
      monitor.stopInner();
    }
  }

  @Test
  public void testStartStopLifecycle() throws Exception {
    assertTrue(monitor.startInner());
    monitor.stopInner();

    // Should be able to start and stop without error
    // Calling stop again should be safe
    monitor.stopInner();
  }

  @Test
  public void testStartDisabledIsNoOp() throws Exception {
    when(serverConfig.isPubSubHealthMonitorEnabled()).thenReturn(false);
    PubSubHealthMonitor disabledMonitor = new PubSubHealthMonitor(serverConfig, topicManagerRepository);
    assertTrue(disabledMonitor.startInner());
    disabledMonitor.stopInner();
  }

  /**
   * Simulates:
   * 1. A PubSub exception triggers reportPubSubException (as SIT would do when pausing a partition)
   * 2. Monitor transitions to UNHEALTHY
   * 3. Recovery probe succeeds → monitor transitions to HEALTHY
   * 4. HEALTHY callback fires → the listener (simulating KSIS) is notified to resume partitions
   */
  @Test
  public void testFullPauseResumeFlowViaHealthMonitor() throws Exception {
    TopicManager topicManager = mock(TopicManager.class);
    when(topicManager.containsTopic(any())).thenReturn(true); // Probe succeeds immediately
    when(topicManagerRepository.getTopicManager("broker1:9092")).thenReturn(topicManager);

    PubSubTopic probeTopic = pubSubTopicRepository.getTopic("test_store_v1");
    monitor.setProbeTopic(probeTopic);

    // Track partition pause/resume state (simulating SIT)
    Set<Integer> pausedPartitions = new HashSet<>();
    AtomicBoolean resumeCalled = new AtomicBoolean(false);
    CountDownLatch resumeLatch = new CountDownLatch(1);

    // Simulate KSIS's onHealthStatusChanged: on HEALTHY, resume all paused partitions
    monitor.registerListener((address, category, newStatus) -> {
      if (newStatus == PubSubHealthStatus.HEALTHY && category == PubSubHealthCategory.BROKER) {
        // This is what KSIS.onHealthStatusChanged does: iterate SITs and resume
        pausedPartitions.clear();
        resumeCalled.set(true);
        resumeLatch.countDown();
      }
    });

    monitor.startInner();
    try {
      // Step 1: Simulate partition encountering PubSub exception → partition gets paused
      pausedPartitions.add(0);
      pausedPartitions.add(1);

      // Step 2: Report the exception to monitor (as SIT.processIngestionException does)
      monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);
      assertFalse(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));

      // Step 3: Wait for recovery probe to succeed and HEALTHY callback to fire
      assertTrue(resumeLatch.await(10, TimeUnit.SECONDS), "Resume callback should fire after probe recovery");
      assertTrue(resumeCalled.get());
      assertTrue(pausedPartitions.isEmpty(), "All partitions should be resumed after broker recovery");
      assertTrue(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
    } finally {
      monitor.stopInner();
    }
  }

  /**
   * Verifies that multiple brokers are tracked independently.
   * Broker1 recovers but broker2 stays unhealthy — only broker1's partitions should resume.
   */
  @Test
  public void testMultipleBrokerIndependentRecovery() throws Exception {
    // broker1 probe succeeds, broker2 probe fails
    TopicManager topicManager1 = mock(TopicManager.class);
    when(topicManager1.containsTopic(any())).thenReturn(true);
    when(topicManagerRepository.getTopicManager("broker1:9092")).thenReturn(topicManager1);

    TopicManager topicManager2 = mock(TopicManager.class);
    when(topicManager2.containsTopic(any())).thenThrow(new RuntimeException("Broker2 still down"));
    when(topicManagerRepository.getTopicManager("broker2:9092")).thenReturn(topicManager2);

    PubSubTopic probeTopic = pubSubTopicRepository.getTopic("test_store_v1");
    monitor.setProbeTopic(probeTopic);

    Set<String> recoveredBrokers = new HashSet<>();
    CountDownLatch broker1RecoveryLatch = new CountDownLatch(1);

    monitor.registerListener((address, category, newStatus) -> {
      if (newStatus == PubSubHealthStatus.HEALTHY) {
        recoveredBrokers.add(address);
        if ("broker1:9092".equals(address)) {
          broker1RecoveryLatch.countDown();
        }
      }
    });

    monitor.startInner();
    try {
      monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);
      monitor.reportPubSubException("broker2:9092", PubSubHealthCategory.BROKER);

      assertFalse(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
      assertFalse(monitor.isHealthy("broker2:9092", PubSubHealthCategory.BROKER));

      // Wait for broker1 recovery
      assertTrue(broker1RecoveryLatch.await(10, TimeUnit.SECONDS));

      assertTrue(recoveredBrokers.contains("broker1:9092"));
      assertFalse(recoveredBrokers.contains("broker2:9092"));
      assertTrue(monitor.isHealthy("broker1:9092", PubSubHealthCategory.BROKER));
      assertFalse(monitor.isHealthy("broker2:9092", PubSubHealthCategory.BROKER));
    } finally {
      monitor.stopInner();
    }
  }

  @Test
  public void testUnregisterListener() throws Exception {
    monitor.startInner();
    try {
      CountDownLatch latch = new CountDownLatch(1);
      PubSubHealthChangeListener listener = (address, category, newStatus) -> latch.countDown();

      monitor.registerListener(listener);
      monitor.unregisterListener(listener);

      monitor.reportPubSubException("broker1:9092", PubSubHealthCategory.BROKER);

      // Listener should NOT be notified since it was unregistered
      assertFalse(latch.await(1, TimeUnit.SECONDS));
    } finally {
      monitor.stopInner();
    }
  }
}
