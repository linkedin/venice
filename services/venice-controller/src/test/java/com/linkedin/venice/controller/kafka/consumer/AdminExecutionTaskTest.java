package com.linkedin.venice.controller.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.meta.DegradedDcInfo;
import com.linkedin.venice.meta.DegradedDcStates;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AdminExecutionTaskTest {
  private Logger mockLogger;
  private VeniceHelixAdmin mockAdmin;
  private ExecutionIdAccessor mockExecutionIdAccessor;
  private AdminConsumptionStats mockStats;
  private ConcurrentHashMap<String, Long> lastSucceededExecutionIdMap;
  private ConcurrentHashMap<String, AtomicInteger> inflightThreadsByStore;
  private String clusterName;
  private String storeName;
  private String regionName;
  private long lastPersistedExecutionId;
  private boolean isParentController;

  @BeforeMethod
  public void setUp() {
    mockLogger = mock(Logger.class);
    mockAdmin = mock(VeniceHelixAdmin.class);
    mockExecutionIdAccessor = mock(ExecutionIdAccessor.class);
    mockStats = mock(AdminConsumptionStats.class);
    lastSucceededExecutionIdMap = new ConcurrentHashMap<>();
    inflightThreadsByStore = new ConcurrentHashMap<>();
    clusterName = "test-cluster";
    storeName = "test-store";
    regionName = "test-region";
    lastPersistedExecutionId = 0L;
    isParentController = false;
  }

  /**
   * Test when there is a new store operation and no violation occurs in inflight counter.
   */
  @Test
  public void testNoViolationInflightCounter() {
    // Setup: Create a task with an empty queue (will exit immediately)
    when(mockAdmin.isLeaderControllerFor(clusterName)).thenReturn(true);
    // Add a message to the queue so the task has something to process
    Queue<AdminOperationWrapper> internalTopic = new ConcurrentLinkedQueue<>();
    internalTopic.add(createMockAdminOperationWrapper(1L));

    AdminExecutionTask task = new AdminExecutionTask(
        mockLogger,
        clusterName,
        storeName,
        lastSucceededExecutionIdMap,
        lastPersistedExecutionId,
        internalTopic,
        mockAdmin,
        mockExecutionIdAccessor,
        isParentController,
        mockStats,
        regionName,
        inflightThreadsByStore);

    // Execute
    task.call();

    // Verify: The inflight counter should have been incremented and then decremented back to 0
    assertNull(inflightThreadsByStore.get(storeName), "Counter should be removed when it reaches 0");

    assertTrue(internalTopic.isEmpty(), "The internal topic queue should be empty after processing.");
  }

  /**
   * Test when there is a new store operation and no violation occurs in inflight counter. The task fails with an exception.
   */
  @Test
  public void testNoViolationInflightCounterWithCancellationException() throws Exception {
    // Add a message to the queue so the task has something to process
    Queue<AdminOperationWrapper> internalTopic = new ConcurrentLinkedQueue<>();
    internalTopic.add(createMockAdminOperationWrapper(1L));

    CountDownLatch threadStartedLatch = new CountDownLatch(1);
    AtomicReference<Exception> threadException = new AtomicReference<>();
    CountDownLatch completionLatch = new CountDownLatch(1);

    // Setup: Mock admin to throw exception when processing
    when(mockAdmin.isLeaderControllerFor(clusterName)).thenAnswer(invocation -> {
      threadStartedLatch.countDown();
      throw new CancellationException("Task was cancelled");
    });

    AdminExecutionTask task = new AdminExecutionTask(
        mockLogger,
        clusterName,
        storeName,
        lastSucceededExecutionIdMap,
        lastPersistedExecutionId,
        internalTopic,
        mockAdmin,
        mockExecutionIdAccessor,
        isParentController,
        mockStats,
        regionName,
        inflightThreadsByStore);

    // Execute
    Thread thread = new Thread(() -> {
      try {
        task.call();
      } catch (Exception e) {
        threadException.set(e);
      } finally {
        completionLatch.countDown();
      }
    }, "thread-with-exception");

    thread.start();
    boolean threadReady = threadStartedLatch.await(5, TimeUnit.SECONDS);
    assertTrue(threadReady, "The thread should have started");

    // Wait for thread to complete
    boolean completed = completionLatch.await(5, TimeUnit.SECONDS);
    assertTrue(completed, "The thread should complete within timeout");

    assertNotNull(threadException.get(), "Thread should have thrown an exception");
    assertTrue(
        threadException.get() instanceof CancellationException,
        "Thread should have thrown CancellationException");

    // Verify: The inflight counter should have been incremented and then decremented back to 0
    assertNull(inflightThreadsByStore.get(storeName), "Counter should be removed when there is exception");

    assertEquals(internalTopic.size(), 1, "The internal topic queue should have operation after exception.");
  }

  /**
   * Test concurrent execution where one thread gets a CancellationException.
   * This test verifies that:
   * 1. Two threads start processing the same store (counter reaches 2)
   * 2. One thread throws a CancellationException
   * 3. The counter is properly decremented even when exception occurs
   * 4. The other thread continues and completes successfully
   * 5. Violation stats are correctly recorded and cleaned up
   */
  @Test
  public void testConcurrentExecutionWithCancellationException() throws Exception {
    // Add messages
    Queue<AdminOperationWrapper> internalTopic = new ConcurrentLinkedQueue<>();
    internalTopic.add(createMockAdminOperationWrapper(1L));
    internalTopic.add(createMockAdminOperationWrapper(2L));

    // Latches to control execution flow
    CountDownLatch thread1StartedLatch = new CountDownLatch(1);
    CountDownLatch thread2StartedLatch = new CountDownLatch(1);
    CountDownLatch verificationDoneLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(2);

    AtomicReference<Exception> thread1Exception = new AtomicReference<>();
    AtomicReference<Exception> thread2Exception = new AtomicReference<>();

    // Mock admin to introduce controlled delays and throw exception for thread 1
    when(mockAdmin.isLeaderControllerFor(clusterName)).thenAnswer(invocation -> {
      String threadName = Thread.currentThread().getName();
      if (threadName.equals("thread-with-exception")) {
        // Thread 1: Signal that it has started
        thread1StartedLatch.countDown();
        // // Wait for thread 2 to start
        assertTrue(thread2StartedLatch.await(5, java.util.concurrent.TimeUnit.SECONDS));
        // Throw CancellationException
        throw new CancellationException("Task was cancelled");
      } else if (threadName.equals("thread-normal")) {
        // Thread 2: Wait for thread 1 to start first
        assertTrue(thread1StartedLatch.await(5, java.util.concurrent.TimeUnit.SECONDS));
        // Signal that thread 2 has started
        thread2StartedLatch.countDown();
        // Wait for verification
        assertTrue(verificationDoneLatch.await(5, java.util.concurrent.TimeUnit.SECONDS));
        return true;
      }
      return true;
    });

    // Create two tasks for the same store
    AdminExecutionTask task1 = new AdminExecutionTask(
        mockLogger,
        clusterName,
        storeName,
        lastSucceededExecutionIdMap,
        lastPersistedExecutionId,
        internalTopic,
        mockAdmin,
        mockExecutionIdAccessor,
        isParentController,
        mockStats,
        regionName,
        inflightThreadsByStore);

    AdminExecutionTask task2 = new AdminExecutionTask(
        mockLogger,
        clusterName,
        storeName,
        lastSucceededExecutionIdMap,
        lastPersistedExecutionId,
        internalTopic,
        mockAdmin,
        mockExecutionIdAccessor,
        isParentController,
        mockStats,
        regionName,
        inflightThreadsByStore);

    // Thread 1 (will throw CancellationException)
    Thread thread1 = new Thread(() -> {
      try {
        task1.call();
      } catch (Exception e) {
        thread1Exception.set(e);
      } finally {
        completionLatch.countDown();
      }
    }, "thread-with-exception");

    // Thread 2 (normal execution)
    Thread thread2 = new Thread(() -> {
      try {
        task2.call();
      } catch (Exception e) {
        thread2Exception.set(e);
      } finally {
        completionLatch.countDown();
      }
    }, "thread-normal");

    // Start both threads
    thread1.start();
    thread2.start();

    // Wait for both threads to have started
    boolean thread1Ready = thread1StartedLatch.await(5, java.util.concurrent.TimeUnit.SECONDS);
    boolean thread2Ready = thread2StartedLatch.await(5, java.util.concurrent.TimeUnit.SECONDS);

    assertTrue(thread1Ready, "Thread 1 should have started");
    assertTrue(thread2Ready, "Thread 2 should have started");

    // At this point, both threads have incremented the counter
    // Verify that the counter value is 2 before exception occurs
    AtomicInteger counter = inflightThreadsByStore.get(storeName);
    assertNotNull(counter, "Counter should exist when both threads are running");
    assertEquals(counter.get(), 2, "Counter should be 2 when both threads are executing concurrently");

    // Release thread 2 to complete
    verificationDoneLatch.countDown();

    // Wait for both threads to complete
    boolean completed = completionLatch.await(5, TimeUnit.SECONDS);
    assertTrue(completed, "Both threads should complete within timeout");

    // Verify: Thread 1 should have CancellationException
    assertNotNull(thread1Exception.get(), "Thread 1 should have thrown an exception");
    assertTrue(
        thread1Exception.get() instanceof CancellationException,
        "Thread 1 should have thrown CancellationException");

    // Verify: Thread 2 should complete successfully without exception
    assertNull(thread2Exception.get(), "Thread 2 should not have thrown an exception");

    // Verify: After both threads finish, the counter should be cleaned up
    // Even though one thread threw an exception, the finally block should decrement the counter
    assertNull(inflightThreadsByStore.get(storeName), "Counter should be removed after both threads complete");
  }

  /**
   * Helper method to create a mock AdminOperationWrapper.
   */
  private AdminOperationWrapper createMockAdminOperationWrapper(long executionId) {
    AdminOperation adminOperation = new AdminOperation();
    adminOperation.operationType = AdminMessageType.STORE_CREATION.getValue();
    adminOperation.executionId = executionId;

    StoreCreation storeCreation = new StoreCreation();
    storeCreation.clusterName = clusterName;
    storeCreation.storeName = storeName;
    storeCreation.owner = "test-owner";
    storeCreation.keySchema = new com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta();
    storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.getValue();
    storeCreation.keySchema.definition = "\"string\"";
    storeCreation.valueSchema = new com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta();
    storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.getValue();
    storeCreation.valueSchema.definition = "\"string\"";

    adminOperation.payloadUnion = storeCreation;

    PubSubPosition position = InMemoryPubSubPosition.of(1L);
    AdminOperationWrapper wrapper = new AdminOperationWrapper(
        adminOperation,
        position,
        executionId,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        System.currentTimeMillis());

    return wrapper;
  }

  @Test
  public void testResolvePausedRegions_NotPausedReturnsEmpty() {
    com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore message =
        new com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore();
    message.ingestionPauseMode = com.linkedin.venice.meta.IngestionPauseMode.NOT_PAUSED.getValue();
    message.ingestionPausedRegions = java.util.Arrays.asList("should-be-ignored");
    // Even if the message carries a stale list, NOT_PAUSED normalizes to empty so stale regions
    // never leak past a resume.
    assertTrue(AdminExecutionTask.resolvePausedRegions(message).isEmpty());
  }

  @Test
  public void testResolvePausedRegions_NullRegionsReturnsEmpty() {
    com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore message =
        new com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore();
    message.ingestionPauseMode = com.linkedin.venice.meta.IngestionPauseMode.ALL_VERSIONS.getValue();
    message.ingestionPausedRegions = null;
    // Defensive: older admin messages may not populate the field; must not NPE.
    assertTrue(AdminExecutionTask.resolvePausedRegions(message).isEmpty());
  }

  @Test
  public void testResolvePausedRegions_PausedWithRegionsConvertsCharSequenceToString() {
    com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore message =
        new com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore();
    message.ingestionPauseMode = com.linkedin.venice.meta.IngestionPauseMode.ALL_VERSIONS.getValue();
    message.ingestionPausedRegions = java.util.Arrays.asList("dc-0", "dc-1");
    java.util.List<String> resolved = AdminExecutionTask.resolvePausedRegions(message);
    assertEquals(resolved, java.util.Arrays.asList("dc-0", "dc-1"));
  }

  // --- Degraded mode skipConsumption tests ---
  // These exercise the actual AdminExecutionTask.handleAddVersion code path with mock
  // AddVersion messages and degraded DC state, verifying addVersionAndStartIngestion
  // is called or skipped based on the degraded DC condition.

  @Test
  public void testDegradedDcSkipsAddVersionEvenWithDeferredSwap() {
    // Region is excluded from targetedRegions AND marked degraded AND versionSwapDeferred=true.
    // The degraded DC should skip consumption (no addVersionAndStartIngestion call).
    when(mockAdmin.isLeaderControllerFor(clusterName)).thenReturn(true);
    when(mockAdmin.isDegradedModeEnabled(clusterName)).thenReturn(true);
    DegradedDcStates states = new DegradedDcStates();
    states.addDegradedDatacenter(regionName, new DegradedDcInfo(System.currentTimeMillis(), 120, "op"));
    when(mockAdmin.getDegradedDcStates(clusterName)).thenReturn(states);

    Queue<AdminOperationWrapper> queue = new ConcurrentLinkedQueue<>();
    queue.add(createAddVersionWrapper(1L, Arrays.asList("other-region"), true));

    AdminExecutionTask task = new AdminExecutionTask(
        mockLogger,
        clusterName,
        storeName,
        lastSucceededExecutionIdMap,
        lastPersistedExecutionId,
        queue,
        mockAdmin,
        mockExecutionIdAccessor,
        isParentController,
        mockStats,
        regionName,
        inflightThreadsByStore);
    task.call();

    // addVersionAndStartIngestion should NOT have been called — degraded DC skips
    // 14-param overload on VeniceHelixAdmin (not the 12-param Admin interface method)
    verify(mockAdmin, never()).addVersionAndStartIngestion(
        anyString(),
        anyString(),
        anyString(),
        anyInt(),
        anyInt(),
        any(),
        any(),
        anyLong(),
        anyInt(),
        any(Boolean.class),
        anyString(),
        anyInt(),
        anyInt(),
        anyInt());
  }

  @Test
  public void testNonDegradedDcWithDeferredSwapProcessesAddVersion() {
    // Region is excluded from targetedRegions AND versionSwapDeferred=true BUT NOT degraded.
    // Existing behavior: non-degraded region with deferred swap should process the version
    // (this is how targeted region push with deferred swap works normally).
    when(mockAdmin.isLeaderControllerFor(clusterName)).thenReturn(true);
    when(mockAdmin.isDegradedModeEnabled(clusterName)).thenReturn(true);
    when(mockAdmin.getDegradedDcStates(clusterName)).thenReturn(new DegradedDcStates());

    Queue<AdminOperationWrapper> queue = new ConcurrentLinkedQueue<>();
    queue.add(createAddVersionWrapper(1L, Arrays.asList("other-region"), true));

    AdminExecutionTask task = new AdminExecutionTask(
        mockLogger,
        clusterName,
        storeName,
        lastSucceededExecutionIdMap,
        lastPersistedExecutionId,
        queue,
        mockAdmin,
        mockExecutionIdAccessor,
        isParentController,
        mockStats,
        regionName,
        inflightThreadsByStore);
    task.call();

    // addVersionAndStartIngestion SHOULD have been called — non-degraded DC with deferred swap
    // 14-param overload on VeniceHelixAdmin (not the 12-param Admin interface method)
    verify(mockAdmin).addVersionAndStartIngestion(
        anyString(),
        anyString(),
        anyString(),
        anyInt(),
        anyInt(),
        any(),
        any(),
        anyLong(),
        anyInt(),
        any(Boolean.class),
        anyString(),
        anyInt(),
        anyInt(),
        anyInt());
  }

  @Test
  public void testTargetedRegionAlwaysProcessesAddVersion() {
    // Region IS in targetedRegions — should always process regardless of degraded state.
    when(mockAdmin.isLeaderControllerFor(clusterName)).thenReturn(true);
    // Don't even need to mock degraded state — skipConsumption is false for targeted regions.

    Queue<AdminOperationWrapper> queue = new ConcurrentLinkedQueue<>();
    queue.add(createAddVersionWrapper(1L, Arrays.asList(regionName), true));

    AdminExecutionTask task = new AdminExecutionTask(
        mockLogger,
        clusterName,
        storeName,
        lastSucceededExecutionIdMap,
        lastPersistedExecutionId,
        queue,
        mockAdmin,
        mockExecutionIdAccessor,
        isParentController,
        mockStats,
        regionName,
        inflightThreadsByStore);
    task.call();

    // addVersionAndStartIngestion SHOULD have been called — targeted region always processes
    // 14-param overload on VeniceHelixAdmin (not the 12-param Admin interface method)
    verify(mockAdmin).addVersionAndStartIngestion(
        anyString(),
        anyString(),
        anyString(),
        anyInt(),
        anyInt(),
        any(),
        any(),
        anyLong(),
        anyInt(),
        any(Boolean.class),
        anyString(),
        anyInt(),
        anyInt(),
        anyInt());
  }

  private AdminOperationWrapper createAddVersionWrapper(
      long executionId,
      java.util.List<String> targetedRegions,
      boolean versionSwapDeferred) {
    AdminOperation adminOperation = new AdminOperation();
    adminOperation.operationType = AdminMessageType.ADD_VERSION.getValue();
    adminOperation.executionId = executionId;

    AddVersion addVersion = new AddVersion();
    addVersion.clusterName = clusterName;
    addVersion.storeName = storeName;
    addVersion.pushJobId = "test-push-job";
    addVersion.versionNum = 1;
    addVersion.numberOfPartitions = 1;
    addVersion.pushType = Version.PushType.BATCH.getValue();
    addVersion.pushStreamSourceAddress = null;
    addVersion.rewindTimeInSecondsOverride = -1;
    addVersion.timestampMetadataVersionId = -1;
    addVersion.versionSwapDeferred = versionSwapDeferred;
    addVersion.targetedRegions = new java.util.ArrayList<>(targetedRegions);
    addVersion.repushSourceVersion = -1;
    addVersion.currentRTVersionNumber = 0;
    addVersion.repushTtlSeconds = -1;

    adminOperation.payloadUnion = addVersion;

    PubSubPosition position = InMemoryPubSubPosition.of(1L);
    return new AdminOperationWrapper(
        adminOperation,
        position,
        executionId,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        System.currentTimeMillis());
  }
}
