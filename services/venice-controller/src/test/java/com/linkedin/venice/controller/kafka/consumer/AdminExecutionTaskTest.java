package com.linkedin.venice.controller.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
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
  private Queue<AdminOperationWrapper> internalTopic;
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
    internalTopic = new ConcurrentLinkedQueue<>();
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
    verify(mockStats, never()).recordIncrementViolationStoresCount();
    verify(mockStats, never()).recordDecrementViolationStoresCount();
  }

  /**
   * Test when there are more than one thread working on the same store
   */
  @Test
  public void testViolationWhenMultipleThreads() {
    // Setup: Simulate that another thread is already processing this store
    inflightThreadsByStore.put(storeName, new AtomicInteger(1));
    when(mockAdmin.isLeaderControllerFor(clusterName)).thenReturn(true);

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

    // Verify: Violation should be recorded when counter goes from 1 to 2
    verify(mockStats, times(1)).recordIncrementViolationStoresCount();
    // And decremented when it goes from 2 to 1
    verify(mockStats, times(1)).recordDecrementViolationStoresCount();
    // Counter should be back to 1
    assertEquals(inflightThreadsByStore.get(storeName).get(), 1);
  }

  /**
   * Test concurrent execution of multiple tasks on the same store to verify thread-safe counter updates.
   * This test spawns two threads that execute tasks for the same store simultaneously and verifies:
   * 1. The inflight counter correctly tracks concurrent threads (goes up to 2)
   * 2. Violation stats are recorded when multiple threads are detected
   * 3. The counter is properly decremented and cleaned up after both threads finish
   *
   * Thread 1 is intentionally slowed down to ensure both threads are running concurrently,
   * allowing us to observe the counter value of 2.
   */
  @Test
  public void testConcurrentTaskExecutionUpdatesInflightCounterCorrectly() throws Exception {
    // Setup: Create two separate queues for two tasks
    Queue<AdminOperationWrapper> queue1 = new ConcurrentLinkedQueue<>();
    Queue<AdminOperationWrapper> queue2 = new ConcurrentLinkedQueue<>();

    // Add messages to both queues
    queue1.add(createMockAdminOperationWrapper(1L));
    queue2.add(createMockAdminOperationWrapper(2L));

    // Latches to control execution flow and observe concurrent state
    CountDownLatch thread1StartedLatch = new CountDownLatch(1);
    CountDownLatch thread2CanCheckCounterLatch = new CountDownLatch(1);
    CountDownLatch verificationDoneLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(2);

    AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    // Mock admin to introduce controlled delays
    when(mockAdmin.isLeaderControllerFor(clusterName)).thenAnswer(invocation -> {
      String threadName = Thread.currentThread().getName();
      if (threadName.equals("slow-thread")) {
        // Thread 1 (slow): Signal that it has started and incremented counter to 1
        thread1StartedLatch.countDown();
        // Wait for thread 2 to start and observe the counter
        assertTrue(verificationDoneLatch.await(10, java.util.concurrent.TimeUnit.SECONDS));
      } else if (threadName.equals("fast-thread")) {
        // Thread 2 (fast): Wait for thread 1 to start first
        assertTrue(thread1StartedLatch.await(10, java.util.concurrent.TimeUnit.SECONDS));
        // Signal that we can now check the counter (both threads are running)
        thread2CanCheckCounterLatch.countDown();
        // Wait for verification to complete
        assertTrue(verificationDoneLatch.await(10, java.util.concurrent.TimeUnit.SECONDS));
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
        queue1,
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
        queue2,
        mockAdmin,
        mockExecutionIdAccessor,
        isParentController,
        mockStats,
        regionName,
        inflightThreadsByStore);

    // Thread 1 (slow thread)
    Thread thread1 = new Thread(() -> {
      try {
        task1.call();
      } catch (Exception e) {
        exceptionRef.set(e);
      } finally {
        completionLatch.countDown();
      }
    }, "slow-thread");

    // Thread 2 (fast thread)
    Thread thread2 = new Thread(() -> {
      try {
        task2.call();
      } catch (Exception e) {
        exceptionRef.set(e);
      } finally {
        completionLatch.countDown();
      }
    }, "fast-thread");

    // Start both threads
    thread1.start();
    thread2.start();

    // Wait for both threads to be in their paused state (both have incremented the counter)
    boolean thread2Ready = thread2CanCheckCounterLatch.await(5, java.util.concurrent.TimeUnit.SECONDS);
    assertTrue(thread2Ready, "Thread 2 should have started and both threads should be paused");

    // At this point, both threads have incremented the counter and are paused
    // Verify that the counter value is 2
    AtomicInteger counter = inflightThreadsByStore.get(storeName);
    assertNotNull(counter, "Counter should exist when both threads are running");
    assertEquals(counter.get(), 2, "Counter should be 2 when both threads are executing concurrently");

    // Release both threads to complete
    verificationDoneLatch.countDown();

    // Wait for both threads to complete (with timeout)
    boolean completed = completionLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);

    // Check if any exception occurred
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }

    // Verify completion
    assertTrue(completed, "Both threads should complete within timeout");

    // Verify: After both threads finish, the counter should be cleaned up (back to 0/null)
    assertNull(inflightThreadsByStore.get(storeName), "Counter should be removed after both threads complete");

    // Verify: Violation stats should have been recorded at least once
    // (when the second thread started and saw counter = 1)
    verify(mockStats, times(1)).recordIncrementViolationStoresCount();
    verify(mockStats, times(1)).recordDecrementViolationStoresCount();

    // Verify: Both queues should be empty
    assertEquals(queue1.size(), 0, "Queue 1 should be empty");
    assertEquals(queue2.size(), 0, "Queue 2 should be empty");
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

    PubSubPosition position = mock(PubSubPosition.class);
    when(position.getNumericOffset()).thenReturn(1L);

    AdminOperationWrapper wrapper = new AdminOperationWrapper(
        adminOperation,
        position,
        executionId,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        System.currentTimeMillis());

    return wrapper;
  }
}
