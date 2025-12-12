package com.linkedin.venice.controller.init;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.LogContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;


public class ClusterLeaderInitializationManagerTest {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final long TIMEOUT_MS = 5000;
  private static final LogContext TEST_LOG_CONTEXT = new LogContext.Builder().build();

  /**
   * Tests blocking behavior for first and subsequent executions.
   */
  @Test(timeOut = TIMEOUT_MS)
  public void testBlockingBehavior() throws InterruptedException {
    // Case 1: Async execution when blocking is disabled
    CountDownLatch routineStarted = new CountDownLatch(1);
    CountDownLatch routineCanFinish = new CountDownLatch(1);
    ClusterLeaderInitializationRoutine slowRoutine = mock(ClusterLeaderInitializationRoutine.class);
    doAnswer(invocation -> {
      routineStarted.countDown();
      routineCanFinish.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return null;
    }).when(slowRoutine).execute(anyString());

    ClusterLeaderInitializationManager asyncManager = new ClusterLeaderInitializationManager(
        Collections.singletonList(slowRoutine),
        false, // concurrentInit
        false, // blockUntilComplete
        TEST_LOG_CONTEXT);

    long startTime = System.currentTimeMillis();
    asyncManager.execute(TEST_CLUSTER);
    long executionTime = System.currentTimeMillis() - startTime;

    assertTrue(executionTime < 500, "Execute should return immediately when blocking is disabled");
    assertTrue(routineStarted.await(TIMEOUT_MS, TimeUnit.MILLISECONDS), "Routine should start executing");
    routineCanFinish.countDown();
    Thread.sleep(100);
    verify(slowRoutine, times(1)).execute(TEST_CLUSTER);

    // Case 2: Blocking on first execution when flag is enabled
    AtomicBoolean routineCompleted = new AtomicBoolean(false);
    ClusterLeaderInitializationRoutine blockingRoutine = mock(ClusterLeaderInitializationRoutine.class);
    doAnswer(invocation -> {
      Thread.sleep(200);
      routineCompleted.set(true);
      return null;
    }).when(blockingRoutine).execute(anyString());

    ClusterLeaderInitializationManager blockingManager = new ClusterLeaderInitializationManager(
        Collections.singletonList(blockingRoutine),
        false, // concurrentInit
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    blockingManager.execute(TEST_CLUSTER);
    assertTrue(routineCompleted.get(), "Routine should be completed after execute returns");
    verify(blockingRoutine, times(1)).execute(TEST_CLUSTER);

    // Case 3: Subsequent executions are async even when blockUntilComplete is true
    AtomicInteger executionCount = new AtomicInteger(0);
    ClusterLeaderInitializationRoutine subsequentRoutine = mock(ClusterLeaderInitializationRoutine.class);
    doAnswer(invocation -> {
      executionCount.incrementAndGet();
      return null;
    }).when(subsequentRoutine).execute(anyString());

    ClusterLeaderInitializationManager subsequentManager = new ClusterLeaderInitializationManager(
        Collections.singletonList(subsequentRoutine),
        false, // concurrentInit
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    // First execute() - blocks
    subsequentManager.execute(TEST_CLUSTER);
    assertEquals(executionCount.get(), 1, "First execution should have run");

    // Second execute() - async (doesn't block)
    startTime = System.currentTimeMillis();
    subsequentManager.execute(TEST_CLUSTER);
    executionTime = System.currentTimeMillis() - startTime;

    assertTrue(executionTime < 500, "Second execution should return immediately (async)");
    Thread.sleep(100); // Wait for async execution
    assertEquals(executionCount.get(), 1, "Routine should not execute again (already initialized)");
  }

  /**
   * Tests concurrent and sequential execution modes.
   */
  @Test(timeOut = TIMEOUT_MS)
  public void testExecutionModes() throws InterruptedException {
    // Case 1: Concurrent execution - routines run in parallel
    int routineCount = 3;
    CountDownLatch allRoutinesStarted = new CountDownLatch(routineCount);
    List<ClusterLeaderInitializationRoutine> concurrentRoutines = new ArrayList<>();

    for (int i = 0; i < routineCount; i++) {
      ClusterLeaderInitializationRoutine routine = mock(ClusterLeaderInitializationRoutine.class);
      doAnswer(invocation -> {
        allRoutinesStarted.countDown();
        Thread.sleep(100);
        return null;
      }).when(routine).execute(anyString());
      concurrentRoutines.add(routine);
    }

    ClusterLeaderInitializationManager concurrentManager = new ClusterLeaderInitializationManager(
        concurrentRoutines,
        true, // concurrentInit
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    long startTime = System.currentTimeMillis();
    concurrentManager.execute(TEST_CLUSTER);
    long executionTime = System.currentTimeMillis() - startTime;

    assertTrue(allRoutinesStarted.await(100, TimeUnit.MILLISECONDS), "All routines should start concurrently");
    assertTrue(executionTime < 250, "Concurrent execution should be faster than sequential");
    for (ClusterLeaderInitializationRoutine routine: concurrentRoutines) {
      verify(routine, times(1)).execute(TEST_CLUSTER);
    }

    // Case 2: Sequential execution - routines run in order
    List<Integer> executionSequence = Collections.synchronizedList(new ArrayList<>());
    List<ClusterLeaderInitializationRoutine> sequentialRoutines = new ArrayList<>();

    for (int i = 0; i < routineCount; i++) {
      final int routineId = i;
      ClusterLeaderInitializationRoutine routine = mock(ClusterLeaderInitializationRoutine.class);
      doAnswer(invocation -> {
        executionSequence.add(routineId);
        Thread.sleep(50);
        return null;
      }).when(routine).execute(anyString());
      sequentialRoutines.add(routine);
    }

    ClusterLeaderInitializationManager sequentialManager = new ClusterLeaderInitializationManager(
        sequentialRoutines,
        false, // concurrentInit (sequential)
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    sequentialManager.execute(TEST_CLUSTER);

    assertEquals(executionSequence.size(), routineCount, "All routines should execute");
    assertEquals(executionSequence, Arrays.asList(0, 1, 2), "Routines should execute in order");
  }

  /**
   * Tests failure handling in both concurrent and sequential modes, plus retry logic.
   */
  @Test(timeOut = TIMEOUT_MS)
  public void testFailureHandling() throws InterruptedException {
    // Case 1: Concurrent mode - failures don't prevent other routines
    ClusterLeaderInitializationRoutine failingRoutine1 = mock(ClusterLeaderInitializationRoutine.class);
    doThrow(new RuntimeException("Test failure")).when(failingRoutine1).execute(anyString());

    AtomicBoolean successRoutineExecuted1 = new AtomicBoolean(false);
    ClusterLeaderInitializationRoutine successRoutine1 = mock(ClusterLeaderInitializationRoutine.class);
    doAnswer(invocation -> {
      Thread.sleep(50);
      successRoutineExecuted1.set(true);
      return null;
    }).when(successRoutine1).execute(anyString());

    ClusterLeaderInitializationManager concurrentManager = new ClusterLeaderInitializationManager(
        Arrays.asList(failingRoutine1, successRoutine1),
        true, // concurrentInit
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    concurrentManager.execute(TEST_CLUSTER);
    assertTrue(successRoutineExecuted1.get(), "Successful routine should execute despite other routine failing");
    verify(failingRoutine1, times(1)).execute(TEST_CLUSTER);
    verify(successRoutine1, times(1)).execute(TEST_CLUSTER);

    // Case 2: Sequential mode - failures don't prevent subsequent routines
    ClusterLeaderInitializationRoutine failingRoutine2 = mock(ClusterLeaderInitializationRoutine.class);
    doThrow(new RuntimeException("Test failure")).when(failingRoutine2).execute(anyString());

    AtomicBoolean successRoutineExecuted2 = new AtomicBoolean(false);
    ClusterLeaderInitializationRoutine successRoutine2 = mock(ClusterLeaderInitializationRoutine.class);
    doAnswer(invocation -> {
      successRoutineExecuted2.set(true);
      return null;
    }).when(successRoutine2).execute(anyString());

    ClusterLeaderInitializationManager sequentialManager = new ClusterLeaderInitializationManager(
        Arrays.asList(failingRoutine2, successRoutine2),
        false, // concurrentInit (sequential)
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    sequentialManager.execute(TEST_CLUSTER);
    assertTrue(successRoutineExecuted2.get(), "Subsequent routine should execute despite previous failure");
    verify(failingRoutine2, times(1)).execute(TEST_CLUSTER);
    verify(successRoutine2, times(1)).execute(TEST_CLUSTER);

    // Case 3: Failed routines can be retried on subsequent execute() calls (async after first)
    AtomicInteger attemptCount = new AtomicInteger(0);
    ClusterLeaderInitializationRoutine flakyRoutine = mock(ClusterLeaderInitializationRoutine.class);
    doAnswer(invocation -> {
      int attempt = attemptCount.incrementAndGet();
      if (attempt == 1) {
        throw new RuntimeException("First attempt fails");
      }
      return null;
    }).when(flakyRoutine).execute(anyString());

    ClusterLeaderInitializationManager retryManager = new ClusterLeaderInitializationManager(
        Collections.singletonList(flakyRoutine),
        false, // concurrentInit
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    // First execute() - blocks and routine fails
    retryManager.execute(TEST_CLUSTER);
    assertEquals(attemptCount.get(), 1, "First attempt should have been made");

    // Second execute() - async, routine retries and succeeds
    retryManager.execute(TEST_CLUSTER);
    Thread.sleep(100); // Wait for async execution
    assertEquals(attemptCount.get(), 2, "Second attempt should have been made");

    // Third execute() - async, routine should not execute again (already succeeded)
    retryManager.execute(TEST_CLUSTER);
    Thread.sleep(100);
    assertEquals(attemptCount.get(), 2, "No third attempt should be made (already succeeded)");
  }

  /**
   * Tests edge cases: multiple clusters and empty routine list.
   */
  @Test(timeOut = TIMEOUT_MS)
  public void testEdgeCases() throws InterruptedException {
    // Case 1: First execute() blocks, subsequent calls are async
    AtomicInteger cluster1Count = new AtomicInteger(0);
    AtomicInteger cluster2Count = new AtomicInteger(0);

    ClusterLeaderInitializationRoutine routine = mock(ClusterLeaderInitializationRoutine.class);
    doAnswer(invocation -> {
      String cluster = invocation.getArgument(0);
      if ("cluster1".equals(cluster)) {
        cluster1Count.incrementAndGet();
      } else if ("cluster2".equals(cluster)) {
        cluster2Count.incrementAndGet();
      }
      return null;
    }).when(routine).execute(anyString());

    ClusterLeaderInitializationManager multiClusterManager = new ClusterLeaderInitializationManager(
        Collections.singletonList(routine),
        false, // concurrentInit
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    // First execute() - blocks
    multiClusterManager.execute("cluster1");
    assertEquals(cluster1Count.get(), 1, "Cluster1 should be initialized");
    assertEquals(cluster2Count.get(), 0, "Cluster2 should not be initialized yet");

    // Second execute() - async (doesn't block)
    multiClusterManager.execute("cluster2");
    Thread.sleep(100); // Wait for async execution
    assertEquals(cluster1Count.get(), 1, "Cluster1 count should not change");
    assertEquals(cluster2Count.get(), 1, "Cluster2 should be initialized");

    // Third execute() - async, cluster1 already initialized
    multiClusterManager.execute("cluster1");
    Thread.sleep(100);
    assertEquals(cluster1Count.get(), 1, "Cluster1 should not re-initialize");

    // Fourth execute() - async, cluster2 already initialized
    multiClusterManager.execute("cluster2");
    Thread.sleep(100);
    assertEquals(cluster2Count.get(), 1, "Cluster2 should not re-initialize");

    // Case 2: Empty routine list completes immediately
    ClusterLeaderInitializationManager emptyManager = new ClusterLeaderInitializationManager(
        Collections.emptyList(),
        false, // concurrentInit
        true, // blockUntilComplete
        TEST_LOG_CONTEXT);

    long startTime = System.currentTimeMillis();
    emptyManager.execute(TEST_CLUSTER);
    long executionTime = System.currentTimeMillis() - startTime;

    assertTrue(executionTime < 100, "Empty routine list should complete immediately");
  }
}
