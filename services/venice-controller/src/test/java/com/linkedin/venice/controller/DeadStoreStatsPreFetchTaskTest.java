package com.linkedin.venice.controller;

import static org.mockito.Mockito.*;

import com.linkedin.venice.meta.Store;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DeadStoreStatsPreFetchTaskTest {
  private static final String CLUSTER_NAME = "test-cluster";
  private VeniceHelixAdmin mockAdmin;
  private Store mockStore;

  @BeforeClass
  public void setUp() {
    mockAdmin = mock(VeniceHelixAdmin.class);
    mockStore = mock(Store.class);
    when(mockAdmin.getAllStores(CLUSTER_NAME)).thenReturn(Collections.singletonList(mockStore));
    when(mockAdmin.isLeaderControllerFor(CLUSTER_NAME)).thenReturn(true);
  }

  @Test
  public void testBecomesLeaderAfterRetries() throws InterruptedException {
    // Simulate: false 1st check, true on 2nd check (~10 sec delay)
    when(mockAdmin.isLeaderControllerFor(CLUSTER_NAME)).thenReturn(false).thenReturn(true);

    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask(CLUSTER_NAME, mockAdmin, 1000);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);

    // Wait enough time for two isLeaderControllerFor() calls (~10s + small buffer)
    Thread.sleep(13_000);

    shutdownTask(task, executor);

    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq(CLUSTER_NAME), anyList());
    verify(mockAdmin, atLeast(2)).isLeaderControllerFor(CLUSTER_NAME);
  }

  @Test
  public void testInitialFetchIsCalled() throws InterruptedException {
    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask(CLUSTER_NAME, mockAdmin, 1000);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    waitForAsyncExecution();

    verify(mockAdmin, atLeastOnce()).getAllStores(CLUSTER_NAME);
    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq(CLUSTER_NAME), anyList());

    shutdownTask(task, executor);
  }

  @Test
  public void testFullRunLifecycle() throws InterruptedException {
    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask(CLUSTER_NAME, mockAdmin, 200);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);

    // Wait a bit longer than 1 cycle
    Thread.sleep(500);

    shutdownTask(task, executor);

    // 1 call should happen immediately (before loop)
    // at least 1 additional call should happen from loop
    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq(CLUSTER_NAME), anyList());
    verify(mockAdmin, atLeast(2)).getAllStores(CLUSTER_NAME);
  }

  @Test
  public void testFetchRepeatsUntilClosed() throws InterruptedException {
    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask(CLUSTER_NAME, mockAdmin, 100);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    Thread.sleep(400);

    shutdownTask(task, executor);
    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq(CLUSTER_NAME), anyList());
  }

  @Test
  public void testExceptionIsHandledGracefully() throws InterruptedException {
    doThrow(new RuntimeException("Simulated error")).when(mockAdmin)
        .preFetchDeadStoreStats(eq(CLUSTER_NAME), anyList());

    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask(CLUSTER_NAME, mockAdmin, 100);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    Thread.sleep(300);

    shutdownTask(task, executor);
    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq(CLUSTER_NAME), anyList());
  }

  private void waitForAsyncExecution() throws InterruptedException {
    Thread.sleep(300);
  }

  private void shutdownTask(DeadStoreStatsPreFetchTask task, ExecutorService executor) throws InterruptedException {
    task.close();
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }
}
