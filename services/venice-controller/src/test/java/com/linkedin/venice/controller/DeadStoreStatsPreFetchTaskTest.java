package com.linkedin.venice.controller;

import static org.mockito.Mockito.*;

import com.linkedin.venice.controller.stats.DeadStoreStats;
import com.linkedin.venice.meta.Store;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DeadStoreStatsPreFetchTaskTest {
  private VeniceHelixAdmin mockAdmin;
  private DeadStoreStats mockStats;
  private Store mockStore;

  @BeforeClass
  public void setUp() {
    mockAdmin = mock(VeniceHelixAdmin.class);
    mockStats = mock(DeadStoreStats.class);
    mockStore = mock(Store.class);
    mockAdmin.deadStoreStats = mockStats;
    when(mockAdmin.getAllStores("test-cluster")).thenReturn(Collections.singletonList(mockStore));
    when(mockAdmin.isLeaderControllerFor("test-cluster")).thenReturn(true);
  }

  @Test
  public void testBecomesLeaderAfterRetries() throws InterruptedException {
    // Simulate: false 1st check, true on 2nd check (~10 sec delay)
    when(mockAdmin.isLeaderControllerFor("test-cluster")).thenReturn(false).thenReturn(true);

    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask("test-cluster", mockAdmin, 1000);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);

    // Wait enough time for two isLeaderControllerFor() calls (~10s + small buffer)
    Thread.sleep(13_000);

    shutdownTask(task, executor);

    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq("test-cluster"), anyList());
    verify(mockAdmin, atLeast(2)).isLeaderControllerFor("test-cluster");
  }

  @Test
  public void testInitialFetchIsCalled() throws InterruptedException {
    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask("test-cluster", mockAdmin, 1000);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    waitForAsyncExecution();

    verify(mockAdmin, atLeastOnce()).getAllStores("test-cluster");
    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq("test-cluster"), anyList());

    shutdownTask(task, executor);
  }

  @Test
  public void testFullRunLifecycle() throws InterruptedException {
    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask("test-cluster", mockAdmin, 200);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);

    // Wait a bit longer than 1 cycle
    Thread.sleep(500);

    shutdownTask(task, executor);

    // 1 call should happen immediately (before loop)
    // at least 1 additional call should happen from loop
    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq("test-cluster"), anyList());
    verify(mockAdmin, atLeast(2)).getAllStores("test-cluster");
  }

  @Test
  public void testFetchRepeatsUntilClosed() throws InterruptedException {
    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask("test-cluster", mockAdmin, 100);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    Thread.sleep(400);

    shutdownTask(task, executor);
    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq("test-cluster"), anyList());
  }

  @Test
  public void testExceptionIsHandledGracefully() throws InterruptedException {
    doThrow(new RuntimeException("Simulated error")).when(mockAdmin)
        .preFetchDeadStoreStats(eq("test-cluster"), anyList());

    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask("test-cluster", mockAdmin, 100);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    Thread.sleep(300);

    shutdownTask(task, executor);
    verify(mockAdmin, atLeastOnce()).preFetchDeadStoreStats(eq("test-cluster"), anyList());
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
