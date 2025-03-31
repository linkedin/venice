package com.linkedin.venice.controller;

import static org.mockito.Mockito.*;

import com.linkedin.venice.controller.stats.DeadStoreStats;
import com.linkedin.venice.meta.Store;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DeadStoreStatsPreFetchTaskTest {
  private VeniceParentHelixAdmin mockAdmin;
  private DeadStoreStats mockStats;
  private Store mockStore;

  @BeforeClass
  public void setUp() {
    mockAdmin = mock(VeniceParentHelixAdmin.class);
    mockStats = mock(DeadStoreStats.class);
    mockStore = mock(Store.class);
    mockAdmin.deadStoreStats = mockStats;
    when(mockAdmin.getAllStores("test-cluster")).thenReturn(Collections.singletonList(mockStore));
  }

  @Test
  public void testInitialFetchIsCalled() throws InterruptedException {
    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask("test-cluster", mockAdmin, 1000);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    waitForAsyncExecution();

    verify(mockAdmin, atLeastOnce()).getAllStores("test-cluster");
    verify(mockStats, atLeastOnce()).preFetchStats(any(List.class));

    shutdownTask(task, executor);
  }

  @Test
  public void testFetchRepeatsUntilClosed() throws InterruptedException {
    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask("test-cluster", mockAdmin, 100);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    Thread.sleep(400);

    shutdownTask(task, executor);
    verify(mockStats, atLeast(2)).preFetchStats(any(List.class));
  }

  @Test
  public void testExceptionIsHandledGracefully() throws InterruptedException {
    doThrow(new RuntimeException("Simulated error")).when(mockStats).preFetchStats(any(List.class));

    DeadStoreStatsPreFetchTask task = new DeadStoreStatsPreFetchTask("test-cluster", mockAdmin, 100);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(task);
    Thread.sleep(300);

    shutdownTask(task, executor);
    verify(mockStats, atLeastOnce()).preFetchStats(any(List.class));
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
