package com.linkedin.venice.controller;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;


public class FabricControllerClientProviderTest {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";
  private static final int SENTINEL = -1;

  private static StoreResponse successStore(int currentVersion) {
    // StoreInfo#getCurrentVersion is final, so use a real StoreInfo/StoreResponse rather than mocks.
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setCurrentVersion(currentVersion);
    StoreResponse response = new StoreResponse();
    response.setStore(storeInfo);
    return response;
  }

  private static StoreResponse errorStore() {
    StoreResponse response = new StoreResponse();
    response.setError("boom");
    return response;
  }

  private static FabricControllerClientProvider providerWith(
      VeniceControllerMultiClusterConfig configs,
      Map<String, D2Client> d2Clients) {
    return new FabricControllerClientProvider(configs, Optional.empty(), d2Clients);
  }

  private static VeniceControllerClusterConfig clusterConfig(VeniceControllerMultiClusterConfig configs) {
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(configs.getControllerConfig(CLUSTER)).thenReturn(clusterConfig);
    return clusterConfig;
  }

  private static ThreadPoolExecutor executor(int workers, int queueCapacity) {
    return new ThreadPoolExecutor(workers, workers, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(queueCapacity));
  }

  private static FabricControllerClientProvider providerWith(ThreadPoolExecutor executor) {
    return new FabricControllerClientProvider(
        mock(VeniceControllerMultiClusterConfig.class),
        Optional.empty(),
        null,
        executor);
  }

  private static void awaitIdle(ThreadPoolExecutor executor) {
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertEquals(executor.getActiveCount(), 0);
      assertTrue(executor.getQueue().isEmpty());
    });
  }

  @Test(timeOut = 15_000)
  public void testCancellationRacingWorkerStartReleasesAdmissionExactlyOnce() throws Exception {
    CountDownLatch dequeued = new CountDownLatch(1);
    CountDownLatch allowRun = new CountDownLatch(1);
    CountDownLatch cancelledRunExited = new CountDownLatch(1);
    AtomicReference<Runnable> firstTask = new AtomicReference<>();
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(7, 7, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(256)) {
          @Override
          protected void beforeExecute(Thread thread, Runnable task) {
            if (firstTask.compareAndSet(null, task)) {
              dequeued.countDown();
              try {
                allowRun.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          }

          @Override
          protected void afterExecute(Runnable task, Throwable failure) {
            if (task == firstTask.get()) {
              cancelledRunExited.countDown();
            }
          }
        };
    CountDownLatch running = new CountDownLatch(6);
    CountDownLatch release = new CountDownLatch(1);
    ControllerClient stalled = mock(ControllerClient.class);
    when(stalled.getStoreWithDeadline(anyString(), anyLong())).thenAnswer(invocation -> {
      running.countDown();
      release.await();
      return successStore(1);
    });
    ControllerClient healthy = mock(ControllerClient.class);
    when(healthy.getStoreWithDeadline(anyString(), anyLong())).thenReturn(successStore(2));
    List<Thread> callers = new ArrayList<>();
    List<FutureTask<Map<String, Integer>>> queries = new ArrayList<>();
    try (FabricControllerClientProvider provider = providerWith(executor)) {
      FutureTask<Map<String, Integer>> firstQuery = new FutureTask<>(
          () -> provider.queryCurrentVersions(Collections.singletonMap("region", healthy), CLUSTER, STORE));
      Thread firstCaller = new Thread(firstQuery);
      callers.add(firstCaller);
      firstCaller.start();
      assertTrue(dequeued.await(5, TimeUnit.SECONDS));
      firstCaller.interrupt();
      assertEquals(firstQuery.get(5, TimeUnit.SECONDS).get("region").intValue(), SENTINEL);

      for (int i = 0; i < 6; i++) {
        FutureTask<Map<String, Integer>> query = new FutureTask<>(
            () -> provider.queryCurrentVersions(Collections.singletonMap("region", stalled), CLUSTER, STORE));
        queries.add(query);
        Thread caller = new Thread(query);
        callers.add(caller);
        caller.start();
      }
      assertTrue(running.await(5, TimeUnit.SECONDS), "Cancellation before run must return the original admission");
      allowRun.countDown();
      assertTrue(cancelledRunExited.await(5, TimeUnit.SECONDS));
      assertEquals(
          provider.queryCurrentVersions(Collections.singletonMap("region", healthy), CLUSTER, STORE)
              .get("region")
              .intValue(),
          SENTINEL,
          "A cancelled task subsequently entering run must not release another task's admission");
      verify(healthy, never()).getStoreWithDeadline(anyString(), anyLong());
      release.countDown();
      for (FutureTask<Map<String, Integer>> query: queries) {
        assertEquals(query.get(5, TimeUnit.SECONDS).get("region").intValue(), 1);
      }
      for (Thread caller: callers) {
        caller.join(5000);
      }
      awaitIdle(executor);
      assertEquals(
          provider.queryCurrentVersions(Collections.singletonMap("region", healthy), CLUSTER, STORE)
              .get("region")
              .intValue(),
          2);
    } finally {
      allowRun.countDown();
      release.countDown();
      callers.forEach(Thread::interrupt);
      for (Thread caller: callers) {
        caller.join(5000);
      }
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeOut = 15_000)
  public void testSixRegionAdmissionsSharedAcrossStoresAndClustersAndHeldUntilWorkerExit() throws Exception {
    CountDownLatch started = new CountDownLatch(6);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(6);
    ControllerClient stalled = mock(ControllerClient.class);
    when(stalled.getStoreWithDeadline(anyString(), anyLong())).thenAnswer(invocation -> {
      started.countDown();
      // Model I/O that has received cancellation but has not actually stopped.
      while (release.getCount() != 0) {
        try {
          release.await();
        } catch (InterruptedException e) {
          interrupted.countDown();
        }
      }
      return successStore(99);
    });
    ControllerClient healthy = mock(ControllerClient.class);
    when(healthy.getStoreWithDeadline(anyString(), anyLong())).thenReturn(successStore(7));
    ThreadPoolExecutor executor = executor(16, 256);
    List<Thread> callers = new ArrayList<>();
    List<FutureTask<Map<String, Integer>>> queries = new ArrayList<>();
    try (FabricControllerClientProvider provider = providerWith(executor)) {
      for (int i = 0; i < 6; i++) {
        String cluster = "cluster-" + i;
        String store = "store-" + i;
        FutureTask<Map<String, Integer>> query = new FutureTask<>(
            () -> provider.queryCurrentVersions(Collections.singletonMap("stalled", stalled), cluster, store));
        Thread caller = new Thread(query);
        queries.add(query);
        callers.add(caller);
        caller.start();
      }
      assertTrue(started.await(5, TimeUnit.SECONDS), "All six slots must be admitted");
      Map<String, ControllerClient> clients = new LinkedHashMap<>();
      clients.put("stalled", stalled);
      clients.put("healthy-a", healthy);
      clients.put("healthy-b", healthy);
      Map<String, Integer> result = provider.queryCurrentVersions(clients, "other-cluster", "other-store");
      assertEquals(result.get("stalled").intValue(), SENTINEL, "The seventh task must be rejected");
      assertEquals(result.get("healthy-a").intValue(), 7);
      assertEquals(result.get("healthy-b").intValue(), 7);

      callers.forEach(Thread::interrupt);
      for (FutureTask<Map<String, Integer>> query: queries) {
        assertEquals(query.get(5, TimeUnit.SECONDS).get("stalled").intValue(), SENTINEL);
      }
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      assertEquals(
          provider.queryCurrentVersions(Collections.singletonMap("stalled", healthy), CLUSTER, STORE)
              .get("stalled")
              .intValue(),
          SENTINEL,
          "Cancelled responses must not release permits while their workers are still executing");
      release.countDown();
      awaitIdle(executor);
      for (int i = 0; i < 12; i++) {
        assertEquals(
            provider.queryCurrentVersions(Collections.singletonMap("stalled", healthy), CLUSTER, STORE)
                .get("stalled")
                .intValue(),
            7,
            "Worker exit and normal completion must return admission");
        awaitIdle(executor);
      }
    } finally {
      release.countDown();
      callers.forEach(Thread::interrupt);
      for (Thread caller: callers) {
        caller.join(5000);
      }
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeOut = 15_000)
  public void testQueuedCancellationRejectionAndShutdownReturnAdmission() throws Exception {
    ThreadPoolExecutor executor = executor(1, 6);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    executor.execute(() -> {
      workerStarted.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    assertTrue(workerStarted.await(5, TimeUnit.SECONDS));
    ControllerClient client = mock(ControllerClient.class);
    when(client.getStoreWithDeadline(anyString(), anyLong())).thenReturn(successStore(8));
    List<Thread> callers = new ArrayList<>();
    List<FutureTask<Map<String, Integer>>> queries = new ArrayList<>();
    try (FabricControllerClientProvider provider = providerWith(executor)) {
      for (int i = 0; i < 6; i++) {
        FutureTask<Map<String, Integer>> query = new FutureTask<>(
            () -> provider.queryCurrentVersions(Collections.singletonMap("queued", client), CLUSTER, STORE));
        Thread caller = new Thread(query);
        queries.add(query);
        callers.add(caller);
        caller.start();
      }
      TestUtils
          .waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> assertEquals(executor.getQueue().size(), 6));
      assertEquals(
          provider.queryCurrentVersions(Collections.singletonMap("queued", client), CLUSTER, STORE)
              .get("queued")
              .intValue(),
          SENTINEL);
      assertEquals(executor.getQueue().size(), 6, "Regional rejection must not enqueue work");
      // This region has admission but is rejected by the full global queue. Repeat to expose permit leaks.
      for (int i = 0; i < 12; i++) {
        assertEquals(
            provider.queryCurrentVersions(Collections.singletonMap("rejected", client), CLUSTER, STORE)
                .get("rejected")
                .intValue(),
            SENTINEL);
      }
      verify(client, never()).getStoreWithDeadline(anyString(), anyLong());
      callers.forEach(Thread::interrupt);
      for (FutureTask<Map<String, Integer>> query: queries) {
        assertEquals(query.get(5, TimeUnit.SECONDS).get("queued").intValue(), SENTINEL);
      }
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> assertTrue(executor.getQueue().isEmpty()));
      release.countDown();
      awaitIdle(executor);
      for (String region: new String[] { "queued", "rejected" }) {
        for (int i = 0; i < 6; i++) {
          assertEquals(
              provider.queryCurrentVersions(Collections.singletonMap(region, client), CLUSTER, STORE)
                  .get(region)
                  .intValue(),
              8);
          awaitIdle(executor);
        }
      }
      provider.close();
      assertEquals(
          provider.queryCurrentVersions(Collections.singletonMap("queued", client), CLUSTER, STORE)
              .get("queued")
              .intValue(),
          SENTINEL);
    } finally {
      release.countDown();
      callers.forEach(Thread::interrupt);
      for (Thread caller: callers) {
        caller.join(5000);
      }
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeOut = 10_000)
  public void testExceptionalAndPreSubmissionExitsDoNotLeakAdmission() throws Exception {
    ControllerClient client = mock(ControllerClient.class);
    when(client.getStoreWithDeadline(anyString(), anyLong())).thenThrow(new IllegalStateException("unexpected"));
    ThreadPoolExecutor executor = executor(1, 6);
    try (FabricControllerClientProvider provider = providerWith(executor)) {
      Map<String, ControllerClient> clients = Collections.singletonMap("region", client);
      for (int i = 0; i < 12; i++) {
        assertThrows(IllegalStateException.class, () -> provider.queryCurrentVersions(clients, CLUSTER, STORE));
        awaitIdle(executor);
        assertEquals(
            provider.queryCurrentVersions(clients, CLUSTER, STORE, System.nanoTime()).get("region").intValue(),
            SENTINEL);
        try {
          Thread.currentThread().interrupt();
          assertEquals(provider.queryCurrentVersions(clients, CLUSTER, STORE).get("region").intValue(), SENTINEL);
          assertTrue(Thread.currentThread().isInterrupted());
        } finally {
          Thread.interrupted();
        }
      }
      doReturn(successStore(3)).when(client).getStoreWithDeadline(anyString(), anyLong());
      assertEquals(provider.queryCurrentVersions(clients, CLUSTER, STORE).get("region").intValue(), 3);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeOut = 10_000)
  public void testShutdownCancelsDrainedQueuedTasksAndUnblocksCaller() throws Exception {
    ThreadPoolExecutor executor = executor(1, 6);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    executor.execute(() -> {
      started.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    ControllerClient client = mock(ControllerClient.class);
    try (FabricControllerClientProvider provider = providerWith(executor)) {
      assertTrue(started.await(5, TimeUnit.SECONDS));
      FutureTask<Map<String, Integer>> query = new FutureTask<>(
          () -> provider.queryCurrentVersions(Collections.singletonMap("region", client), CLUSTER, STORE));
      Thread caller = new Thread(query);
      caller.start();
      try {
        TestUtils
            .waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> assertEquals(executor.getQueue().size(), 1));
        Future<?> queuedTask = (Future<?>) executor.getQueue().peek();
        provider.close();
        assertTrue(queuedTask.isCancelled(), "Draining a queue alone does not cancel its FutureTasks");
        assertEquals(query.get(5, TimeUnit.SECONDS).get("region").intValue(), SENTINEL);
        verify(client, never()).getStoreWithDeadline(anyString(), anyLong());
      } finally {
        caller.interrupt();
        caller.join(5000);
      }
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeOut = 10_000)
  public void testCurrentVersionDeadlineRetainsHealthyRegionsAndInterruptsSlowWorker() throws Exception {
    CountDownLatch slowStarted = new CountDownLatch(1);
    CountDownLatch healthyFinished = new CountDownLatch(1);
    CountDownLatch slowInterrupted = new CountDownLatch(1);
    ControllerClient slow = mock(ControllerClient.class);
    when(slow.getStoreWithDeadline(eq(STORE), anyLong())).thenAnswer(invocation -> {
      slowStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        slowInterrupted.countDown();
        Thread.currentThread().interrupt();
      }
      return successStore(99);
    });
    ControllerClient healthy = mock(ControllerClient.class);
    when(healthy.getStoreWithDeadline(eq(STORE), anyLong())).thenAnswer(invocation -> {
      assertTrue(slowStarted.await(5, TimeUnit.SECONDS), "Both regions must run concurrently");
      healthyFinished.countDown();
      return successStore(7);
    });
    Map<String, ControllerClient> clients = new LinkedHashMap<>();
    clients.put("slow", slow);
    clients.put("healthy", healthy);

    try (FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null)) {
      Map<String, Integer> result =
          provider.queryCurrentVersions(clients, CLUSTER, STORE, System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
      assertEquals(healthyFinished.getCount(), 0L);
      assertEquals(result.get("healthy").intValue(), 7);
      assertEquals(result.get("slow").intValue(), SENTINEL);
      assertTrue(slowInterrupted.await(5, TimeUnit.SECONDS));
      assertEquals(result.get("slow").intValue(), SENTINEL, "Late responses must not change the returned map");
    }
  }

  @Test
  public void testCurrentVersionsEmptyErrorsAndUnexpectedExceptions() {
    ControllerClient good = mock(ControllerClient.class);
    when(good.getStoreWithDeadline(eq(STORE), anyLong())).thenAnswer(invocation -> {
      long remainingNs = invocation.<Long>getArgument(1) - System.nanoTime();
      assertTrue(remainingNs > 0 && remainingNs <= TimeUnit.SECONDS.toNanos(15));
      return successStore(9);
    });
    ControllerClient bad = mock(ControllerClient.class);
    when(bad.getStoreWithDeadline(eq(STORE), anyLong())).thenReturn(errorStore());
    Map<String, ControllerClient> clients = new LinkedHashMap<>();
    clients.put("good", good);
    clients.put("bad", bad);
    try (FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null)) {
      assertTrue(provider.queryCurrentVersions(Collections.emptyMap(), CLUSTER, STORE).isEmpty());
      Map<String, Integer> result = provider.queryCurrentVersions(clients, CLUSTER, STORE);
      assertEquals(result.get("good").intValue(), 9);
      assertEquals(result.get("bad").intValue(), SENTINEL);
      when(bad.getStoreWithDeadline(eq(STORE), anyLong())).thenThrow(new IllegalStateException("unexpected"));
      assertThrows(IllegalStateException.class, () -> provider.queryCurrentVersions(clients, CLUSTER, STORE));
    }
  }

  @Test(timeOut = 10_000)
  public void testCurrentVersionsCallerInterruptionCancelsWork() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    AtomicBoolean callerInterrupted = new AtomicBoolean();
    ControllerClient client = mock(ControllerClient.class);
    when(client.getStoreWithDeadline(eq(STORE), anyLong())).thenAnswer(invocation -> {
      started.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        interrupted.countDown();
        Thread.currentThread().interrupt();
      }
      return errorStore();
    });
    try (FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null)) {
      FutureTask<Map<String, Integer>> query = new FutureTask<>(() -> {
        Map<String, Integer> result =
            provider.queryCurrentVersions(Collections.singletonMap("region", client), CLUSTER, STORE);
        callerInterrupted.set(Thread.currentThread().isInterrupted());
        return result;
      });
      Thread caller = new Thread(query);
      caller.start();
      try {
        assertTrue(started.await(5, TimeUnit.SECONDS));
        caller.interrupt();
        assertEquals(query.get(5, TimeUnit.SECONDS).get("region").intValue(), SENTINEL);
        assertTrue(callerInterrupted.get());
        assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      } finally {
        caller.interrupt();
        caller.join(5000);
      }
    }
  }

  @Test(timeOut = 10_000)
  public void testCurrentVersionsQueueingSharesDeadlineAndRejectsExcessWork() throws Exception {
    CountDownLatch running = new CountDownLatch(16);
    CountDownLatch interrupted = new CountDownLatch(16);
    ControllerClient slow = mock(ControllerClient.class);
    when(slow.getStoreWithDeadline(eq(STORE), anyLong())).thenAnswer(invocation -> {
      running.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        interrupted.countDown();
        Thread.currentThread().interrupt();
      }
      return errorStore();
    });
    ControllerClient queued = mock(ControllerClient.class);
    Map<String, ControllerClient> clients = new LinkedHashMap<>();
    for (int i = 0; i < 16; i++) {
      clients.put("running-" + i, slow);
    }
    // Fill the bounded queue and force explicit rejection of the last region.
    for (int i = 0; i < 257; i++) {
      clients.put("queued-" + i, queued);
    }
    try (FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null)) {
      Map<String, Integer> result =
          provider.queryCurrentVersions(clients, CLUSTER, STORE, System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
      assertEquals(running.getCount(), 0L);
      assertEquals(result.size(), clients.size());
      result.values().forEach(version -> assertEquals(version.intValue(), SENTINEL));
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      verify(queued, never()).getStoreWithDeadline(eq(STORE), anyLong());
      // Cancelled queued work must not prevent a subsequent healthy call from being admitted.
      when(queued.getStoreWithDeadline(eq(STORE), anyLong())).thenReturn(successStore(4));
      assertEquals(
          provider.queryCurrentVersions(Collections.singletonMap("healthy", queued), CLUSTER, STORE)
              .get("healthy")
              .intValue(),
          4);
    }
  }

  @Test
  public void testCurrentVersionsExpiredBudgetAndClosedProviderDoNotIssueRequests() {
    ControllerClient client = mock(ControllerClient.class);
    FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null);
    Map<String, ControllerClient> clients = Collections.singletonMap("region", client);
    try {
      assertEquals(
          provider.queryCurrentVersions(clients, CLUSTER, STORE, System.nanoTime()).get("region").intValue(),
          SENTINEL);
    } finally {
      provider.close();
    }
    assertEquals(provider.queryCurrentVersions(clients, CLUSTER, STORE).get("region").intValue(), SENTINEL);
    verify(client, never()).getStoreWithDeadline(eq(STORE), anyLong());
  }

  @Test
  public void testQueryAllRegionsNoRetryAggregatesPerRegion() {
    ControllerClient regionA = mock(ControllerClient.class);
    when(regionA.getStore(STORE)).thenReturn(successStore(3));
    ControllerClient regionB = mock(ControllerClient.class);
    when(regionB.getStore(STORE)).thenReturn(successStore(5));
    Map<String, ControllerClient> clients = new LinkedHashMap<>();
    clients.put("regionA", regionA);
    clients.put("regionB", regionB);

    FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null);
    Map<String, Integer> result = provider
        .queryAllRegions(clients, CLUSTER, 1, c -> c.getStore(STORE), r -> r.getStore().getCurrentVersion(), SENTINEL);

    assertEquals(result.size(), 2);
    assertEquals(result.get("regionA").intValue(), 3);
    assertEquals(result.get("regionB").intValue(), 5);
  }

  @Test
  public void testQueryAllRegionsStoresSentinelOnError() {
    ControllerClient good = mock(ControllerClient.class);
    when(good.getStore(STORE)).thenReturn(successStore(9));
    ControllerClient bad = mock(ControllerClient.class);
    when(bad.getStore(STORE)).thenReturn(errorStore());
    Map<String, ControllerClient> clients = new LinkedHashMap<>();
    clients.put("good", good);
    clients.put("bad", bad);

    FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null);
    Map<String, Integer> result = provider
        .queryAllRegions(clients, CLUSTER, 1, c -> c.getStore(STORE), r -> r.getStore().getCurrentVersion(), SENTINEL);

    assertEquals(result.get("good").intValue(), 9);
    assertEquals(result.get("bad").intValue(), SENTINEL);
  }

  @Test
  public void testQueryAllRegionsWithRetryUsesRetryableRequest() {
    // maxAttempts > 1 exercises the ControllerClient.retryableRequest branch; a successful response returns on the
    // first attempt, so no retry sleep is incurred.
    ControllerClient region = mock(ControllerClient.class);
    when(region.getStore(STORE)).thenReturn(successStore(4));
    Map<String, ControllerClient> clients = Collections.singletonMap("region", region);

    FabricControllerClientProvider provider = providerWith(mock(VeniceControllerMultiClusterConfig.class), null);
    Map<String, Integer> result = provider
        .queryAllRegions(clients, CLUSTER, 3, c -> c.getStore(STORE), r -> r.getStore().getCurrentVersion(), SENTINEL);

    assertEquals(result.get("region").intValue(), 4);
  }

  @Test
  public void testGetControllerClientMapBuildsAndCachesClients() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("urlRegion", "http://localhost:1234"));
    when(clusterConfig.getChildDataCenterControllerD2Map())
        .thenReturn(Collections.singletonMap("d2Region", "d2-zk-host"));
    when(clusterConfig.getD2ServiceName()).thenReturn("venice-controller");

    Map<String, D2Client> d2Clients = new HashMap<>();
    d2Clients.put("d2Region", mock(D2Client.class));
    FabricControllerClientProvider provider = providerWith(configs, d2Clients);

    Map<String, ControllerClient> map = provider.getControllerClientMap(CLUSTER);
    assertEquals(map.size(), 2);
    assertNotNull(map.get("urlRegion"));
    assertNotNull(map.get("d2Region"));
    // The per-cluster map is cached: the same instance is returned on a second call.
    assertSame(provider.getControllerClientMap(CLUSTER), map);
  }

  @Test
  public void testGetControllerClientMapWithoutInjectedD2Clients() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("urlRegion", "http://localhost:1234"));
    // Empty D2 map so the d2Clients==null branch is exercised without constructing a ZK-backed D2 client.
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());

    FabricControllerClientProvider provider = providerWith(configs, null);
    Map<String, ControllerClient> map = provider.getControllerClientMap(CLUSTER);
    assertEquals(map.size(), 1);
    assertNotNull(map.get("urlRegion"));
  }

  @Test
  public void testGetFabricBuildoutReturnsAllowlistedClient() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("dc-1", "http://localhost:1234"));
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());

    FabricControllerClientProvider provider = providerWith(configs, null);
    ControllerClient client = provider.getFabricBuildoutControllerClient(CLUSTER, "dc-1");
    assertNotNull(client);
    // An allowlisted fabric is served straight from the standard controller-client map.
    assertSame(provider.getControllerClientMap(CLUSTER).get("dc-1"), client);
  }

  @Test
  public void testGetFabricBuildoutBuildsD2ClientForNonAllowlistedFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildControllerD2ZkHost("dc-x")).thenReturn("dc-x-zk");
    when(clusterConfig.getD2ServiceName()).thenReturn("venice-controller");

    Map<String, D2Client> d2Clients = new HashMap<>();
    d2Clients.put("dc-x", mock(D2Client.class));
    FabricControllerClientProvider provider = providerWith(configs, d2Clients);

    ControllerClient client = provider.getFabricBuildoutControllerClient(CLUSTER, "dc-x");
    assertNotNull(client);
    // Cached in the non-allowlist fabric map on subsequent lookups.
    assertSame(provider.getFabricBuildoutControllerClient(CLUSTER, "dc-x"), client);
  }

  @Test
  public void testGetFabricBuildoutBuildsUrlClientForNonAllowlistedFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildControllerD2ZkHost("dc-y")).thenReturn("");
    when(clusterConfig.getD2ServiceName()).thenReturn("venice-controller");
    when(clusterConfig.getChildControllerUrl("dc-y")).thenReturn("http://localhost:5678");

    FabricControllerClientProvider provider = providerWith(configs, null);
    assertNotNull(provider.getFabricBuildoutControllerClient(CLUSTER, "dc-y"));
  }

  @Test
  public void testGetFabricBuildoutThrowsWhenNoConfigForFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = clusterConfig(configs);
    when(clusterConfig.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(clusterConfig.getChildControllerD2ZkHost("dc-z")).thenReturn("");
    when(clusterConfig.getD2ServiceName()).thenReturn("");
    when(clusterConfig.getChildControllerUrl("dc-z")).thenReturn("");

    FabricControllerClientProvider provider = providerWith(configs, null);
    assertThrows(VeniceException.class, () -> provider.getFabricBuildoutControllerClient(CLUSTER, "dc-z"));
  }
}
