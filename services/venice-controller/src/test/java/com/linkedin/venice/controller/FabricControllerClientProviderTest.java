package com.linkedin.venice.controller;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;


public class FabricControllerClientProviderTest {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";
  private static final int SENTINEL = -1;

  private static StoreResponse successStore(int currentVersion) {
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

  private static FabricControllerClientProvider provider() {
    return new FabricControllerClientProvider(mock(VeniceControllerMultiClusterConfig.class), Optional.empty(), null);
  }

  private static void awaitIdle(ThreadPoolExecutor executor) {
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertEquals(executor.getActiveCount(), 0);
      assertTrue(executor.getQueue().isEmpty());
    });
  }

  @Test(timeOut = 15_000)
  public void testRegionPoolsIsolateHealthyRegionsAndShareCapacityAcrossStoresAndClusters() throws Exception {
    CountDownLatch running = new CountDownLatch(6);
    CountDownLatch interrupted = new CountDownLatch(6);
    CountDownLatch release = new CountDownLatch(1);
    ControllerClient stalled = mock(ControllerClient.class);
    when(stalled.getStoreWithDeadline(anyString(), anyLong())).thenAnswer(invocation -> {
      running.countDown();
      // I/O can ignore cancellation; it must still occupy only this region's workers.
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
    List<Thread> callers = new ArrayList<>();
    List<FutureTask<Map<String, Integer>>> queries = new ArrayList<>();
    try (FabricControllerClientProvider provider = provider()) {
      ThreadPoolExecutor executor = provider.getCurrentVersionQueryExecutor("stalled");
      assertEquals(executor.getCorePoolSize(), 6);
      assertEquals(executor.getMaximumPoolSize(), 6);
      assertEquals(executor.getQueue().remainingCapacity(), 256);
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
      assertTrue(running.await(5, TimeUnit.SECONDS));
      callers.forEach(Thread::interrupt);
      for (FutureTask<Map<String, Integer>> query: queries) {
        assertEquals(query.get(5, TimeUnit.SECONDS).get("stalled").intValue(), SENTINEL);
      }
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      assertEquals(executor.getActiveCount(), 6, "Cancellation must not manufacture worker capacity");

      Map<String, ControllerClient> clients = new LinkedHashMap<>();
      clients.put("stalled", healthy);
      clients.put("healthy-a", healthy);
      clients.put("healthy-b", healthy);
      Map<String, Integer> result = provider.queryCurrentVersions(
          clients,
          "other-cluster",
          "other-store",
          System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
      assertEquals(result.get("stalled").intValue(), SENTINEL);
      assertEquals(result.get("healthy-a").intValue(), 7);
      assertEquals(result.get("healthy-b").intValue(), 7);
      assertTrue(executor.getQueue().isEmpty(), "Expired queued work must be purged");
      release.countDown();
      awaitIdle(executor);
      assertEquals(
          provider.queryCurrentVersions(Collections.singletonMap("stalled", healthy), CLUSTER, STORE)
              .get("stalled")
              .intValue(),
          7);
    } finally {
      release.countDown();
      callers.forEach(Thread::interrupt);
      for (Thread caller: callers) {
        caller.join(5000);
      }
    }
  }

  @Test(timeOut = 10_000)
  public void testFullRegionalQueueRejectsOnlyThatRegionAndRecoversAfterCancellation() throws Exception {
    ControllerClient client = mock(ControllerClient.class);
    when(client.getStoreWithDeadline(anyString(), anyLong())).thenReturn(successStore(8));
    CountDownLatch started = new CountDownLatch(6);
    CountDownLatch release = new CountDownLatch(1);
    try (FabricControllerClientProvider provider = provider()) {
      ThreadPoolExecutor executor = provider.getCurrentVersionQueryExecutor("busy");
      for (int i = 0; i < 6; i++) {
        executor.submit(() -> {
          started.countDown();
          release.await();
          return null;
        });
      }
      assertTrue(started.await(5, TimeUnit.SECONDS));
      List<Future<?>> queued = new ArrayList<>();
      for (int i = 0; i < 256; i++) {
        queued.add(executor.submit(() -> {}));
      }
      Map<String, ControllerClient> clients = new LinkedHashMap<>();
      clients.put("busy", client);
      clients.put("healthy", client);
      Map<String, Integer> result = provider.queryCurrentVersions(clients, CLUSTER, STORE);
      assertEquals(result.get("busy").intValue(), SENTINEL);
      assertEquals(result.get("healthy").intValue(), 8);
      assertEquals(executor.getQueue().size(), 256);
      queued.forEach(task -> task.cancel(true));
      executor.purge();

      // With space available, expiry must cancel and purge the actual queued Future, not just its response.
      assertEquals(
          provider
              .queryCurrentVersions(
                  Collections.singletonMap("busy", client),
                  CLUSTER,
                  STORE,
                  System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100))
              .get("busy")
              .intValue(),
          SENTINEL);
      assertTrue(executor.getQueue().isEmpty());
      release.countDown();
      awaitIdle(executor);
      assertEquals(
          provider.queryCurrentVersions(Collections.singletonMap("busy", client), CLUSTER, STORE)
              .get("busy")
              .intValue(),
          8);
    } finally {
      release.countDown();
    }
  }

  @Test(timeOut = 10_000)
  public void testDeadlineRetainsHealthyResponseAndInterruptsSlowIO() throws Exception {
    CountDownLatch slowStarted = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    ControllerClient slow = mock(ControllerClient.class);
    when(slow.getStoreWithDeadline(anyString(), anyLong())).thenAnswer(invocation -> {
      slowStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        interrupted.countDown();
        Thread.currentThread().interrupt();
      }
      return successStore(99);
    });
    ControllerClient healthy = mock(ControllerClient.class);
    when(healthy.getStoreWithDeadline(anyString(), anyLong())).thenAnswer(invocation -> {
      assertTrue(slowStarted.await(5, TimeUnit.SECONDS));
      return successStore(7);
    });
    Map<String, ControllerClient> clients = new LinkedHashMap<>();
    clients.put("slow", slow);
    clients.put("healthy", healthy);
    try (FabricControllerClientProvider provider = provider()) {
      Map<String, Integer> result =
          provider.queryCurrentVersions(clients, CLUSTER, STORE, System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
      assertEquals(result.get("healthy").intValue(), 7);
      assertEquals(result.get("slow").intValue(), SENTINEL);
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      awaitIdle(provider.getCurrentVersionQueryExecutor("slow"));
      assertEquals(result.get("slow").intValue(), SENTINEL, "Late I/O must not change returned results");
    }
  }

  @Test(timeOut = 10_000)
  public void testCallerInterruptionCancelsIOAndPreservesInterruptFlag() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    AtomicBoolean callerInterrupted = new AtomicBoolean();
    ControllerClient client = mock(ControllerClient.class);
    when(client.getStoreWithDeadline(anyString(), anyLong())).thenAnswer(invocation -> {
      started.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        interrupted.countDown();
        Thread.currentThread().interrupt();
      }
      return errorStore();
    });
    try (FabricControllerClientProvider provider = provider()) {
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
  public void testCloseCancelsQueuedWorkAndPreventsPoolCreation() throws Exception {
    CountDownLatch started = new CountDownLatch(6);
    ControllerClient client = mock(ControllerClient.class);
    try (FabricControllerClientProvider provider = provider()) {
      ThreadPoolExecutor executor = provider.getCurrentVersionQueryExecutor("region");
      ThreadPoolExecutor otherExecutor = provider.getCurrentVersionQueryExecutor("other-region");
      otherExecutor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      for (int i = 0; i < 6; i++) {
        executor.submit(() -> {
          started.countDown();
          new CountDownLatch(1).await();
          return null;
        });
      }
      assertTrue(started.await(5, TimeUnit.SECONDS));
      FutureTask<Map<String, Integer>> query = new FutureTask<>(
          () -> provider.queryCurrentVersions(Collections.singletonMap("region", client), CLUSTER, STORE));
      Thread caller = new Thread(query);
      caller.start();
      try {
        TestUtils
            .waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> assertEquals(executor.getQueue().size(), 1));
        Future<?> queued = (Future<?>) executor.getQueue().peek();
        provider.close();
        assertTrue(queued.isCancelled());
        assertEquals(query.get(5, TimeUnit.SECONDS).get("region").intValue(), SENTINEL);
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(otherExecutor.awaitTermination(5, TimeUnit.SECONDS), "Close must shut down every regional pool");
        assertThrows(RejectedExecutionException.class, () -> provider.getCurrentVersionQueryExecutor("new-region"));
        assertEquals(
            provider.queryCurrentVersions(Collections.singletonMap("new-region", client), CLUSTER, STORE)
                .get("new-region")
                .intValue(),
            SENTINEL);
        verify(client, never()).getStoreWithDeadline(anyString(), anyLong());
      } finally {
        caller.interrupt();
        caller.join(5000);
      }
    }
  }

  @Test(timeOut = 10_000)
  public void testConcurrentPoolCreationAndClose() throws Exception {
    FabricControllerClientProvider provider = provider();
    CountDownLatch start = new CountDownLatch(1);
    List<FutureTask<ThreadPoolExecutor>> creations = new ArrayList<>();
    List<Thread> callers = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      FutureTask<ThreadPoolExecutor> creation = new FutureTask<>(() -> {
        start.await();
        return provider.getCurrentVersionQueryExecutor("region");
      });
      creations.add(creation);
      Thread caller = new Thread(creation);
      callers.add(caller);
      caller.start();
    }
    try {
      start.countDown();
      ThreadPoolExecutor executor = creations.get(0).get(5, TimeUnit.SECONDS);
      for (FutureTask<ThreadPoolExecutor> creation: creations) {
        assertSame(creation.get(5, TimeUnit.SECONDS), executor);
      }
      CountDownLatch race = new CountDownLatch(1);
      FutureTask<ThreadPoolExecutor> creation = new FutureTask<>(() -> {
        race.await();
        try {
          return provider.getCurrentVersionQueryExecutor("racing-region");
        } catch (RejectedExecutionException e) {
          return null;
        }
      });
      Thread creator = new Thread(creation);
      callers.add(creator);
      creator.start();
      race.countDown();
      provider.close();
      ThreadPoolExecutor racedExecutor = creation.get(5, TimeUnit.SECONDS);
      if (racedExecutor != null) {
        assertTrue(racedExecutor.isShutdown());
      }
      assertTrue(executor.isShutdown());
      assertThrows(RejectedExecutionException.class, () -> provider.getCurrentVersionQueryExecutor("region"));
    } finally {
      provider.close();
      for (Thread caller: callers) {
        caller.interrupt();
        caller.join(5000);
      }
    }
  }

  @Test
  public void testEmptyErrorsExceptionsAndPreSubmissionExpiry() {
    ControllerClient client = mock(ControllerClient.class);
    when(client.getStoreWithDeadline(anyString(), anyLong())).thenAnswer(invocation -> {
      long remaining = invocation.<Long>getArgument(1) - System.nanoTime();
      assertTrue(remaining > 0 && remaining <= TimeUnit.SECONDS.toNanos(15));
      return errorStore();
    });
    Map<String, ControllerClient> clients = Collections.singletonMap("region", client);
    try (FabricControllerClientProvider provider = provider()) {
      assertTrue(provider.queryCurrentVersions(Collections.emptyMap(), CLUSTER, STORE).isEmpty());
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
      verify(client, never()).getStoreWithDeadline(anyString(), anyLong());
      assertEquals(provider.queryCurrentVersions(clients, CLUSTER, STORE).get("region").intValue(), SENTINEL);
      for (RuntimeException failure: new RuntimeException[] { new CancellationException(),
          new RejectedExecutionException() }) {
        doThrow(failure).when(client).getStoreWithDeadline(anyString(), anyLong());
        assertEquals(provider.queryCurrentVersions(clients, CLUSTER, STORE).get("region").intValue(), SENTINEL);
      }
      doThrow(new IllegalStateException("unexpected")).when(client).getStoreWithDeadline(anyString(), anyLong());
      assertThrows(IllegalStateException.class, () -> provider.queryCurrentVersions(clients, CLUSTER, STORE));
      doReturn(successStore(3)).when(client).getStoreWithDeadline(anyString(), anyLong());
      assertEquals(provider.queryCurrentVersions(clients, CLUSTER, STORE).get("region").intValue(), 3);
    }
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
    try (FabricControllerClientProvider provider = provider()) {
      Map<String, Integer> result = provider.queryAllRegions(
          clients,
          CLUSTER,
          1,
          c -> c.getStore(STORE),
          r -> r.getStore().getCurrentVersion(),
          SENTINEL);
      assertEquals(result.get("good").intValue(), 9);
      assertEquals(result.get("bad").intValue(), SENTINEL);
    }
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
    try (FabricControllerClientProvider provider = provider()) {
      Map<String, Integer> result = provider.queryAllRegions(
          clients,
          CLUSTER,
          1,
          c -> c.getStore(STORE),
          r -> r.getStore().getCurrentVersion(),
          SENTINEL);
      assertEquals(result.size(), 2);
      assertEquals(result.get("regionA").intValue(), 3);
      assertEquals(result.get("regionB").intValue(), 5);
    }
  }

  @Test
  public void testQueryAllRegionsWithRetryUsesRetryableRequest() {
    ControllerClient region = mock(ControllerClient.class);
    when(region.getStore(STORE)).thenReturn(successStore(4));
    try (FabricControllerClientProvider provider = provider()) {
      Map<String, Integer> result = provider.queryAllRegions(
          Collections.singletonMap("region", region),
          CLUSTER,
          3,
          c -> c.getStore(STORE),
          r -> r.getStore().getCurrentVersion(),
          SENTINEL);
      assertEquals(result.get("region").intValue(), 4);
    }
  }

  @Test
  public void testGetControllerClientMapBuildsAndCachesClients() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = mock(VeniceControllerClusterConfig.class);
    when(configs.getControllerConfig(CLUSTER)).thenReturn(config);
    when(config.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("url-region", "http://localhost:1234"));
    when(config.getChildDataCenterControllerD2Map()).thenReturn(Collections.singletonMap("d2-region", "zk-host"));
    when(config.getD2ServiceName()).thenReturn("venice-controller");
    Map<String, D2Client> d2Clients = new HashMap<>();
    d2Clients.put("d2-region", mock(D2Client.class));
    try (FabricControllerClientProvider provider =
        new FabricControllerClientProvider(configs, Optional.empty(), d2Clients)) {
      Map<String, ControllerClient> clients = provider.getControllerClientMap(CLUSTER);
      assertEquals(clients.size(), 2);
      assertNotNull(clients.get("url-region"));
      assertNotNull(clients.get("d2-region"));
      assertSame(provider.getControllerClientMap(CLUSTER), clients);
      ControllerClient standard = mock(ControllerClient.class);
      clients.put("mock-region", standard);
      provider.close();
      verify(standard).close();
    }
  }

  @Test
  public void testGetControllerClientMapWithoutInjectedD2Clients() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = mock(VeniceControllerClusterConfig.class);
    when(configs.getControllerConfig(CLUSTER)).thenReturn(config);
    when(config.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("url-region", "http://localhost:1234"));
    when(config.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    try (
        FabricControllerClientProvider provider = new FabricControllerClientProvider(configs, Optional.empty(), null)) {
      assertEquals(provider.getControllerClientMap(CLUSTER).size(), 1);
    }
  }

  private static VeniceControllerClusterConfig clusterConfig(VeniceControllerMultiClusterConfig configs) {
    VeniceControllerClusterConfig config = mock(VeniceControllerClusterConfig.class);
    when(configs.getControllerConfig(CLUSTER)).thenReturn(config);
    return config;
  }

  @Test
  public void testGetFabricBuildoutReturnsAllowlistedClient() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = clusterConfig(configs);
    when(config.getChildDataCenterControllerUrlMap())
        .thenReturn(Collections.singletonMap("dc-1", "http://localhost:1234"));
    when(config.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    try (
        FabricControllerClientProvider provider = new FabricControllerClientProvider(configs, Optional.empty(), null)) {
      ControllerClient client = provider.getFabricBuildoutControllerClient(CLUSTER, "dc-1");
      assertNotNull(client);
      assertSame(provider.getControllerClientMap(CLUSTER).get("dc-1"), client);
    }
  }

  @Test
  public void testGetFabricBuildoutBuildsD2ClientForNonAllowlistedFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = clusterConfig(configs);
    when(config.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(config.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(config.getChildControllerD2ZkHost("dc-x")).thenReturn("dc-x-zk");
    when(config.getD2ServiceName()).thenReturn("venice-controller");
    Map<String, D2Client> d2Clients = Collections.singletonMap("dc-x", mock(D2Client.class));
    try (FabricControllerClientProvider provider =
        new FabricControllerClientProvider(configs, Optional.empty(), d2Clients)) {
      ControllerClient client = provider.getFabricBuildoutControllerClient(CLUSTER, "dc-x");
      assertNotNull(client);
      assertSame(provider.getFabricBuildoutControllerClient(CLUSTER, "dc-x"), client);
    }
  }

  @Test
  public void testGetFabricBuildoutBuildsUrlClientForNonAllowlistedFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = clusterConfig(configs);
    when(config.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(config.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(config.getChildControllerD2ZkHost("dc-y")).thenReturn("");
    when(config.getD2ServiceName()).thenReturn("venice-controller");
    when(config.getChildControllerUrl("dc-y")).thenReturn("http://localhost:5678");
    try (
        FabricControllerClientProvider provider = new FabricControllerClientProvider(configs, Optional.empty(), null)) {
      ControllerClient client = provider.getFabricBuildoutControllerClient(CLUSTER, "dc-y");
      assertNotNull(client);
      assertSame(provider.getFabricBuildoutControllerClient(CLUSTER, "dc-y"), client);
    }
  }

  @Test
  public void testGetFabricBuildoutThrowsWhenNoConfigForFabric() {
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = clusterConfig(configs);
    when(config.getChildDataCenterControllerUrlMap()).thenReturn(Collections.emptyMap());
    when(config.getChildDataCenterControllerD2Map()).thenReturn(Collections.emptyMap());
    when(config.getChildControllerD2ZkHost("dc-z")).thenReturn("");
    when(config.getD2ServiceName()).thenReturn("");
    when(config.getChildControllerUrl("dc-z")).thenReturn("");
    try (
        FabricControllerClientProvider provider = new FabricControllerClientProvider(configs, Optional.empty(), null)) {
      assertThrows(VeniceException.class, () -> provider.getFabricBuildoutControllerClient(CLUSTER, "dc-z"));
    }
  }
}
