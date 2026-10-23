package com.linkedin.venice.fastclient.meta;

import static org.apache.hc.core5.http.HttpStatus.SC_GONE;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;


public class InstanceHealthMonitorTest {
  private final static String instance = "https://test.host:1234";

  /**
   * ResourceRegistry.globalShutdown() runs from a JVM shutdown hook and is process wide, so it runs in a child JVM.
   */
  @Test
  public void testTimerSurvivesGlobalShutdownInSeparateProcess() throws Exception {
    Set<String> classpath = new LinkedHashSet<>();
    for (ClassLoader loader = getClass().getClassLoader(); loader != null; loader = loader.getParent()) {
      if (loader instanceof URLClassLoader) {
        for (URL url: ((URLClassLoader) loader).getURLs()) {
          classpath.add(new File(url.toURI()).getPath());
        }
      }
    }
    classpath.add(System.getProperty("java.class.path"));
    Process process = new ProcessBuilder(
        new File(System.getProperty("java.home"), "bin/java").getPath(),
        "-cp",
        String.join(File.pathSeparator, classpath),
        getClass().getName()).inheritIO().start();
    try {
      assertTrue(process.waitFor(45, TimeUnit.SECONDS), "Isolated monitor shutdown regression timed out");
      assertEquals(process.exitValue(), 0, "Isolated monitor shutdown regression failed");
    } finally {
      process.destroyForcibly();
    }
  }

  /** Child JVM for {@link #testTimerSurvivesGlobalShutdownInSeparateProcess()}. */
  public static void main(String[] args) throws Exception {
    InstanceHealthMonitor monitor = new InstanceHealthMonitor(
        InstanceHealthMonitorConfig.builder()
            .setRoutingRequestDefaultTimeoutMS(1)
            .setRoutingTimedOutRequestCounterResetDelayMS(0)
            .build());
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      // Hold the timer thread so the request timeout is still queued when global shutdown runs.
      monitor.getTimeoutProcessor().execute(() -> {
        started.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      CompletableFuture<TransportClientResponse> transport = new CompletableFuture<>();
      ChainedCompletableFuture<Integer, Integer> request =
          monitor.trackHealthBasedOnRequestToInstance(instance, transport);
      ResourceRegistry.globalShutdown();
      assertFalse(transport.isDone());
      release.countDown();
      // The queued timeout still fires, and the delayed counter reset still runs, after global shutdown.
      expectThrows(ExecutionException.class, () -> transport.get(10, TimeUnit.SECONDS));
      request.getOriginalFuture().complete(SC_GONE);
      request.getResultFuture().get(10, TimeUnit.SECONDS);
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> assertEquals(monitor.getPendingRequestCounter(instance), 0));
    } finally {
      release.countDown();
      monitor.close();
      RedundantExceptionFilter.getRedundantExceptionFilter().shutdown();
    }
    assertThrows(RejectedExecutionException.class, () -> monitor.getTimeoutProcessor().execute(() -> {}));
  }

  @Test
  public void testFailedHeartbeatSchedulingReleasesTimer() {
    TimeoutProcessor timer = mock(TimeoutProcessor.class);
    try (MockedStatic<TimeoutProcessor> timers = mockStatic(TimeoutProcessor.class)) {
      timers.when(() -> TimeoutProcessor.createManaged(true, 1)).thenReturn(timer);
      assertThrows(
          IllegalArgumentException.class,
          () -> new InstanceHealthMonitor(
              InstanceHealthMonitorConfig.builder().setHeartBeatIntervalSeconds(0).build()));
      verify(timer).shutdownNow();
    }
  }

  /**
   * Closing the metadata closes its monitor; closing again (from metadata or directly) is harmless.
   */
  @Test
  public void testCloseIsIdempotentAndDrainsLateCompletions() throws Exception {
    InstanceHealthMonitor monitor = new InstanceHealthMonitor(InstanceHealthMonitorConfig.builder().build());
    ClientConfig clientConfig = mock(ClientConfig.class);
    when(clientConfig.getClientRoutingStrategyType()).thenReturn(ClientRoutingStrategyType.LEAST_LOADED);
    when(clientConfig.getStoreName()).thenReturn("test-store");
    AbstractStoreMetadata metadata = mock(
        AbstractStoreMetadata.class,
        withSettings().useConstructor(clientConfig, monitor).defaultAnswer(CALLS_REAL_METHODS));
    ChainedCompletableFuture<Integer, Integer> request =
        monitor.trackHealthBasedOnRequestToInstance(instance, new CompletableFuture<>());
    try {
      Thread.currentThread().interrupt();
      metadata.close();
      assertTrue(Thread.currentThread().isInterrupted(), "close must preserve the caller's interrupt");
    } finally {
      Thread.interrupted();
    }
    assertThrows(RejectedExecutionException.class, () -> monitor.getTimeoutProcessor().execute(() -> {}));
    metadata.close();
    monitor.close();

    // A late timeout completion must not schedule on the stopped timer, and still releases its counter.
    request.getOriginalFuture().complete(SC_GONE);
    request.getResultFuture().get(10, TimeUnit.SECONDS);
    assertEquals(monitor.getPendingRequestCounter(instance), 0);
    assertThrows(IllegalStateException.class, () -> monitor.trackHealthBasedOnRequestToInstance(instance));
  }

  /**
   * close() can stop the timer after a caller passed the closed check. A rejected schedule must fail the new request
   * without leaking its pending counter, and a timed-out completion must reset its counter immediately.
   */
  @Test
  public void testTimerRejectionAfterClosedCheck() throws Exception {
    TimeoutProcessor timer = mock(TimeoutProcessor.class);
    when(timer.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .thenThrow(new RejectedExecutionException("timer stopped"));
    try (MockedStatic<TimeoutProcessor> timers = mockStatic(TimeoutProcessor.class)) {
      timers.when(() -> TimeoutProcessor.createManaged(true, 1)).thenReturn(timer);
      try (InstanceHealthMonitor monitor = new InstanceHealthMonitor(InstanceHealthMonitorConfig.builder().build())) {
        // No transport future, so tracking does not schedule a timeout.
        ChainedCompletableFuture<Integer, Integer> inFlight = monitor.trackHealthBasedOnRequestToInstance(instance);
        assertEquals(monitor.getPendingRequestCounter(instance), 1);

        assertThrows(
            IllegalStateException.class,
            () -> monitor.trackHealthBasedOnRequestToInstance(instance, new CompletableFuture<>()));
        assertEquals(monitor.getPendingRequestCounter(instance), 1);

        inFlight.getOriginalFuture().complete(SC_GONE);
        inFlight.getResultFuture().get(10, TimeUnit.SECONDS);
        assertEquals(monitor.getPendingRequestCounter(instance), 0);
      }
    }
  }

  /** Requests racing with close() fail only with IllegalStateException, never with RejectedExecutionException. */
  @Test(timeOut = 60_000)
  public void testRequestsRacingWithClose() throws Exception {
    InstanceHealthMonitor monitor = new InstanceHealthMonitor(InstanceHealthMonitorConfig.builder().build());
    int threadCount = 8;
    CountDownLatch warmedUp = new CountDownLatch(threadCount);
    AtomicReference<Throwable> unexpected = new AtomicReference<>();
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      int status = i % 2 == 0 ? SC_OK : SC_GONE;
      threads[i] = new Thread(() -> {
        try {
          for (int n = 0;; n++) {
            ChainedCompletableFuture<Integer, Integer> request =
                monitor.trackHealthBasedOnRequestToInstance(instance, new CompletableFuture<>());
            request.getOriginalFuture().complete(status);
            request.getResultFuture().join();
            if (n == 100) {
              warmedUp.countDown();
            }
          }
        } catch (IllegalStateException expected) {
          // Closed.
        } catch (Throwable t) {
          unexpected.compareAndSet(null, t);
        } finally {
          warmedUp.countDown();
        }
      });
      threads[i].start();
    }
    assertTrue(warmedUp.await(30, TimeUnit.SECONDS));
    monitor.close();
    for (Thread thread: threads) {
      thread.join(TimeUnit.SECONDS.toMillis(30));
      assertFalse(thread.isAlive());
    }
    assertEquals(unexpected.get(), null);
  }

  @Test
  public void testHeartbeatProducerStopsBeforeTimer() throws Exception {
    Client client = mock(Client.class);
    CountDownLatch heartbeatStarted = new CountDownLatch(1);
    CompletableFuture<Void> heartbeatStoppedWithTimerAlive = new CompletableFuture<>();
    AtomicReference<InstanceHealthMonitor> monitorRef = new AtomicReference<>();
    doAnswer(invocation -> {
      heartbeatStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        try {
          CompletableFuture<Void> timerAlive = new CompletableFuture<>();
          monitorRef.get().getTimeoutProcessor().execute(() -> timerAlive.complete(null));
          timerAlive.get(10, TimeUnit.SECONDS);
          heartbeatStoppedWithTimerAlive.complete(null);
        } catch (Throwable failure) {
          heartbeatStoppedWithTimerAlive.completeExceptionally(failure);
        } finally {
          Thread.currentThread().interrupt();
        }
      }
      return null;
    }).when(client).restRequest(any(RestRequest.class), any(Callback.class));
    try (InstanceHealthMonitor monitor = new InstanceHealthMonitor(
        InstanceHealthMonitorConfig.builder()
            .setClient(client)
            .setHeartBeatIntervalSeconds(1)
            .setRoutingRequestDefaultTimeoutMS(1)
            .build())) {
      monitorRef.set(monitor);
      CompletableFuture<TransportClientResponse> transport = new CompletableFuture<>();
      ChainedCompletableFuture<Integer, Integer> request =
          monitor.trackHealthBasedOnRequestToInstance(instance, transport);
      expectThrows(ExecutionException.class, () -> transport.get(10, TimeUnit.SECONDS));
      assertTrue(heartbeatStarted.await(10, TimeUnit.SECONDS));
      monitor.close();
      heartbeatStoppedWithTimerAlive.get(10, TimeUnit.SECONDS);
      request.getOriginalFuture().complete(SC_GONE);
      request.getResultFuture().get(10, TimeUnit.SECONDS);
      assertThrows(RejectedExecutionException.class, () -> monitor.getTimeoutProcessor().execute(() -> {}));
    }
  }

  @Test
  public void testPendingRequestCounterWithSuccessfulRequest() throws Exception {
    InstanceHealthMonitorConfig config =
        InstanceHealthMonitorConfig.builder().setRoutingRequestDefaultTimeoutMS(10000l).build();

    try (InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(config)) {
      ChainedCompletableFuture<Integer, Integer> chainedFuture =
          healthMonitor.trackHealthBasedOnRequestToInstance(instance);
      assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
      chainedFuture.getOriginalFuture().complete(SC_OK);
      waitQuietly(chainedFuture.getResultFuture());
      assertEquals(healthMonitor.getPendingRequestCounter(instance), 0);

      chainedFuture = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
      assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
      chainedFuture.getOriginalFuture().complete(SC_NOT_FOUND);
      waitQuietly(chainedFuture.getResultFuture());
      assertEquals(healthMonitor.getPendingRequestCounter(instance), 0);
    }
  }

  @Test
  public void testIndependentStoreRefreshPreservesHealthCountersAndOverloadHistory() throws Exception {
    Client client = mock(Client.class);
    doAnswer(invocation -> {
      Callback<RestResponse> callback = invocation.getArgument(1);
      callback.onError(new VeniceClientException("heartbeat failed"));
      return null;
    }).when(client).restRequest(any(RestRequest.class), any(Callback.class));
    InstanceHealthMonitorConfig config = InstanceHealthMonitorConfig.builder()
        .setClient(client)
        .setHeartBeatIntervalSeconds(1)
        .setRoutingRequestDefaultTimeoutMS(1)
        .setLoadControllerEnabled(true)
        .setLoadControllerRejectionRatioUpdateIntervalInSec(0)
        .build();
    try (InstanceHealthMonitor first = new InstanceHealthMonitor(config);
        InstanceHealthMonitor second = new InstanceHealthMonitor(config)) {
      String secondHost = "https://second.store:1234";
      first.updateLiveInstanceSet(Collections.singleton(instance));
      second.updateLiveInstanceSet(Collections.singleton(secondHost));
      CompletableFuture<TransportClientResponse> timedOut = new CompletableFuture<>();
      ChainedCompletableFuture<Integer, Integer> request =
          first.trackHealthBasedOnRequestToInstance(instance, timedOut);
      expectThrows(ExecutionException.class, () -> timedOut.get(10, TimeUnit.SECONDS));
      // A refresh for the other store must not erase suspicion before the heartbeat runs.
      second.updateLiveInstanceSet(Collections.singleton(secondHost));
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        assertFalse(first.isInstanceHealthy(instance));
        assertEquals(first.getUnhealthyInstanceCount(), 1);
      });
      request.getOriginalFuture().complete(HttpConstants.SC_SERVICE_OVERLOADED);
      request.getResultFuture().get(10, TimeUnit.SECONDS);
      assertEquals(first.getPendingRequestCounter(instance), 0);
      assertTrue(first.hasPendingRequestCounter(instance));
      double rejectionRatio = first.getRejectionRatio(instance);
      assertTrue(rejectionRatio > 0, "Real overload responses must leave load-controller history");

      ChainedCompletableFuture<Integer, Integer> inFlight = first.trackHealthBasedOnRequestToInstance(secondHost);
      second.updateLiveInstanceSet(Collections.singleton(secondHost));
      assertFalse(first.isInstanceHealthy(instance), "Other-store refresh cannot erase unhealthy state");
      assertTrue(first.hasPendingRequestCounter(instance), "Other-store refresh cannot erase drained counters");
      assertEquals(first.getRejectionRatio(instance), rejectionRatio);
      assertEquals(first.getPendingRequestCounter(secondHost), 1);
      assertEquals(second.getPendingRequestCounter(secondHost), 0, "Even overlapping replicas have independent counts");
      assertEquals(second.getUnhealthyInstanceCount(), 0);
      assertEquals(second.getOverloadedInstanceCount(), 0);

      // A real same-store departure must still prune health and load-controller state.
      first.updateLiveInstanceSet(Collections.singleton(secondHost));
      assertTrue(first.isInstanceHealthy(instance));
      assertFalse(first.hasPendingRequestCounter(instance));
      assertEquals(first.getRejectionRatio(instance), 0.0);
      assertEquals(first.getPendingRequestCounter(secondHost), 1, "Nonzero counters survive reconciliation");
      inFlight.getOriginalFuture().complete(SC_OK);
      inFlight.getResultFuture().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testPendingRequestCounterWithTooManyPendingRequests() throws Exception {
    int instanceBlockingThreshold = 10;
    InstanceHealthMonitorConfig config = InstanceHealthMonitorConfig.builder()
        .setRoutingRequestDefaultTimeoutMS(10000l)
        .setRoutingPendingRequestCounterInstanceBlockThreshold(instanceBlockingThreshold)
        .build();
    try (InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(config)) {
      ChainedCompletableFuture<Integer, Integer> chainedRequestFuture = null;
      for (int i = 0; i < 10; ++i) {
        chainedRequestFuture = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
      }
      assertTrue(healthMonitor.isInstanceBlocked(instance));
      assertEquals(healthMonitor.getPendingRequestCounter(instance), instanceBlockingThreshold);
      // Finish one request
      chainedRequestFuture.getOriginalFuture().complete(SC_OK);
      waitQuietly(chainedRequestFuture.getResultFuture());
      assertFalse(healthMonitor.isInstanceBlocked(instance));
      assertEquals(healthMonitor.getPendingRequestCounter(instance), instanceBlockingThreshold - 1);
    }
  }

  private static class MockClient implements Client {
    private final Map<String, Long> requestPathToResponseDelayMap;
    private final Map<String, CompletableFuture<RestResponse>> requestPathToResponseFutureMap;
    private final static ScheduledExecutorService SCHEDULER =
        Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("Mock_request_timeout"));

    public MockClient(
        Map<String, Long> requestPathToResponseDelayMap,
        Map<String, CompletableFuture<RestResponse>> requestPathToResponseFutureMap) {
      this.requestPathToResponseDelayMap = requestPathToResponseDelayMap;
      this.requestPathToResponseFutureMap = requestPathToResponseFutureMap;
    }

    @Override
    public Future<RestResponse> restRequest(RestRequest request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<RestResponse> restRequest(RestRequest request, RequestContext requestContext) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void restRequest(RestRequest request, Callback<RestResponse> callback) {
      String requestUri = request.getURI().toString();
      if (!requestPathToResponseFutureMap.containsKey(requestUri)) {
        throw new VeniceClientException("Unknown request path: " + requestUri);
      }
      CompletableFuture<RestResponse> future = requestPathToResponseFutureMap.get(requestUri);
      Long delay = requestPathToResponseDelayMap.get(requestUri);
      if (delay == null) {
        delay = 0l;
      }
      SCHEDULER.schedule(() -> {
        future.whenComplete((result, error) -> {
          if (error != null) {
            callback.onError(error);
          } else {
            callback.onSuccess(result);
          }
        });
      }, delay, TimeUnit.SECONDS);
    }

    @Override
    public void restRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown(Callback<None> callback) {
      // Do nothing
    }
  }

  @Test
  public void testUnhealthyNodeDetectedByHB() throws Exception {
    int instanceBlockingThreshold = 10;

    Map<String, Long> requestPathToResponseDelayMap = new VeniceConcurrentHashMap<>();
    Map<String, CompletableFuture<RestResponse>> requestPathToResponseFutureMap = new VeniceConcurrentHashMap<>();
    CompletableFuture<RestResponse> hbResponseFuture =
        CompletableFuture.completedFuture(new RestResponseBuilder().setStatus(SC_OK).build());
    String hbPath = instance + "/" + QueryAction.HEALTH.toString().toLowerCase();
    requestPathToResponseFutureMap.put(hbPath, hbResponseFuture);
    requestPathToResponseDelayMap.put(hbPath, 10000L);
    MockClient client = new MockClient(requestPathToResponseDelayMap, requestPathToResponseFutureMap);

    InstanceHealthMonitorConfig config = InstanceHealthMonitorConfig.builder()
        .setRoutingRequestDefaultTimeoutMS(1000l)
        .setRoutingPendingRequestCounterInstanceBlockThreshold(instanceBlockingThreshold)
        .setHeartBeatIntervalSeconds(1)
        .setHeartBeatRequestTimeoutMS(100l)
        .setRoutingTimedOutRequestCounterResetDelayMS(2000)
        .setClient(client)
        .build();

    try (InstanceHealthMonitor monitor = new InstanceHealthMonitor(config)) {
      CompletableFuture<TransportClientResponse> requestFuture = new CompletableFuture<>();
      ChainedCompletableFuture<Integer, Integer> chainedRequestFuture =
          monitor.trackHealthBasedOnRequestToInstance(instance, requestFuture);
      // Wait for the routing request to time out (it completes the request future) before completing the request,
      // so the timeout is not cancelled and the instance is marked suspicious.
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          true,
          () -> assertTrue(requestFuture.isCompletedExceptionally(), "routing request should have timed out"));
      requestFuture.complete(null);
      chainedRequestFuture.getOriginalFuture().complete(SC_GONE);
      // Pending request counter will be reset with a delay
      assertEquals(monitor.getPendingRequestCounter(instance), 1);
      // Heartbeat request should be triggered
      // Validate that instance should be put into unhealthy set
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          true,
          () -> assertTrue(
              !monitor.isInstanceHealthy(instance),
              "instance: " + instance + " should be marked as unhealthy"));
      // Remove the delay and the instance should become healthy again
      requestPathToResponseDelayMap.remove(hbPath);
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          true,
          () -> assertTrue(
              monitor.isInstanceHealthy(instance),
              "instance: " + instance + " should be marked as healthy again"));
      // Pending request count will be reset eventually
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          true,
          () -> assertEquals(monitor.getPendingRequestCounter(instance), 0));
    }
  }

  /**
   * A host left in the unhealthy set but removed from the fleet must be evicted by updateLiveInstanceSet and not
   * re-added by the still-failing heartbeat.
   */
  @Test
  public void testUpdateLiveInstanceSetEvictsHostRemovedFromFleet() throws Exception {
    try (InstanceHealthMonitor monitor = new InstanceHealthMonitor(failingHeartbeatConfig())) {
      // A request to the host hangs, so the routing timeout + failing heartbeat mark it unhealthy.
      monitor.trackHealthBasedOnRequestToInstance(instance, new CompletableFuture<>());
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          true,
          () -> assertFalse(monitor.isInstanceHealthy(instance), "slow host should be marked unhealthy"));
      assertEquals(monitor.getUnhealthyInstanceCount(), 1);

      // A refresh whose serving set still contains the host keeps it tracked (a live-but-unhealthy host).
      monitor.updateLiveInstanceSet(Collections.singleton(instance));
      assertFalse(monitor.isInstanceHealthy(instance));
      assertEquals(monitor.getUnhealthyInstanceCount(), 1);

      // An empty or null serving set is treated as "unknown" and must NOT wipe accumulated health state.
      monitor.updateLiveInstanceSet(Collections.emptySet());
      assertEquals(monitor.getUnhealthyInstanceCount(), 1);
      monitor.updateLiveInstanceSet(null);
      assertEquals(monitor.getUnhealthyInstanceCount(), 1);

      // A refresh whose serving set no longer contains the host evicts it, and the still-failing heartbeat must not
      // bring it back.
      monitor.updateLiveInstanceSet(Collections.singleton("https://other.host:4321"));
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
        assertTrue(
            monitor.isInstanceHealthy(instance),
            "host removed from the fleet should no longer be tracked as unhealthy");
        assertEquals(monitor.getUnhealthyInstanceCount(), 0);
      });
    }
  }

  /**
   * Unhealthy-host lifecycle: a slow host is marked unhealthy by the monitor, evicted when it leaves the serving set,
   * then rejoins clean.
   */
  @Test
  public void testUnhealthyHostEvictedThenRejoinsClean() throws Exception {
    try (InstanceHealthMonitor monitor = new InstanceHealthMonitor(failingHeartbeatConfig())) {
      // Host is in the cluster; a request to it hangs, so the routing timeout + failing heartbeat mark it unhealthy.
      monitor.updateLiveInstanceSet(Collections.singleton(instance));
      monitor.trackHealthBasedOnRequestToInstance(instance, new CompletableFuture<>());
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
        assertFalse(monitor.isInstanceHealthy(instance), "slow host should be marked unhealthy");
        assertEquals(monitor.getUnhealthyInstanceCount(), 1);
      });

      // Host leaves the cluster: the next serving set drops it, so the monitor evicts it.
      monitor.updateLiveInstanceSet(Collections.singleton("https://other.host:4321"));
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
        assertTrue(monitor.isInstanceHealthy(instance), "departed host should no longer be tracked");
        assertEquals(monitor.getUnhealthyInstanceCount(), 0);
      });

      // The same host returns and starts clean.
      monitor.updateLiveInstanceSet(Collections.singleton(instance));
      assertTrue(monitor.isInstanceHealthy(instance), "rejoined host should start clean");
      assertEquals(monitor.getUnhealthyInstanceCount(), 0);
    }
  }

  /**
   * Healthy-host lifecycle: even a host that never went unhealthy has its tracking state evicted when it leaves the
   * serving set, and rejoins clean.
   */
  @Test
  public void testHealthyHostEvictedThenRejoinsClean() throws Exception {
    InstanceHealthMonitorConfig config =
        InstanceHealthMonitorConfig.builder().setRoutingRequestDefaultTimeoutMS(10000L).build();
    try (InstanceHealthMonitor monitor = new InstanceHealthMonitor(config)) {
      // Host is in the cluster and serves a successful request, so it is tracked (a drained counter) and healthy.
      monitor.updateLiveInstanceSet(Collections.singleton(instance));
      ChainedCompletableFuture<Integer, Integer> chainedFuture = monitor.trackHealthBasedOnRequestToInstance(instance);
      chainedFuture.getOriginalFuture().complete(SC_OK);
      waitQuietly(chainedFuture.getResultFuture());
      assertTrue(monitor.isInstanceHealthy(instance));
      assertTrue(monitor.hasPendingRequestCounter(instance));

      // Host leaves the cluster: even though it is healthy, its tracking state is evicted.
      monitor.updateLiveInstanceSet(Collections.singleton("https://other.host:4321"));
      assertTrue(monitor.isInstanceHealthy(instance));
      assertFalse(monitor.hasPendingRequestCounter(instance));

      // The same host returns clean.
      monitor.updateLiveInstanceSet(Collections.singleton(instance));
      assertTrue(monitor.isInstanceHealthy(instance));
      assertFalse(monitor.hasPendingRequestCounter(instance));
    }
  }

  /**
   * A drained (zero) pending-request counter for a departed host is evicted; a counter for a live host or one with an
   * in-flight request is kept so accounting stays correct.
   */
  @Test
  public void testUpdateLiveInstanceSetEvictsDrainedPendingRequestCounters() throws Exception {
    InstanceHealthMonitorConfig config =
        InstanceHealthMonitorConfig.builder().setRoutingRequestDefaultTimeoutMS(10000L).build();

    try (InstanceHealthMonitor monitor = new InstanceHealthMonitor(config)) {
      // Complete a request so the pending-request counter drains to 0 but the entry remains.
      ChainedCompletableFuture<Integer, Integer> chainedFuture = monitor.trackHealthBasedOnRequestToInstance(instance);
      chainedFuture.getOriginalFuture().complete(SC_OK);
      waitQuietly(chainedFuture.getResultFuture());
      assertEquals(monitor.getPendingRequestCounter(instance), 0);
      assertTrue(monitor.hasPendingRequestCounter(instance));

      // A refresh that still lists the host keeps its drained counter.
      monitor.updateLiveInstanceSet(Collections.singleton(instance));
      assertTrue(monitor.hasPendingRequestCounter(instance));

      // A refresh that drops the host evicts the drained counter.
      monitor.updateLiveInstanceSet(Collections.singleton("https://other.host:4321"));
      assertFalse(monitor.hasPendingRequestCounter(instance));

      // An in-flight request (non-zero counter) is preserved even when the host is not in the serving set, so the
      // accounting and its completion-time reset stay correct.
      ChainedCompletableFuture<Integer, Integer> inFlightFuture = monitor.trackHealthBasedOnRequestToInstance(instance);
      assertEquals(monitor.getPendingRequestCounter(instance), 1);
      monitor.updateLiveInstanceSet(Collections.singleton("https://other.host:4321"));
      assertTrue(monitor.hasPendingRequestCounter(instance));
      assertEquals(monitor.getPendingRequestCounter(instance), 1);
      inFlightFuture.getOriginalFuture().complete(SC_OK);
      waitQuietly(inFlightFuture.getResultFuture());
    }
  }

  /** Config whose heartbeat to {@link #instance} always times out, so the instance stays unhealthy once tracked. */
  private static InstanceHealthMonitorConfig failingHeartbeatConfig() {
    Map<String, CompletableFuture<RestResponse>> futureMap = new VeniceConcurrentHashMap<>();
    Map<String, Long> delayMap = new VeniceConcurrentHashMap<>();
    String hbPath = instance + "/" + QueryAction.HEALTH.toString().toLowerCase();
    futureMap.put(hbPath, CompletableFuture.completedFuture(new RestResponseBuilder().setStatus(SC_OK).build()));
    // a large delay forces every heartbeat to this instance to time out, so it keeps failing
    delayMap.put(hbPath, 10000L);
    return InstanceHealthMonitorConfig.builder()
        .setRoutingRequestDefaultTimeoutMS(1000L)
        .setRoutingPendingRequestCounterInstanceBlockThreshold(10)
        .setHeartBeatIntervalSeconds(1)
        .setHeartBeatRequestTimeoutMS(100L)
        .setRoutingTimedOutRequestCounterResetDelayMS(2000)
        .setClient(new MockClient(delayMap, futureMap))
        .build();
  }

  private void waitQuietly(CompletableFuture future) throws InterruptedException {
    try {
      future.get();
    } catch (ExecutionException e) {
      // Do nothing
    }
  }
}
