package com.linkedin.venice.fastclient.meta;

import static org.apache.hc.core5.http.HttpStatus.SC_GONE;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class InstanceHealthMonitorTest {
  private final static String instance = "https://test.host:1234";

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
        .setClient(client)
        .build();

    try (InstanceHealthMonitor monitor = new InstanceHealthMonitor(config)) {
      CompletableFuture<TransportClientResponse> requestFuture = new CompletableFuture<>();
      ChainedCompletableFuture<Integer, Integer> chainedRequestFuture =
          monitor.trackHealthBasedOnRequestToInstance(instance, requestFuture);
      Thread.sleep(2000); // 2s to simulate request timeout
      requestFuture.complete(null);
      chainedRequestFuture.getOriginalFuture().complete(SC_GONE);
      // Pending request counter will be reset with a delay
      assertEquals(monitor.getPendingRequestCounter(instance), 1);
      // Heartbeat request should be triggered
      // Validate that instance should be put into unhealthy set
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          true,
          () -> assertTrue(
              !monitor.isInstanceHealthy(instance),
              "instance: " + instance + " should be marked as unhealthy"));
      // Remove the delay and the instance should become healthy again
      requestPathToResponseDelayMap.remove(hbPath);
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          true,
          () -> assertTrue(
              monitor.isInstanceHealthy(instance),
              "instance: " + instance + " should be marked as healthy again"));
      // Pending request count will be reset eventually
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          true,
          () -> assertEquals(monitor.getPendingRequestCounter(instance), 0));
    }
  }

  private void waitQuietly(CompletableFuture future) throws InterruptedException {
    try {
      future.get();
    } catch (ExecutionException e) {
      // Do nothing
    }
  }
}
