package com.linkedin.venice.fastclient.meta;

import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_TOO_MANY_REQUESTS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class InstanceHealthMonitorTest {
  private final static String instance = "https://test.host:1234";

  @Test
  public void testPendingRequestCounterWithSuccessfulRequest() throws InterruptedException {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);
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

  @Test
  public void testPendingRequestCounterWithTooManyPendingRequests() throws InterruptedException {
    ClientConfig clientConfig = mock(ClientConfig.class);
    int instanceBlockingThreshold = 10;
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    doReturn(instanceBlockingThreshold).when(clientConfig).getRoutingPendingRequestCounterInstanceBlockThreshold();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);

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

  @Test
  public void testPendingRequestCounterWithQuotaExceededRequest() throws InterruptedException {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    doReturn(50l).when(clientConfig).getRoutingQuotaExceededRequestCounterResetDelayMS();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);
    ChainedCompletableFuture<Integer, Integer> chainedRequestFuture =
        healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    chainedRequestFuture.getOriginalFuture()
        .completeExceptionally(new VeniceClientHttpException("Quota exceeded", SC_TOO_MANY_REQUESTS));
    waitQuietly(chainedRequestFuture.getResultFuture());
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    TestUtils.waitForNonDeterministicAssertion(
        3,
        TimeUnit.SECONDS,
        () -> assertEquals(healthMonitor.getPendingRequestCounter(instance), 1));
  }

  @Test
  public void testMonitorsUnsuccessfulStatusCodeWithoutException() throws InterruptedException {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    doReturn(50l).when(clientConfig).getRoutingQuotaExceededRequestCounterResetDelayMS();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);
    ChainedCompletableFuture<Integer, Integer> chainedRequestFuture =
        healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    // Test if completing successfully with unsuccessful status code is handled correctly. This is legacy now, but left
    // for backward compatibility
    chainedRequestFuture.getOriginalFuture().complete(SC_TOO_MANY_REQUESTS);
    waitQuietly(chainedRequestFuture.getResultFuture());
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    TestUtils.waitForNonDeterministicAssertion(
        3,
        TimeUnit.SECONDS,
        () -> assertEquals(healthMonitor.getPendingRequestCounter(instance), 1));
  }

  @Test
  public void testPendingRequestCounterWithErrorRequest() throws InterruptedException {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(100l).when(clientConfig).getRoutingErrorRequestCounterResetDelayMS();
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);
    ChainedCompletableFuture<Integer, Integer> chainedRequestFuture =
        healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    // Received an error response
    chainedRequestFuture.getOriginalFuture()
        .completeExceptionally(new VeniceClientHttpException("Internal Server Error", SC_INTERNAL_SERVER_ERROR));
    waitQuietly(chainedRequestFuture.getResultFuture());
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    assertFalse(healthMonitor.isInstanceHealthy(instance));
    TestUtils.waitForNonDeterministicAssertion(
        3,
        TimeUnit.SECONDS,
        () -> assertEquals(healthMonitor.getPendingRequestCounter(instance), 0));
    assertFalse(healthMonitor.isInstanceHealthy(instance));

    chainedRequestFuture = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    // Received a good response
    chainedRequestFuture.getOriginalFuture().complete(SC_OK);
    waitQuietly(chainedRequestFuture.getResultFuture());
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 0);
    assertTrue(healthMonitor.isInstanceHealthy(instance));
  }

  private void waitQuietly(CompletableFuture future) throws InterruptedException {
    try {
      future.get();
    } catch (ExecutionException e) {
      // Do nothing
    }
  }
}
