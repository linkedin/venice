package com.linkedin.venice.fastclient.meta;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class InstanceHealthMonitorTest {
  private final static String instance = "https://test.host:1234";

  @Test
  public void testPendingRequestCounterWithSuccessfulRequest() {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);
    CompletableFuture<HttpStatus> future = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    future.complete(HttpStatus.S_200_OK);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 0);

    future = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    future.complete(HttpStatus.S_404_NOT_FOUND);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 0);
  }

  @Test
  public void testPendingRequestCounterWithTooManyPendingRequests() {
    ClientConfig clientConfig = mock(ClientConfig.class);
    int instanceBlockingThreshold = 10;
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    doReturn(instanceBlockingThreshold).when(clientConfig).getRoutingPendingRequestCounterInstanceBlockThreshold();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);

    CompletableFuture<HttpStatus> requestFuture = new CompletableFuture<>();
    for (int i = 0; i < 10; ++i) {
      requestFuture = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    }
    assertTrue(healthMonitor.isInstanceBlocked(instance));
    assertEquals(healthMonitor.getPendingRequestCounter(instance), instanceBlockingThreshold);
    // Finish one request
    requestFuture.complete(HttpStatus.S_200_OK);
    assertFalse(healthMonitor.isInstanceBlocked(instance));
    assertEquals(healthMonitor.getPendingRequestCounter(instance), instanceBlockingThreshold - 1);
  }

  @Test
  public void testPendingRequestCounterWithQuotaExceededRequest() {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    doReturn(50l).when(clientConfig).getRoutingQuotaExceededRequestCounterResetDelayMS();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);
    CompletableFuture<HttpStatus> requestFuture = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    requestFuture.complete(HttpStatus.S_429_TOO_MANY_REQUESTS);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    TestUtils.waitForNonDeterministicAssertion(
        3,
        TimeUnit.SECONDS,
        () -> assertEquals(healthMonitor.getPendingRequestCounter(instance), 1));
  }

  @Test
  public void testPendingRequestCounterWithErrorRequest() {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(100l).when(clientConfig).getRoutingErrorRequestCounterResetDelayMS();
    doReturn(10000l).when(clientConfig).getRoutingLeakedRequestCleanupThresholdMS();
    InstanceHealthMonitor healthMonitor = new InstanceHealthMonitor(clientConfig);
    CompletableFuture<HttpStatus> requestFuture = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    // Received an error response
    requestFuture.complete(HttpStatus.S_500_INTERNAL_SERVER_ERROR);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    assertFalse(healthMonitor.isInstanceHealthy(instance));
    TestUtils.waitForNonDeterministicAssertion(
        3,
        TimeUnit.SECONDS,
        () -> assertEquals(healthMonitor.getPendingRequestCounter(instance), 0));
    assertFalse(healthMonitor.isInstanceHealthy(instance));

    requestFuture = healthMonitor.trackHealthBasedOnRequestToInstance(instance);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 1);
    // Received a good response
    requestFuture.complete(HttpStatus.S_200_OK);
    assertEquals(healthMonitor.getPendingRequestCounter(instance), 0);
    assertTrue(healthMonitor.isInstanceHealthy(instance));
  }
}
