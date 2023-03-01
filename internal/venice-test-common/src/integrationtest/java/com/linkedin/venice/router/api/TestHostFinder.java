package com.linkedin.venice.router.api;

import static com.linkedin.venice.router.api.TestRouterHeartbeat.mockVeniceRouterConfig;
import static org.apache.commons.httpclient.HttpStatus.SC_BAD_REQUEST;
import static org.apache.commons.httpclient.HttpStatus.SC_OK;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.router.api.HostHealthMonitor;
import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.utils.TestUtils;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHostFinder {
  public static final HostHealthMonitor NULL_HOST_HEALTH_MONITOR = (hostName, partitionName) -> true;

  @Test
  public void hostFinderShouldFindHosts() {
    RoutingDataRepository mockRepo = Mockito.mock(RoutingDataRepository.class);
    Instance dummyInstance1 = new Instance("0", "localhost1", 1234);
    Instance dummyInstance2 = new Instance("0", "localhost2", 1234);
    List<Instance> dummyList = new ArrayList<>();
    dummyList.add(dummyInstance1);
    dummyList.add(dummyInstance2);
    doReturn(dummyList).when(mockRepo).getReadyToServeInstances(anyString(), anyInt());

    HostHealthMonitor mockHostHealthMonitor = mock(HostHealthMonitor.class);
    doReturn(true).when(mockHostHealthMonitor).isHostHealthy(any(), any());

    VeniceHostFinder finder = new VeniceHostFinder(mockRepo, mock(RouterStats.class), mockHostHealthMonitor);

    List<Instance> hosts = finder.findHosts("get", "store_v0", "store_v0_3", NULL_HOST_HEALTH_MONITOR, null);
    Assert.assertEquals(hosts.size(), 2);
    List<String> hostNames = hosts.stream().map((h) -> h.getHost()).collect(Collectors.toList());
    Assert.assertTrue(hostNames.contains("localhost1"), "\"localhost1\" not found in " + hostNames.toString());
    Assert.assertTrue(hostNames.contains("localhost2"), "\"localhost2\" not found in " + hostNames.toString());

    // Mark dummyInstance1 as unhealthy
    HostHealthMonitor<Instance> anotherHostHealthyMonitor =
        ((hostName, partitionName) -> !hostName.equals(dummyInstance1));
    hosts = finder.findHosts("get", "store_v0", "store_v0_3", anotherHostHealthyMonitor, null);
    Assert.assertEquals(hosts.size(), 1);
    Assert.assertEquals(hosts.get(0).getHost(), "localhost2");
  }

  @Test
  public void testFindNothingWhenHeartBeatFailed() throws Exception {
    try (MockHttpServerWrapper server = ServiceFactory.getMockHttpServer("storage-node")) {
      int port = server.getPort();
      String nodeId = "localhost_" + port;
      Instance dummyInstance = Instance.fromNodeId(nodeId);
      Set<Instance> instanceSet = new HashSet<>();
      instanceSet.add(dummyInstance);

      // mock LiveInstanceMonitor
      LiveInstanceMonitor mockLiveInstanceMonitor = mock(LiveInstanceMonitor.class);
      doReturn(true).when(mockLiveInstanceMonitor).isInstanceAlive(any());
      doReturn(instanceSet).when(mockLiveInstanceMonitor).getAllLiveInstances();
      RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);

      Set<String> unhealthyHostsSet = getMockSetWithRealFunctionality();
      // mock VeniceHostHealth
      VeniceHostHealthTest healthMon =
          new VeniceHostHealthTest(mockLiveInstanceMonitor, routeHttpRequestStats, mockVeniceRouterConfig());
      healthMon.setUnhealthyHostSet(unhealthyHostsSet);

      // mock VeniceHostFinder
      RoutingDataRepository mockRepo = Mockito.mock(RoutingDataRepository.class);
      List<Instance> instanceList = new ArrayList<>();
      instanceList.add(dummyInstance);
      doReturn(instanceList).when(mockRepo).getReadyToServeInstances(anyString(), anyInt());
      RouterStats mockRouterStats = mock(RouterStats.class);
      when(mockRouterStats.getStatsByType(any())).thenReturn(mock(AggRouterHttpRequestStats.class));
      VeniceHostFinder finder = new VeniceHostFinder(mockRepo, mockRouterStats, healthMon);

      // mock HeartBeat
      FullHttpResponse goodHealthResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      goodHealthResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
      String uri = "/" + QueryAction.HEALTH.toString().toLowerCase();
      server.addResponseForUri(uri, goodHealthResponse);
      VeniceRouterConfig mockConfig = mockVeniceRouterConfig();

      // server response unhealthy for the heartbeat check
      FullHttpResponse badHealthResponse =
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
      badHealthResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);

      server.addResponseForUri(uri, badHealthResponse);

      StorageNodeClient storageNodeClient = mockStorageNodeClient(SC_OK);

      RouterHeartbeat heartbeat =
          new RouterHeartbeat(mockLiveInstanceMonitor, healthMon, mockConfig, Optional.empty(), storageNodeClient);
      heartbeat.start();

      // the HostFinder should find host now
      TestUtils.waitForNonDeterministicAssertion(
          4,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(1, finder.findHosts("get", "store_v0", "store_v0_3", healthMon, null).size()));

      // the HostFinder should find nothing now because heartbeat marks host as unhealthy in the VeniceHostHealth
      TestUtils.waitForNonDeterministicAssertion(
          15,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(0, finder.findHosts("get", "store_v0", "store_v0_3", healthMon, null).size()));

      /**
       * Verify that the unhealthy host is never removed from unhealthy set after a few healthy check cycles
       */
      Thread.sleep(2 * (long) (mockConfig.getHeartbeatCycleMs() + mockConfig.getHeartbeatTimeoutMs()));
      verify(unhealthyHostsSet, times(0)).remove(any());
    }
  }

  private StorageNodeClient mockStorageNodeClient(int code) {
    CloseableHttpAsyncClient httpAsyncClient = mock(CloseableHttpAsyncClient.class);
    CompletableFuture<HttpResponse> goodFuture = new CompletableFuture();
    CompletableFuture<HttpResponse> badFuture = new CompletableFuture();

    HttpResponse response = mock(HttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    doReturn(SC_OK).when(statusLine).getStatusCode();
    doReturn(statusLine).when(response).getStatusLine();
    goodFuture.complete(response);

    HttpResponse response1 = mock(HttpResponse.class);
    StatusLine statusLine1 = mock(StatusLine.class);
    doReturn(SC_BAD_REQUEST).when(statusLine1).getStatusCode();
    doReturn(statusLine1).when(response1).getStatusLine();
    badFuture.complete(response1);

    when(httpAsyncClient.execute(any(), any())).thenReturn(goodFuture).thenReturn(badFuture);

    StorageNodeClient storageNodeClient = mock(StorageNodeClient.class);
    return storageNodeClient;
  }

  private Set<String> getMockSetWithRealFunctionality() {
    Set<String> mockSet = mock(ConcurrentSkipListSet.class);
    Set<String> trueSet = new ConcurrentSkipListSet<>();
    doAnswer(invocation -> {
      trueSet.add(invocation.getArgument(0));
      return null;
    }).when(mockSet).add(any());
    doAnswer(invocation -> {
      return trueSet.contains(invocation.getArgument(0));
    }).when(mockSet).contains(any());
    doAnswer(invocation -> {
      trueSet.remove(invocation.getArgument(0));
      return null;
    }).when(mockSet).remove(any());
    return mockSet;
  }

  private StorageNodeClient mockStorageNodeClient(boolean ret) {
    StorageNodeClient client = mock(StorageNodeClient.class);
    doReturn(ret).when(client).isInstanceReadyToServe(anyString());
    return client;
  }

  /**
   * VeniceHostHealthTest extends the actual VeniceHostHealth;
   * the purpose of this subclass is to override the unhealthy host
   * set inside VeniceHostHealth with the mocking set which can
   * keep track of whether some APIs inside the set have been invoked.
   */
  private class VeniceHostHealthTest extends VeniceHostHealth {
    public VeniceHostHealthTest(
        LiveInstanceMonitor liveInstanceMonitor,
        RouteHttpRequestStats routeHttpRequestStats,
        VeniceRouterConfig config) {
      super(
          liveInstanceMonitor,
          mockStorageNodeClient(true),
          config,
          routeHttpRequestStats,
          mock(AggHostHealthStats.class));
    }

    /**
     * This API is used for testing only.
     * @param unhealthyHostSet
     */
    public void setUnhealthyHostSet(Set<String> unhealthyHostSet) {
      this.unhealthyHosts = unhealthyHostSet;
    }
  }
}
