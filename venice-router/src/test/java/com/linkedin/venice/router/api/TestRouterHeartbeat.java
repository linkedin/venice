package com.linkedin.venice.router.api;

import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.utils.TestUtils;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestRouterHeartbeat {

  private LiveInstanceMonitor mockLiveInstanceMonitor(Set<Instance> liveInstance) {
    LiveInstanceMonitor mockLiveInstanceMonitor = mock(LiveInstanceMonitor.class);
    doReturn(true).when(mockLiveInstanceMonitor).isInstanceAlive(any());
    doReturn(liveInstance).when(mockLiveInstanceMonitor).getAllLiveInstances();

    return mockLiveInstanceMonitor;
  }

  private StorageNodeClient mockStorageNodeClient(boolean ret) {
    StorageNodeClient client =  mock(StorageNodeClient.class);
    doReturn(ret).when(client).isInstanceReadyToServe(anyString());
    return client;
  }

  public static VeniceRouterConfig mockVeniceRouterConfig() {
    VeniceRouterConfig mockConfig = mock(VeniceRouterConfig.class);
    doReturn(500d).when(mockConfig).getHeartbeatTimeoutMs();
    doReturn(100l).when(mockConfig).getHeartbeatCycleMs();
    doReturn(1000).when(mockConfig).getSocketTimeout();
    doReturn(3000).when(mockConfig).getConnectionTimeout();

    return mockConfig;
  }

  @Test
  public void heartBeatMarksUnreachableNodes()
      throws Exception {
    // This is a fake instance that wont respond.  Nothing is runing on that port
    Instance dummyInstance = Instance.fromNodeId("localhost_58262");
    Set<Instance> instanceSet = new HashSet<>();
    instanceSet.add(dummyInstance);
    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);

    LiveInstanceMonitor mockLiveInstanceMonitor = mockLiveInstanceMonitor(instanceSet);
    VeniceRouterConfig config = mockVeniceRouterConfig();

    VeniceHostHealth healthMon = new VeniceHostHealth(mockLiveInstanceMonitor, mockStorageNodeClient(true), config, routeHttpRequestStats, mock(AggHostHealthStats.class));

    Assert.assertTrue(healthMon.isHostHealthy(dummyInstance, "partition"));

    RouterHeartbeat heartbeat = new RouterHeartbeat(mockLiveInstanceMonitor, healthMon, config, Optional.empty());
    heartbeat.start();

    // Since the heartbeat is querying an instance that wont respond, we expect it to tell the health monitor that the
    // host is unhealthy.
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS,
        () -> Assert.assertFalse(healthMon.isHostHealthy(dummyInstance, "partition")));
    heartbeat.stop();
  }

  @Test
  public void heartBeatKeepsGoodNodesHealthy() throws Exception {
    // We want to verify the heartbeat can get a response from a server, so we create a server that
    // responds to a health check.
    MockHttpServerWrapper server = ServiceFactory.getMockHttpServer("storage-node");
    FullHttpResponse goodHealthResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    server.addResponseForUri("/" + QueryAction.HEALTH.toString().toLowerCase(), goodHealthResponse);

    // now our dummy instance lives at the port where a good health check will return
    int port = server.getPort();
    String nodeId = "localhost_" + port;
    Instance dummyInstance = Instance.fromNodeId(nodeId);
    Set<Instance> instanceSet = new HashSet<>();
    instanceSet.add(dummyInstance);

    LiveInstanceMonitor mockLiveInstanceMonitor = mockLiveInstanceMonitor(instanceSet);
    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);
    VeniceRouterConfig config = mockVeniceRouterConfig();

    VeniceHostHealth healthMon = new VeniceHostHealth(mockLiveInstanceMonitor, mockStorageNodeClient(true), config, routeHttpRequestStats, mock(AggHostHealthStats.class));

    RouterHeartbeat heartbeat = new RouterHeartbeat(mockLiveInstanceMonitor, healthMon, config, Optional.empty());
    heartbeat.start();

    // our instance should stay healthy since it responds to the health check.
    Assert.assertTrue(healthMon.isHostHealthy(dummyInstance, "partition"));
    heartbeat.stop();
    server.close();
  }

  @Test
  public void heartBeatKeepBadNodesUnHealthy() throws Exception {
    // We want to verify the heartbeat can get a response from a server, so we create a server that
    // responds to a health check.
    MockHttpServerWrapper server = ServiceFactory.getMockHttpServer("storage-node");
    FullHttpResponse badHealthResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    badHealthResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH,0);
    server.addResponseForUri("/" + QueryAction.HEALTH.toString().toLowerCase(), badHealthResponse);

    // now our dummy instance lives at the port where a good health check will return
    int port = server.getPort();
    String nodeId = "localhost_" + port;
    Instance dummyInstance = Instance.fromNodeId(nodeId);
    Set<Instance> instanceSet = new HashSet<>();
    instanceSet.add(dummyInstance);

    LiveInstanceMonitor mockLiveInstanceMonitor = mockLiveInstanceMonitor(instanceSet);
    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);
    VeniceRouterConfig config = mockVeniceRouterConfig();

    VeniceHostHealth healthMon = new VeniceHostHealth(mockLiveInstanceMonitor, mockStorageNodeClient(true), config, routeHttpRequestStats,  mock(AggHostHealthStats.class));

    Assert.assertTrue(healthMon.isHostHealthy(dummyInstance, "partition"));

    RouterHeartbeat heartbeat = new RouterHeartbeat(mockLiveInstanceMonitor, healthMon, config, Optional.empty());
    heartbeat.start();
    Thread.sleep(1000);

    // our instance should report unhealthy
    Assert.assertFalse(healthMon.isHostHealthy(dummyInstance, "partition"));

    heartbeat.stop();
    server.close();
  }
}
