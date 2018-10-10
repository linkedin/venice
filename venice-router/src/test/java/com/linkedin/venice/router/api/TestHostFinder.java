package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.utils.TestUtils;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHostFinder {
  public static final HostHealthMonitor NULL_HOST_HEALTH_MONITOR = (hostName, partitionName) -> true;
  @Test
  public void hostFinderShouldFindHosts(){
    RoutingDataRepository mockRepo = Mockito.mock(RoutingDataRepository.class);
    Instance dummyInstance1 = new Instance("0", "localhost1", 1234);
    Instance dummyInstance2 = new Instance("0", "localhost2", 1234);
    List<Instance> dummyList = new ArrayList<>();
    dummyList.add(dummyInstance1);
    dummyList.add(dummyInstance2);
    doReturn(dummyList).when(mockRepo).getReadyToServeInstances(anyString(), anyInt());

    HostHealthMonitor mockHostHealthMonitor = mock(HostHealthMonitor.class);
    doReturn(true).when(mockHostHealthMonitor).isHostHealthy(any(), any());

    VeniceHostFinder finder = new VeniceHostFinder(mockRepo, false, false,
        mock(AggRouterHttpRequestStats.class), mock(AggRouterHttpRequestStats.class), mockHostHealthMonitor);

    List<Instance> hosts = finder.findHosts("get", "store_v0", "store_v0_3", NULL_HOST_HEALTH_MONITOR, null);
    Assert.assertEquals(hosts.size(), 2);
    List<String> hostNames = hosts.stream().map((h) -> h.getHost()).collect(Collectors.toList());
    Assert.assertTrue(hostNames.contains("localhost1"), "\"localhost1\" not found in " + hostNames.toString());
    Assert.assertTrue(hostNames.contains("localhost2"), "\"localhost2\" not found in " + hostNames.toString());

    // Mark dummyInstance1 as unhealthy
    HostHealthMonitor<Instance> anotherHostHealthyMonitor = ((hostName, partitionName) -> !hostName.equals(dummyInstance1));
    hosts = finder.findHosts("get", "store_v0", "store_v0_3", anotherHostHealthyMonitor, null);
    Assert.assertEquals(hosts.size(), 1);
    Assert.assertEquals(hosts.get(0).getHost(), "localhost2");
  }

  @Test
  public void testStickyRoutingWhenAllInstancesAreHealthy() {
    RoutingDataRepository mockRepo = Mockito.mock(RoutingDataRepository.class);
    List<Instance> dummyList = new ArrayList<>();
    int hostCount = 3;
    for (int i = hostCount - 1; i >= 0; --i) {
      dummyList.add(new Instance("node_id_" + i, "host_" + i, 1234));
    }
    doReturn(dummyList).when(mockRepo).getReadyToServeInstances(anyString(), anyInt());

    HostHealthMonitor mockHostHealthMonitor = mock(HostHealthMonitor.class);
    doReturn(true).when(mockHostHealthMonitor).isHostHealthy(any(), any());
    VeniceHostFinder finder = new VeniceHostFinder(mockRepo, true, true, null, null, mockHostHealthMonitor);

    Map<Integer, String> partitionHostMapping = new HashMap<>();
    partitionHostMapping.put(0, "host_0");
    partitionHostMapping.put(1, "host_1");
    partitionHostMapping.put(2, "host_2");
    partitionHostMapping.put(3, "host_0");
    partitionHostMapping.put(4, "host_1");
    partitionHostMapping.put(5, "host_2");
    partitionHostMapping.forEach((partitionId, expectedHost) -> {
      // Notice that the variable name `expectedHost` has lost its meaning here because partition-based sticky routing no longer exists
      List<Instance> hosts = finder.findHosts("get", "store_v0", "store_v0_" + partitionId, NULL_HOST_HEALTH_MONITOR, null);
      // key-based sticky routing; all replicas will be returned and one host will be chosen by the key
      Assert.assertEquals(hosts.size(), 3);
    });
  }

  @Test
  public void testStickyRoutingWhenSomeInstancesAreUnhealthy() {
    RoutingDataRepository mockRepo = Mockito.mock(RoutingDataRepository.class);
    List<Instance> dummyList = new ArrayList<>();
    int hostCount = 3;
    for (int i = hostCount - 1; i >= 0; --i) {
      dummyList.add(new Instance("node_id_" + i, "host_" + i, 1234));
    }
    doReturn(dummyList).when(mockRepo).getReadyToServeInstances(anyString(), anyInt());

    HostHealthMonitor mockHostHealthMonitor = mock(HostHealthMonitor.class);
    doReturn(true).when(mockHostHealthMonitor).isHostHealthy(any(), any());
    doReturn(false).when(mockHostHealthMonitor).isHostHealthy(eq(new Instance("node_id_1", "host_1", 1234)), any());

    AggRouterHttpRequestStats mockSingleGetStats = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockMultiGetStats = mock(AggRouterHttpRequestStats.class);

    VeniceHostFinder finder = new VeniceHostFinder(mockRepo, true, true, mockSingleGetStats, mockMultiGetStats, mockHostHealthMonitor);

    Map<Integer, String> partitionHostMapping = new HashMap<>();
    partitionHostMapping.put(0, "host_0");
    partitionHostMapping.put(1, "host_2");
    partitionHostMapping.put(2, "host_0");
    partitionHostMapping.put(3, "host_2");
    partitionHostMapping.put(4, "host_0");
    partitionHostMapping.put(5, "host_2");
    partitionHostMapping.forEach((partitionId, expectedHost) -> {
      // Notice that the variable name `expectedHost` has lost its meaning here because partition-based sticky routing no longer exists
      List<Instance> hosts = finder.findHosts("get", "store_v0", "store_v0_" + partitionId, NULL_HOST_HEALTH_MONITOR, null);
      // key-based sticky routing; all replicas will be returned and one host will be chosen by the key
      Assert.assertEquals(hosts.size(), 2);
    });
    verify(mockSingleGetStats, times(partitionHostMapping.size())).recordFindUnhealthyHostRequest("store");
    verify(mockMultiGetStats, never()).recordFindUnhealthyHostRequest("store");
  }

  @Test
  public void testFindNothingWhenHeartBeatFailed() {
    // create one instance
    MockHttpServerWrapper server = ServiceFactory.getMockHttpServer("storage-node");
    int port = server.getPort();
    String nodeId = "localhost_" + port;
    Instance dummyInstance = Instance.fromNodeId(nodeId);
    Set<Instance> instanceSet = new HashSet<>();
    instanceSet.add(dummyInstance);

    // mock LiveInstanceMonitor
    LiveInstanceMonitor mockLiveInstanceMonitor = mock(LiveInstanceMonitor.class);
    doReturn(true).when(mockLiveInstanceMonitor).isInstanceAlive(any());
    doReturn(instanceSet).when(mockLiveInstanceMonitor).getAllLiveInstances();

    // mock VeniceHostHealth
    VeniceHostHealth healthMon = new VeniceHostHealth(mockLiveInstanceMonitor);

    // mock VeniceHostFinder
    RoutingDataRepository mockRepo = Mockito.mock(RoutingDataRepository.class);
    List<Instance> instanceList = new ArrayList<>();
    instanceList.add(dummyInstance);
    doReturn(instanceList).when(mockRepo).getReadyToServeInstances(anyString(), anyInt());
    VeniceHostFinder finder = new VeniceHostFinder(mockRepo, false, false,
        mock(AggRouterHttpRequestStats.class), mock(AggRouterHttpRequestStats.class), healthMon);

    // mock HeartBeat
    FullHttpResponse goodHealthResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    server.addResponseForUri("/" + QueryAction.HEALTH.toString().toLowerCase(), goodHealthResponse);
    RouterHeartbeat heartbeat = new RouterHeartbeat(mockLiveInstanceMonitor, healthMon, 100, TimeUnit.MILLISECONDS, 500, Optional
        .empty());
    heartbeat.start();

    // the HostFinder should find host now
    TestUtils.waitForNonDeterministicAssertion(4, TimeUnit.SECONDS,
        () -> Assert.assertEquals(1, finder.findHosts("get", "store_v0", "store_v0_3", healthMon, null).size()));

    // server response unhealthy for the heartbeat check
    FullHttpResponse badHealthResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    server.clearResponseMapping();
    server.addResponseForUri("/" + QueryAction.HEALTH.toString().toLowerCase(), badHealthResponse);

    // the HostFinder should find nothing now because heartbeat marks host as unhealthy in the VeniceHostHealth
    TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS,
        () -> Assert.assertEquals(0, finder.findHosts("get", "store_v0", "store_v0_3", healthMon, null).size()));
  }
}
