package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHostFinder {

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

    VeniceHostFinder finder = new VeniceHostFinder(mockRepo, false, false, null, null, mockHostHealthMonitor);

    List<Instance> hosts = finder.findHosts("get", "store_v0", "store_v0_3", null, null);
    Assert.assertEquals(hosts.size(), 2);
    Assert.assertEquals(hosts.get(0).getHost(), "localhost1");
    Assert.assertEquals(hosts.get(1).getHost(), "localhost2");
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
      List<Instance> hosts = finder.findHosts("get", "store_v0", "store_v0_" + partitionId, null, null);
      Assert.assertEquals(hosts.size(), 1);
      Assert.assertEquals(hosts.get(0).getHost(), expectedHost);
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
      List<Instance> hosts = finder.findHosts("get", "store_v0", "store_v0_" + partitionId, null, null);
      Assert.assertEquals(hosts.size(), 1);
      Assert.assertEquals(hosts.get(0).getHost(), expectedHost);
    });
    verify(mockSingleGetStats, times(partitionHostMapping.size())).recordFindUnhealthyHostRequest("store");
    verify(mockMultiGetStats, never()).recordFindUnhealthyHostRequest("store");
  }
}
