package com.linkedin.venice.fastclient.meta;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.routing.HelixGroupStats;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class HelixLeastLoadedGroupRoutingStrategyTest {
  private final static String instance1 = "https://instance1:1234";
  private final static String instance2 = "https://instance2:1234";
  private final static String instance3 = "https://instance3:1234";

  @Test
  public void testGetHelixGroupId() {
    InstanceHealthMonitor monitor = mock(InstanceHealthMonitor.class);
    HelixGroupStats stats = mock(HelixGroupStats.class);

    Map<String, Integer> instanceToGroupIdMapping = new HashMap<>();
    instanceToGroupIdMapping.put(instance1, 0);
    instanceToGroupIdMapping.put(instance2, 1);
    instanceToGroupIdMapping.put(instance3, 2);

    HelixLeastLoadedGroupRoutingStrategy strategy = new HelixLeastLoadedGroupRoutingStrategy(monitor, stats);
    strategy.updateHelixGroupInfo(instanceToGroupIdMapping);

    doReturn(-1d).when(stats).getGroupResponseWaitingTimeAvg(0);
    doReturn(5d).when(stats).getGroupResponseWaitingTimeAvg(1);
    doReturn(10d).when(stats).getGroupResponseWaitingTimeAvg(2);

    // Test with some group with no traffic in the past time windows.
    assertEquals(strategy.getHelixGroupId(0, -1), 0);
    // Avoid the group id in the original request
    assertEquals(strategy.getHelixGroupId(0, 0), 1);

    doReturn(5d).when(stats).getGroupResponseWaitingTimeAvg(0);
    int groupId = strategy.getHelixGroupId(0, -1);
    assertTrue(groupId == 0 || groupId == 1);
    assertEquals(strategy.getHelixGroupId(0, 1), 0);

    // Test with group 2 has a higher average latency, but still less than 1.5x
    doReturn(7.5d).when(stats).getGroupResponseWaitingTimeAvg(2);
    groupId = strategy.getHelixGroupId(0, 0);
    assertTrue(groupId == 1 || groupId == 2);

    // Test no data points in the past for any group
    doReturn(-1d).when(stats).getGroupResponseWaitingTimeAvg(0);
    doReturn(-1d).when(stats).getGroupResponseWaitingTimeAvg(1);
    doReturn(-1d).when(stats).getGroupResponseWaitingTimeAvg(2);
    assertEquals(strategy.getHelixGroupId(0, -1), 0);
    assertEquals(strategy.getHelixGroupId(1, -1), 1);
    assertEquals(strategy.getHelixGroupId(2, -1), 2);
    assertEquals(strategy.getHelixGroupId(3, -1), 0);
    assertEquals(strategy.getHelixGroupId(3, 0), 1);

    // Retry should choose the least loaded group, but not the group chosen by the original request.
    doReturn(5d).when(stats).getGroupResponseWaitingTimeAvg(0);
    doReturn(12d).when(stats).getGroupResponseWaitingTimeAvg(1);
    doReturn(12d).when(stats).getGroupResponseWaitingTimeAvg(2);
    assertEquals(strategy.getHelixGroupId(1, 1), 0);
  }
}
