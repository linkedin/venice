package com.linkedin.venice.fastclient.meta;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class HelixLatencyAdaptiveGroupRoutingStrategyTest {
  private final static String instance1 = "https://instance1:1234";
  private final static String instance2 = "https://instance2:1234";
  private final static String instance3 = "https://instance3:1234";

  private static HelixLatencyAdaptiveGroupRoutingStrategy threeGroupStrategy(HelixGroupStats stats) {
    InstanceHealthMonitor monitor = mock(InstanceHealthMonitor.class);
    HelixLatencyAdaptiveGroupRoutingStrategy strategy = new HelixLatencyAdaptiveGroupRoutingStrategy(monitor, stats);
    Map<String, Integer> instanceToGroupIdMapping = new HashMap<>();
    instanceToGroupIdMapping.put(instance1, 0);
    instanceToGroupIdMapping.put(instance2, 1);
    instanceToGroupIdMapping.put(instance3, 2);
    strategy.updateHelixGroupInfo(instanceToGroupIdMapping);
    return strategy;
  }

  @Test
  public void testThrowsWhenNoGroups() {
    // No group info was ever set, so the group count is zero.
    InstanceHealthMonitor monitor = mock(InstanceHealthMonitor.class);
    HelixGroupStats stats = mock(HelixGroupStats.class);
    HelixLatencyAdaptiveGroupRoutingStrategy strategy = new HelixLatencyAdaptiveGroupRoutingStrategy(monitor, stats);
    assertThrows(VeniceClientException.class, () -> strategy.getHelixGroupId(0, -1));
  }

  @Test
  public void testAvoidsOriginalGroupOnRetry() {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    doReturn(5d).when(stats).getGroupResponseWaitingTimeAvg(0);
    doReturn(6d).when(stats).getGroupResponseWaitingTimeAvg(1);
    doReturn(7d).when(stats).getGroupResponseWaitingTimeAvg(2);
    HelixLatencyAdaptiveGroupRoutingStrategy strategy = threeGroupStrategy(stats);

    // Whatever the weighted draw picks, it must never be the group already tried on the original request.
    for (long requestId = 0; requestId < 300; requestId++) {
      int excluded = (int) (requestId % 3);
      int picked = strategy.getHelixGroupId(requestId, excluded);
      assertNotEquals(picked, excluded, "retry must avoid the original group");
      assertTrue(picked >= 0 && picked < 3);
    }
  }

  @Test
  public void testUnmeasuredGroupsRouteToValidGroup() {
    // No datapoints for any group (default mock return is 0.0 => unmeasured): routing stays even but must always
    // return a valid group id.
    HelixGroupStats stats = mock(HelixGroupStats.class);
    doReturn(-1d).when(stats).getGroupResponseWaitingTimeAvg(0);
    doReturn(-1d).when(stats).getGroupResponseWaitingTimeAvg(1);
    doReturn(-1d).when(stats).getGroupResponseWaitingTimeAvg(2);
    HelixLatencyAdaptiveGroupRoutingStrategy strategy = threeGroupStrategy(stats);
    for (long requestId = 0; requestId < 30; requestId++) {
      int picked = strategy.getHelixGroupId(requestId, -1);
      assertTrue(picked >= 0 && picked < 3, "must return a valid group, got " + picked);
    }
  }

  @Test
  public void testSingleGroupAlwaysSelected() {
    HelixGroupStats stats = mock(HelixGroupStats.class);
    doReturn(5d).when(stats).getGroupResponseWaitingTimeAvg(0);
    InstanceHealthMonitor monitor = mock(InstanceHealthMonitor.class);
    HelixLatencyAdaptiveGroupRoutingStrategy strategy = new HelixLatencyAdaptiveGroupRoutingStrategy(monitor, stats);
    Map<String, Integer> instanceToGroupIdMapping = new HashMap<>();
    instanceToGroupIdMapping.put(instance1, 0);
    strategy.updateHelixGroupInfo(instanceToGroupIdMapping);
    assertEquals(strategy.getHelixGroupId(0, -1), 0);
    assertEquals(strategy.getHelixGroupId(1, -1), 0);
  }

  @Test
  public void testFavoursFasterGroup() {
    // group0 is 10x faster than the other two => at full skew it should absorb the clear majority of traffic.
    HelixGroupStats stats = mock(HelixGroupStats.class);
    doReturn(10d).when(stats).getGroupResponseWaitingTimeAvg(0);
    doReturn(100d).when(stats).getGroupResponseWaitingTimeAvg(1);
    doReturn(100d).when(stats).getGroupResponseWaitingTimeAvg(2);
    HelixLatencyAdaptiveGroupRoutingStrategy strategy = threeGroupStrategy(stats);
    int fast = 0;
    int total = 6000;
    for (long requestId = 0; requestId < total; requestId++) {
      if (strategy.getHelixGroupId(requestId, -1) == 0) {
        fast++;
      }
    }
    assertTrue(fast > total * 0.5, "fast group should get the majority at full skew, got " + fast + "/" + total);
  }
}
