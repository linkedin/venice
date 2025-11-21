package com.linkedin.venice.fastclient.meta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.fastclient.BatchGetRequestContext;
import com.linkedin.venice.fastclient.GetRequestContext;
import com.linkedin.venice.fastclient.RequestContext;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixGroupRoutingStrategyTest {
  private final static String instance1 = "https://instance1:1234";
  private final static String instance2 = "https://instance2:1234";
  private final static String instance3 = "https://instance3:1234";

  private InstanceHealthMonitor instanceHealthMonitor;

  /**
   * The following mapping belongs to the same partition.
   */
  private Map<String, Integer> getHelixGroupInfo() {
    Map<String, Integer> helixGroupInfo = new HashMap<>();
    helixGroupInfo.put(instance1, 0);
    helixGroupInfo.put(instance2, 1);
    helixGroupInfo.put(instance3, 2);

    return helixGroupInfo;
  }

  @BeforeMethod
  public void setUp() {
    instanceHealthMonitor = mock(InstanceHealthMonitor.class);
    doReturn(true).when(instanceHealthMonitor).isRequestAllowed(any());
  }

  public void runTest(List<String> replicas, long requestId, int expectedGroupId, String expectedReplica) {
    HelixGroupRoutingStrategy strategy =
        new HelixGroupRoutingStrategy(instanceHealthMonitor, new MetricsRepository(), "test_store");
    strategy.updateHelixGroupInfo(getHelixGroupInfo());
    int groupId = strategy.getHelixGroupId(requestId, -1);
    assertEquals(
        groupId,
        expectedGroupId,
        "The group ID selected by HelixGroupRoutingStrategy does not match the expected group ID.");
    String selectedReplica = strategy.getReplicas(requestId, groupId, replicas);
    assertEquals(selectedReplica, expectedReplica);
  }

  @Test
  public void testGetReplicasWithRoundRobin() {
    List<String> replicas = Arrays.asList(instance1, instance2, instance3);
    runTest(replicas, 0, 0, instance1);
    runTest(replicas, 1, 1, instance2);
    runTest(replicas, 2, 2, instance3);
    runTest(replicas, 3, 0, instance1);
  }

  @Test
  public void testGetReplicaWithBlockedInstances() {
    doReturn(false).when(instanceHealthMonitor).isRequestAllowed(instance2);
    List<String> replicas = Arrays.asList(instance1, instance2, instance3);
    runTest(replicas, 0, 0, instance1);
    // 1, 4, 5, 6 are all blocked. Can only get 2 and 3 in order to meet the required replica of 2.
    runTest(replicas, 1, 1, instance3);
    runTest(replicas, 2, 2, instance3);
  }

  @Test
  public void testGetReplicasWithoutAnyFilteredReplicas() {
    List<String> replicas = Collections.emptyList();
    runTest(replicas, 0, 0, null);
  }

  @Test
  public void testLargeRequestId() {
    List<String> replicas = Collections.singletonList(instance1);
    long requestId = Integer.MAX_VALUE;
    requestId += 100;
    runTest(replicas, requestId, 2, instance1);
  }

  @Test
  public void testGetGroupId() {
    HelixGroupRoutingStrategy strategy =
        new HelixGroupRoutingStrategy(instanceHealthMonitor, new MetricsRepository(), "test_store");
    strategy.updateHelixGroupInfo(getHelixGroupInfo());
    assertEquals(strategy.getHelixGroupId(0, -1), 0);
    assertEquals(strategy.getHelixGroupId(1, -1), 1);
    assertEquals(strategy.getHelixGroupId(2, -1), 2);
    assertEquals(strategy.getHelixGroupId(3, -1), 0);
    assertEquals(strategy.getHelixGroupId(3, 0), 1);
    assertEquals(strategy.getHelixGroupId(4, 1), 2);
  }

  @Test
  public void testTrackRequest() {
    HelixGroupStats mockStats = mock(HelixGroupStats.class);
    HelixGroupRoutingStrategy strategy = new HelixGroupRoutingStrategy(instanceHealthMonitor, mockStats);
    strategy.updateHelixGroupInfo(getHelixGroupInfo());

    RequestContext singleGetRequestContext = new GetRequestContext();
    assertFalse(strategy.trackRequest(singleGetRequestContext));

    BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext(10, true);
    batchGetRequestContext.setRetryRequest(true);
    assertFalse(strategy.trackRequest(batchGetRequestContext));

    CompletableFuture resultFuture = new CompletableFuture();
    resultFuture.complete(null);
    batchGetRequestContext.setRetryRequest(false);
    batchGetRequestContext.setHelixGroupId(1);
    batchGetRequestContext.setResultFuture(resultFuture);
    assertTrue(strategy.trackRequest(batchGetRequestContext));
    verify(mockStats).recordGroupNum(3);
    verify(mockStats).recordGroupRequest(1);
    verify(mockStats).recordGroupResponseWaitingTime(eq(1), anyDouble());
  }

  /**
   * Test that getReplicas handles missing replica in helixGroupInfo gracefully.
   * This tests defensive behavior when there's a race condition between routing info
   * and helix group info updates.
   */
  @Test
  public void testGetReplicasWithMissingHelixGroupInfo() {
    HelixGroupRoutingStrategy strategy =
        new HelixGroupRoutingStrategy(instanceHealthMonitor, new MetricsRepository(), "test_store");
    strategy.updateHelixGroupInfo(getHelixGroupInfo());

    // Create a replica list with an unknown instance (not in helixGroupInfo)
    String unknownInstance = "https://unknown:1234";
    List<String> replicasWithUnknown = Arrays.asList(instance1, instance2, unknownInstance);

    // This should NOT throw NPE, even though unknownInstance is not in helixGroupInfo
    String selectedReplica = strategy.getReplicas(0, 0, replicasWithUnknown);

    // Should select instance1 (the known instance in group 0)
    assertEquals(selectedReplica, instance1);
  }

  /**
   * - New replica appears in routing info (from CustomizedView)
   * - But not yet in helix group info (InstanceConfig update lagging)
   */
  @Test
  public void testGetReplicasWithRaceConditionScenario() {
    HelixGroupRoutingStrategy strategy =
        new HelixGroupRoutingStrategy(instanceHealthMonitor, new MetricsRepository(), "test_store");

    // Initial helix group info (only has instance1 and instance2)
    Map<String, Integer> initialHelixGroupInfo = new HashMap<>();
    initialHelixGroupInfo.put(instance1, 0);
    initialHelixGroupInfo.put(instance2, 1);
    strategy.updateHelixGroupInfo(initialHelixGroupInfo);

    // Simulate race condition: routing info has new instance3
    // but helix group info hasn't been updated yet
    String newInstance = instance3;
    List<String> replicasWithNew = Arrays.asList(instance1, instance2, newInstance);

    // Request that would select group 2 (where instance3 should be, but isn't yet)
    long requestId = 2;
    int groupId = 2;

    // This should NOT throw NPE - should handle gracefully
    String selectedReplica = strategy.getReplicas(requestId, groupId, replicasWithNew);

    // Should fall back to one of the known instances or return null
    assertTrue(selectedReplica == null || selectedReplica.equals(instance1) || selectedReplica.equals(instance2));
  }

  /**
   * Test that all replicas missing from helixGroupInfo returns null gracefully.
   */
  @Test
  public void testGetReplicasWhenAllReplicasMissing() {
    HelixGroupRoutingStrategy strategy =
        new HelixGroupRoutingStrategy(instanceHealthMonitor, new MetricsRepository(), "test_store");
    strategy.updateHelixGroupInfo(getHelixGroupInfo());

    // All replicas are unknown
    List<String> unknownReplicas = Arrays.asList("https://unknown1:1234", "https://unknown2:1234");

    // Should return null, not throw NPE
    String selectedReplica = strategy.getReplicas(0, 0, unknownReplicas);
    assertNull(selectedReplica);
  }

  /**
   * Regression test: verify normal operation still works correctly after defensive fix.
   */
  @Test
  public void testGetReplicasNormalOperation() {
    HelixGroupRoutingStrategy strategy =
        new HelixGroupRoutingStrategy(instanceHealthMonitor, new MetricsRepository(), "test_store");
    strategy.updateHelixGroupInfo(getHelixGroupInfo());

    // All replicas are known
    List<String> replicas = Arrays.asList(instance1, instance2, instance3);

    // Should work normally - each request selects the correct group
    String selectedReplica = strategy.getReplicas(0, 0, replicas);
    assertEquals(selectedReplica, instance1);

    selectedReplica = strategy.getReplicas(1, 1, replicas);
    assertEquals(selectedReplica, instance2);

    selectedReplica = strategy.getReplicas(2, 2, replicas);
    assertEquals(selectedReplica, instance3);
  }

}
