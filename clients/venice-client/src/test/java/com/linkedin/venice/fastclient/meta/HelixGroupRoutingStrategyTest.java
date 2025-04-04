package com.linkedin.venice.fastclient.meta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
    doReturn(false).when(instanceHealthMonitor).isInstanceBlocked(any());
    doReturn(true).when(instanceHealthMonitor).isInstanceHealthy(any());
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
    doReturn(true).when(instanceHealthMonitor).isInstanceBlocked(instance2);
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
}
