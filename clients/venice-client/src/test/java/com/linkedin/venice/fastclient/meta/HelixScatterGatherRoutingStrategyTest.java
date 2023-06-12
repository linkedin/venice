package com.linkedin.venice.fastclient.meta;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixScatterGatherRoutingStrategyTest {
  private final static String instance1 = "https://instance1:1234";
  private final static String instance2 = "https://instance2:1234";
  private final static String instance3 = "https://instance3:1234";
  private final static String instance4 = "https://instance4:1234";
  private final static String instance5 = "https://instance5:1234";
  private final static String instance6 = "https://instance6:1234";

  private InstanceHealthMonitor instanceHealthMonitor;

  private Map<String, Integer> getHelixGroupInfo() {
    Map<String, Integer> helixGroupInfo = new HashMap<>();
    helixGroupInfo.put(instance1, 0);
    helixGroupInfo.put(instance2, 0);
    helixGroupInfo.put(instance3, 0);
    helixGroupInfo.put(instance4, 1);
    helixGroupInfo.put(instance5, 1);
    helixGroupInfo.put(instance6, 1);

    return helixGroupInfo;
  }

  @BeforeMethod
  public void setUp() {
    instanceHealthMonitor = mock(InstanceHealthMonitor.class);
  }

  public void runTest(List<String> replicas, long requestId, int requiredReplicaCount, List<String> expectedReplicas) {
    HelixScatterGatherRoutingStrategy strategy = new HelixScatterGatherRoutingStrategy(instanceHealthMonitor);
    strategy.updateHelixGroupInfo(getHelixGroupInfo());
    List<String> selectedReplicas = strategy.getReplicas(requestId, replicas, requiredReplicaCount);
    assertEquals(selectedReplicas, expectedReplicas);
  }

  @Test
  public void testGetReplicasWithAdequateReplicas() {
    List<String> replicas = Arrays.asList(instance1, instance2, instance3);
    runTest(replicas, 0, 2, Arrays.asList(instance1, instance2));
    runTest(replicas, 1, 3, Arrays.asList(instance1, instance2, instance3));
  }

  @Test
  public void testGetReplicasUsingNeighbors() {
    List<String> replicas = Arrays.asList(instance1, instance4, instance5, instance6);
    runTest(replicas, 0, 2, Arrays.asList(instance1, instance4));
    runTest(replicas, 1, 4, Arrays.asList(instance4, instance5, instance6, instance1));
  }

  @Test
  public void testGetReplicaWithBlockedInstances() {
    doReturn(true).when(instanceHealthMonitor).isInstanceBlocked(instance1);
    doReturn(true).when(instanceHealthMonitor).isInstanceBlocked(instance4);
    doReturn(true).when(instanceHealthMonitor).isInstanceBlocked(instance5);
    doReturn(true).when(instanceHealthMonitor).isInstanceBlocked(instance6);
    List<String> replicas = Arrays.asList(instance1, instance2, instance3, instance4, instance5, instance6);
    runTest(replicas, 0, 2, Arrays.asList(instance2, instance3));
    // 1, 4, 5, 6 are all blocked. Can only get 2 and 3 in order to meet the required replica of 2.
    runTest(replicas, 1, 2, Arrays.asList(instance2, instance3));
  }

  @Test
  public void testGetReplicasWithoutReachingRequiredCount() {
    List<String> replicas = Arrays.asList(instance1);
    runTest(replicas, 0, 2, Arrays.asList(instance1));
  }

  @Test
  public void testGetReplicasWithoutAnyFilteredReplicas() {
    List<String> replicas = Arrays.asList();
    runTest(replicas, 0, 2, Arrays.asList());
  }
}
