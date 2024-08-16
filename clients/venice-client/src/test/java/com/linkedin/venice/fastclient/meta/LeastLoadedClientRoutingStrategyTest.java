package com.linkedin.venice.fastclient.meta;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.testng.annotations.Test;


public class LeastLoadedClientRoutingStrategyTest {
  private final static String instance1 = "https://instance1:1234";
  private final static String instance2 = "https://instance2:1234";
  private final static String instance3 = "https://instance3:1234";
  private final static String instance4 = "https://instance4:1234";
  private final static String instance5 = "https://instance5:1234";
  private final static String instance6 = "https://instance6:1234";

  private InstanceHealthMonitor mockInstanceHealthyMonitor(
      String[] instances,
      boolean[] blocked,
      boolean[] healthy,
      int[] counter) {
    InstanceHealthMonitor instanceHealthMonitor = mock(InstanceHealthMonitor.class);
    if (instances.length != blocked.length || blocked.length != healthy.length || healthy.length != counter.length) {
      throw new IllegalArgumentException("The length of each array param should be same");
    }
    for (int i = 0; i < instances.length; ++i) {
      String instance = instances[i];
      doReturn(blocked[i]).when(instanceHealthMonitor).isInstanceBlocked(instance);
      doReturn(healthy[i]).when(instanceHealthMonitor).isInstanceHealthy(instance);
      doReturn(counter[i]).when(instanceHealthMonitor).getPendingRequestCounter(instance);
    }

    return instanceHealthMonitor;
  }

  public void runTest(
      InstanceHealthMonitor monitor,
      List<String> replicas,
      long requestId,
      int requiredReplicaCount,
      List<String> expectedReplicas) {
    LeastLoadedClientRoutingStrategy strategy = new LeastLoadedClientRoutingStrategy(monitor);
    List<String> selectedReplicas = strategy.getReplicas(requestId, replicas, requiredReplicaCount);
    assertEquals(selectedReplicas, expectedReplicas);
  }

  public void runTest(
      InstanceHealthMonitor monitor,
      List<String> replicas,
      long requestId,
      int requiredReplicaCount,
      Function<String, Boolean> expectedReplicaFunc) {
    LeastLoadedClientRoutingStrategy strategy = new LeastLoadedClientRoutingStrategy(monitor);
    List<String> selectedReplicas = strategy.getReplicas(requestId, replicas, requiredReplicaCount);
    selectedReplicas
        .forEach(replica -> assertTrue(expectedReplicaFunc.apply(replica), "replica: " + replica + " is unexpected"));
  }

  @Test
  public void testGetReplicasWithAllHealthyReplicas() {
    String[] instances = new String[] { instance1, instance2, instance3 };
    List<String> replicas = Arrays.asList(instances);
    InstanceHealthMonitor instanceHealthMonitor = mockInstanceHealthyMonitor(
        instances,
        new boolean[] { false, false, false },
        new boolean[] { true, true, true },
        new int[] { 0, 0, 0 });
    runTest(instanceHealthMonitor, replicas, 0, 2, replica -> replicas.contains(replica));
    runTest(instanceHealthMonitor, replicas, 1, 2, replica -> replicas.contains(replica));
    runTest(instanceHealthMonitor, replicas, 2, 2, replica -> replicas.contains(replica));
    runTest(instanceHealthMonitor, replicas, 3, 2, replica -> replicas.contains(replica));
  }

  @Test
  public void testGetReplicasWithAllHealthyReplicasWithDifferentWeights() {
    String[] instances = new String[] { instance1, instance2, instance3 };
    List<String> replicas = Arrays.asList(instances);
    InstanceHealthMonitor instanceHealthMonitor = mockInstanceHealthyMonitor(
        instances,
        new boolean[] { false, false, false },
        new boolean[] { true, true, true },
        new int[] { 6, 5, 4 });
    List<String> expectedReplicas = Arrays.asList(instance2, instance3);
    runTest(instanceHealthMonitor, replicas, 0, 2, replica -> expectedReplicas.contains(replica));
    runTest(instanceHealthMonitor, replicas, 1, 2, replica -> expectedReplicas.contains(replica));
    runTest(instanceHealthMonitor, replicas, 2, 2, replica -> expectedReplicas.contains(replica));
  }

  @Test
  public void testGetReplicasWithBlockedReplicas() {
    String[] instances = new String[] { instance1, instance2, instance3 };
    List<String> replicas = Arrays.asList(instances);
    InstanceHealthMonitor instanceHealthMonitor = mockInstanceHealthyMonitor(
        instances,
        new boolean[] { true, false, false },
        new boolean[] { true, true, true },
        new int[] { 5, 5, 4 });
    runTest(instanceHealthMonitor, replicas, 0, 2, Arrays.asList(instance3, instance2));
    runTest(instanceHealthMonitor, replicas, 1, 2, Arrays.asList(instance3, instance2));
  }

  @Test
  public void testGetReplicasWithUnhealthyReplicas() {
    String[] instances = new String[] { instance1, instance2, instance3, instance4, instance5, instance6 };
    List<String> replicas = Arrays.asList(instances);
    InstanceHealthMonitor instanceHealthMonitor = mockInstanceHealthyMonitor(
        instances,
        new boolean[] { true, false, false, false, false, false },
        new boolean[] { true, false, true, true, true, true },
        new int[] { 100, 1, 2, 4, 5, 3 });
    runTest(instanceHealthMonitor, replicas, 0, 2, Arrays.asList(instance2, instance3, instance6));
  }

  @Test
  public void testLargeRequestId() {
    String[] instances = new String[] { instance1, instance2, instance3 };
    List<String> replicas = Arrays.asList(instances);
    InstanceHealthMonitor instanceHealthMonitor = mockInstanceHealthyMonitor(
        instances,
        new boolean[] { false, false, false },
        new boolean[] { true, true, true },
        new int[] { 0, 0, 0 });
    long requestId = Integer.MAX_VALUE;
    requestId += 100;
    runTest(instanceHealthMonitor, replicas, requestId, 1, replica -> replicas.contains(replica));
  }
}
