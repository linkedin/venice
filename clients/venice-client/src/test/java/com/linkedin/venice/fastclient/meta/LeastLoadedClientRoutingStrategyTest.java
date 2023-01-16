package com.linkedin.venice.fastclient.meta;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
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

  @Test
  public void testGetReplicasWithAllHealthyReplicas() {
    String[] instances = new String[] { instance1, instance2, instance3 };
    List<String> replicas = Arrays.asList(instances);
    InstanceHealthMonitor instanceHealthMonitor = mockInstanceHealthyMonitor(
        instances,
        new boolean[] { false, false, false },
        new boolean[] { true, true, true },
        new int[] { 0, 0, 0 });
    runTest(instanceHealthMonitor, replicas, 0, 2, Arrays.asList(instance1, instance2));
    runTest(instanceHealthMonitor, replicas, 1, 2, Arrays.asList(instance2, instance3));
    runTest(instanceHealthMonitor, replicas, 2, 2, Arrays.asList(instance3, instance1));
    runTest(instanceHealthMonitor, replicas, 3, 2, Arrays.asList(instance1, instance2));
  }

  @Test
  public void testGetReplicasWithAllHealthyReplicasWithDifferentWeights() {
    String[] instances = new String[] { instance1, instance2, instance3 };
    List<String> replicas = Arrays.asList(instances);
    InstanceHealthMonitor instanceHealthMonitor = mockInstanceHealthyMonitor(
        instances,
        new boolean[] { false, false, false },
        new boolean[] { true, true, true },
        new int[] { 5, 5, 4 });
    runTest(instanceHealthMonitor, replicas, 0, 2, Arrays.asList(instance3, instance1));
    runTest(instanceHealthMonitor, replicas, 1, 2, Arrays.asList(instance3, instance2));
    runTest(instanceHealthMonitor, replicas, 2, 2, Arrays.asList(instance3, instance1));
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
        new int[] { 100, 1, 2, 3, 4, 2 });
    runTest(instanceHealthMonitor, replicas, 0, 2, Arrays.asList(instance2, instance3, instance6));
  }
}
