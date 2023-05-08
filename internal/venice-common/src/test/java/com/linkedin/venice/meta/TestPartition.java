package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixState;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestPartition {
  private Map<String, List<Instance>> stateToInstancesMap = new HashMap<>();
  private Partition p;
  private int id = 0;

  @BeforeMethod
  public void setUp() {
    // ONLINE instance
    ArrayList<Instance> onlineInstances = new ArrayList<>();
    onlineInstances.add(new Instance("localhost:1001", "localhost", 1001));
    stateToInstancesMap.put(HelixState.ONLINE_STATE, onlineInstances);
    // Bootstrap instance
    ArrayList<Instance> bootstrapInstances = new ArrayList<>();
    bootstrapInstances.add(new Instance("localhost:1002", "localhost", 1002));
    stateToInstancesMap.put(HelixState.BOOTSTRAP_STATE, bootstrapInstances);
    // Error instance
    ArrayList<Instance> errorInstance = new ArrayList<>();
    errorInstance.add(new Instance("localhost:1003", "localhost", 1003));
    stateToInstancesMap.put(HelixState.ERROR_STATE, errorInstance);
    // no offline instances.

    p = new Partition(id, stateToInstancesMap);
  }

  @Test
  public void testGetInstances() {
    Assert.assertEquals(p.getId(), id);
    Assert.assertEquals(p.getInstancesInState(HelixState.ONLINE_STATE), p.getReadyToServeInstances());
    Assert.assertEquals(
        p.getInstancesInState(HelixState.ONLINE_STATE).size(),
        stateToInstancesMap.get(HelixState.ONLINE_STATE).size());

    Assert.assertEquals(p.getInstancesInState(HelixState.BOOTSTRAP_STATE), p.getBootstrapInstances());
    Assert.assertEquals(
        p.getInstancesInState(HelixState.BOOTSTRAP_STATE).size(),
        stateToInstancesMap.get(HelixState.BOOTSTRAP_STATE).size());

    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR_STATE), p.getErrorInstances());
    Assert.assertEquals(
        p.getInstancesInState(HelixState.ERROR_STATE).size(),
        stateToInstancesMap.get(HelixState.ERROR_STATE).size());

    Assert.assertEquals(p.getInstancesInState(HelixState.OFFLINE_STATE).size(), 0);

    Assert.assertEquals(
        p.getWorkingInstances().size(),
        stateToInstancesMap.get(HelixState.ONLINE_STATE).size()
            + stateToInstancesMap.get(HelixState.BOOTSTRAP_STATE).size());
  }

  @Test
  public void testGetInstanceStatusById() {
    Assert.assertEquals(p.getInstancesInState(HelixState.ONLINE_STATE).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.ONLINE_STATE).get(0).getPort(), 1001);

    Assert.assertEquals(p.getInstancesInState(HelixState.BOOTSTRAP_STATE).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.BOOTSTRAP_STATE).get(0).getPort(), 1002);

    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR_STATE).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR_STATE).get(0).getPort(), 1003);

    Assert.assertEquals(p.getInstancesInState(HelixState.OFFLINE_STATE).size(), 0);
  }

  @Test
  public void testWithRemovedInstance() {
    Partition newPartition = p.withRemovedInstance("localhost:1001");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.ONLINE_STATE).size(), 0);
    newPartition = newPartition.withRemovedInstance("localhost:1002");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.BOOTSTRAP_STATE).size(), 0);
    newPartition = newPartition.withRemovedInstance("localhost:1003");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.OFFLINE_STATE).size(), 0);
  }
}
