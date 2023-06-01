package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestPartition {
  private EnumMap<HelixState, List<Instance>> stateToInstancesMap = new EnumMap<>(HelixState.class);
  private Partition p;
  private int id = 0;

  @BeforeMethod
  public void setUp() {
    // ONLINE instance
    ArrayList<Instance> onlineInstances = new ArrayList<>();
    onlineInstances.add(new Instance("localhost:1001", "localhost", 1001));
    stateToInstancesMap.put(HelixState.ONLINE, onlineInstances);
    // Bootstrap instance
    ArrayList<Instance> bootstrapInstances = new ArrayList<>();
    bootstrapInstances.add(new Instance("localhost:1002", "localhost", 1002));
    stateToInstancesMap.put(HelixState.BOOTSTRAP, bootstrapInstances);
    // Error instance
    ArrayList<Instance> errorInstance = new ArrayList<>();
    errorInstance.add(new Instance("localhost:1003", "localhost", 1003));
    stateToInstancesMap.put(HelixState.ERROR, errorInstance);
    // no offline instances.

    p = new Partition(id, stateToInstancesMap, new EnumMap<>(ExecutionStatus.class));
  }

  @Test
  public void testGetInstances() {
    Assert.assertEquals(p.getId(), id);
    Assert.assertEquals(p.getInstancesInState(HelixState.ONLINE), p.getReadyToServeInstances());
    Assert.assertEquals(
        p.getInstancesInState(HelixState.ONLINE).size(),
        stateToInstancesMap.get(HelixState.ONLINE).size());

    Assert.assertEquals(p.getInstancesInState(HelixState.BOOTSTRAP), p.getBootstrapInstances());
    Assert.assertEquals(
        p.getInstancesInState(HelixState.BOOTSTRAP).size(),
        stateToInstancesMap.get(HelixState.BOOTSTRAP).size());

    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR), p.getErrorInstances());
    Assert
        .assertEquals(p.getInstancesInState(HelixState.ERROR).size(), stateToInstancesMap.get(HelixState.ERROR).size());

    Assert.assertEquals(p.getInstancesInState(HelixState.OFFLINE).size(), 0);

    Assert.assertEquals(
        p.getWorkingInstances().size(),
        stateToInstancesMap.get(HelixState.ONLINE).size() + stateToInstancesMap.get(HelixState.BOOTSTRAP).size());
  }

  @Test
  public void testGetInstanceStatusById() {
    Assert.assertEquals(p.getInstancesInState(HelixState.ONLINE).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.ONLINE).get(0).getPort(), 1001);

    Assert.assertEquals(p.getInstancesInState(HelixState.BOOTSTRAP).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.BOOTSTRAP).get(0).getPort(), 1002);

    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR).get(0).getPort(), 1003);

    Assert.assertEquals(p.getInstancesInState(HelixState.OFFLINE).size(), 0);
  }

  @Test
  public void testWithRemovedInstance() {
    Partition newPartition = p.withRemovedInstance("localhost:1001");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.ONLINE).size(), 0);
    newPartition = newPartition.withRemovedInstance("localhost:1002");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.BOOTSTRAP).size(), 0);
    newPartition = newPartition.withRemovedInstance("localhost:1003");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.OFFLINE).size(), 0);
  }
}
