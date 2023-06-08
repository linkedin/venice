package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestPartition {
  private EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
  private EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
  private Partition p;
  private int id = 0;

  @BeforeMethod
  public void setUp() {
    Instance instance1 = new Instance("localhost:1001", "localhost", 1001);
    Instance instance2 = new Instance("localhost:1002", "localhost", 1002);
    Instance instance3 = new Instance("localhost:1003", "localhost", 1003);

    helixStateToInstancesMap.put(HelixState.LEADER, Arrays.asList(instance1));
    helixStateToInstancesMap.put(HelixState.STANDBY, Arrays.asList(instance2));
    helixStateToInstancesMap.put(HelixState.ERROR, Arrays.asList(instance3));

    executionStatusToInstancesMap.put(ExecutionStatus.COMPLETED, Arrays.asList(instance1));
    executionStatusToInstancesMap.put(ExecutionStatus.STARTED, Arrays.asList(instance2));
    executionStatusToInstancesMap.put(ExecutionStatus.ERROR, Arrays.asList(instance3));

    // no offline instances.

    p = new Partition(id, helixStateToInstancesMap, executionStatusToInstancesMap);
  }

  @Test
  public void testGetInstances() {
    Assert.assertEquals(p.getId(), id);

    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR), p.getErrorInstances());
    Assert.assertEquals(
        p.getInstancesInState(HelixState.ERROR).size(),
        helixStateToInstancesMap.get(HelixState.ERROR).size());

    Assert.assertEquals(p.getInstancesInState(HelixState.OFFLINE).size(), 0);

    Assert.assertEquals(
        p.getWorkingInstances().size(),
        helixStateToInstancesMap.get(HelixState.LEADER).size()
            + helixStateToInstancesMap.get(HelixState.STANDBY).size());
  }

  @Test
  public void testGetInstanceStatusById() {
    Assert.assertEquals(p.getInstancesInState(HelixState.LEADER).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.LEADER).get(0).getPort(), 1001);

    Assert.assertEquals(p.getInstancesInState(HelixState.STANDBY).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.STANDBY).get(0).getPort(), 1002);

    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR).size(), 1);
    Assert.assertEquals(p.getInstancesInState(HelixState.ERROR).get(0).getPort(), 1003);

    Assert.assertEquals(p.getInstancesInState(ExecutionStatus.COMPLETED).size(), 1);
    Assert.assertEquals(p.getInstancesInState(ExecutionStatus.COMPLETED).get(0).getPort(), 1001);

    Assert.assertEquals(p.getInstancesInState(ExecutionStatus.STARTED).size(), 1);
    Assert.assertEquals(p.getInstancesInState(ExecutionStatus.STARTED).get(0).getPort(), 1002);

    Assert.assertEquals(p.getInstancesInState(ExecutionStatus.ERROR).size(), 1);
    Assert.assertEquals(p.getInstancesInState(ExecutionStatus.ERROR).get(0).getPort(), 1003);

  }

  @Test
  public void testWithRemovedInstance() {
    Partition newPartition = p.withRemovedInstance("localhost:1001");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.LEADER).size(), 0);
    Assert.assertEquals(newPartition.getInstancesInState(ExecutionStatus.COMPLETED).size(), 0);

    newPartition = newPartition.withRemovedInstance("localhost:1002");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.STANDBY).size(), 0);
    Assert.assertEquals(newPartition.getInstancesInState(ExecutionStatus.STARTED).size(), 0);

    newPartition = newPartition.withRemovedInstance("localhost:1003");
    Assert.assertEquals(newPartition.getInstancesInState(HelixState.ERROR).size(), 0);
    Assert.assertEquals(newPartition.getInstancesInState(ExecutionStatus.ERROR).size(), 0);
  }
}
