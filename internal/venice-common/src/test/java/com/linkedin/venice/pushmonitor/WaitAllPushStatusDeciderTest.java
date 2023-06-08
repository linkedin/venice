package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class WaitAllPushStatusDeciderTest extends TestPushStatusDecider {
  private WaitAllPushStatusDecider statusDecider = new WaitAllPushStatusDecider();

  @BeforeMethod
  public void setUp() {
    topic = "WaitAllPushStatusDeciderTest";
    partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    createPartitions(numberOfPartition, replicationFactor);
  }

  @Test
  public void testGetPartitionStatus() {
    PartitionStatus partitionStatus = new PartitionStatus(0);

    Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
    instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
    instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
    instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);

    // Not enough replicas
    partitionStatus.updateReplicaStatus("instance0", COMPLETED);
    partitionStatus.updateReplicaStatus("instance1", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // have enough replicas, but one of them hasn't finished yet
    partitionStatus.updateReplicaStatus("instance2", STARTED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // all the replicas have finished
    partitionStatus.updateReplicaStatus("instance2", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        COMPLETED);

    // one of the replicas failed
    partitionStatus.updateReplicaStatus("instance1", ERROR);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        ERROR);

    // a new replica joined but yet registered in external view
    partitionStatus.updateReplicaStatus("instance3", STARTED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        ERROR);

    instanceToStateMap.put(new Instance("instance3", "host3", 1), HelixState.STANDBY);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // new replica has finished
    partitionStatus.updateReplicaStatus("instance3", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        COMPLETED);
  }
}
