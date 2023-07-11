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


public class WaitNMinusOnePushStatusDeciderTest extends TestPushStatusDecider {
  private final WaitNMinusOnePushStatusDecider statusDecider = new WaitNMinusOnePushStatusDecider();

  @BeforeMethod
  public void setUp() {
    topic = "WaitNMinusOnePushStatusDeciderTest";
    partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    createPartitions(numberOfPartition, replicationFactor);
  }

  @Test
  public void testGetPartitionStatus() {
    PartitionStatus partitionStatus = new PartitionStatus(0);

    Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
    Instance instance0 = new Instance("instance0", "host0", 1);
    Instance instance1 = new Instance("instance1", "host0", 1);
    Instance instance2 = new Instance("instance2", "host0", 1);
    Instance instance3 = new Instance("instance3", "host0", 1);

    instanceToStateMap.put(instance0, HelixState.STANDBY);
    instanceToStateMap.put(instance1, HelixState.STANDBY);
    instanceToStateMap.put(instance2, HelixState.LEADER);

    // Not enough replicas
    partitionStatus.updateReplicaStatus("instance0", COMPLETED);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        STARTED);

    // have n - 1 nodes finished, but leader is still bootstrapping
    partitionStatus.updateReplicaStatus("instance1", COMPLETED);
    partitionStatus.updateReplicaStatus("instance2", STARTED);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        STARTED);

    // have n - 1 nodes finished (include leader)
    partitionStatus.updateReplicaStatus("instance1", STARTED);
    partitionStatus.updateReplicaStatus("instance2", COMPLETED);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        COMPLETED);

    // all the replicas have finished
    partitionStatus.updateReplicaStatus("instance1", COMPLETED);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        COMPLETED);

    // one of the replicas failed
    partitionStatus.updateReplicaStatus("instance1", ERROR);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        COMPLETED);

    // another has also failed
    partitionStatus.updateReplicaStatus("instance0", ERROR);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        ERROR);

    // a new replica joined but yet registered in external view
    partitionStatus.updateReplicaStatus("instance3", STARTED);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        ERROR);

    partitionStatus.updateReplicaStatus("instance2", ERROR);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        ERROR);

    // new replica has finished
    partitionStatus.updateReplicaStatus("instance1", COMPLETED);
    partitionStatus.updateReplicaStatus("instance2", COMPLETED);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        COMPLETED);
    // offline instance in error, job will still finish
    instanceToStateMap.put(instance3, HelixState.OFFLINE);
    partitionStatus.updateReplicaStatus("instance3", ERROR);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        COMPLETED);
    // follower nodes are in error, disabled in error. job will fail
    partitionStatus.updateReplicaStatus("instance0", ERROR);
    partitionStatus.updateReplicaStatus("instance1", ERROR);
    Assert.assertEquals(
        statusDecider
            .getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, getDisableReplicaCallback("")),
        ERROR);
  }

  private static DisableReplicaCallback getDisableReplicaCallback(String kafkaTopic) {
    DisableReplicaCallback callback = new DisableReplicaCallback() {
      @Override
      public void disableReplica(String instance, int partitionId) {
      }

      @Override
      public boolean isReplicaDisabled(String instance, int partitionId) {
        return instance.equals("instance3");
      }
    };
    return callback;
  }
}
