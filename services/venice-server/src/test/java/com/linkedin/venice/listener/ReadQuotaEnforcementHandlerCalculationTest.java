package com.linkedin.venice.listener;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ReadQuotaEnforcementHandlerCalculationTest {
  private String nodeId = "thisNodeId";

  @Test
  public void onePartitionFourReplicas() {
    // one partition, four replicas, one replica on this host. Portion should be 1/4
    PartitionAssignment pa = getPartitionAssignment("topic", nodeId, new int[] { 4 }, new int[] { 1 });
    HelixCustomizedViewOfflinePushRepository cvRepository = getCVRepository("topic", nodeId, pa, new int[] { 4 });
    double portion = ReadQuotaEnforcementHandler.getNodeResponsibilityForQuota(cvRepository, pa, nodeId);
    Assert.assertEquals(portion, 0.25d);
  }

  @Test
  void twoPartitionsTwoAndThreeReplicas() {
    PartitionAssignment pa = getPartitionAssignment("topic", nodeId, new int[] { 3, 3 }, new int[] { 1, 1 });
    HelixCustomizedViewOfflinePushRepository cvRepository = getCVRepository("topic", nodeId, pa, new int[] { 2, 3 });
    double portion = ReadQuotaEnforcementHandler.getNodeResponsibilityForQuota(cvRepository, pa, nodeId);
    Assert.assertEquals(portion, 0.41666666666666663d); // (1/2 + 1/3)/2
  }

  @Test
  void twoPartitionsTwoAndThreeReplicasOnlyTwoReplicasLocally() {
    PartitionAssignment pa = getPartitionAssignment("topic", nodeId, new int[] { 3, 3, 3 }, new int[] { 1, 1, 0 });
    HelixCustomizedViewOfflinePushRepository cvRepository = getCVRepository("topic", nodeId, pa, new int[] { 2, 3, 3 });
    double portion = ReadQuotaEnforcementHandler.getNodeResponsibilityForQuota(cvRepository, pa, nodeId);
    Assert.assertEquals(portion, 0.27777777777777773d); // (1/2 + 1/3 + 0/3)/3
  }

  /**
   * If a store as 3 partitions,
   *   partitions 0 and 1 have 3 online replicas,
   *   partition 2 has 2 online replicas,
   *   and this node has a replica for partitions 0 and 2, then
   *   onlineReplicaCounts = [3,3,2]
   *   replicasOnThisNode = [1,0,1]
   *
   * @param topic
   * @param thisNodeId
   * @param onlineReplicaCounts
   * @param replicasOnThisNode
   * @return
   */
  public static PartitionAssignment getPartitionAssignment(
      String topic,
      String thisNodeId,
      int[] onlineReplicaCounts,
      int[] replicasOnThisNode) {
    if (onlineReplicaCounts.length != replicasOnThisNode.length) {
      throw new RuntimeException(
          "onlineReplicaCounts and replicasOnThisNode must have the same number of elements.  This is the number of partitions");
    }
    PartitionAssignment partitionAssignment = mock(PartitionAssignment.class);
    doReturn(topic).when(partitionAssignment).getTopic();
    Instance thisInstance = new Instance(thisNodeId, "dummyHost", 1234);
    List<Partition> partitions = new ArrayList<>();
    for (int p = 0; p < onlineReplicaCounts.length; p++) {
      Partition partition = mock(Partition.class);
      List<Instance> instances = new ArrayList<>();
      for (int i = 0; i < onlineReplicaCounts[p]; i++) {
        if (i == 0 && replicasOnThisNode[p] > 0) {
          instances.add(thisInstance);
        } else {
          instances.add(new Instance(Utils.getUniqueString("nodeid"), "dummyHost-" + i, 1234));
        }
      }
      doReturn(p).when(partition).getId();
      doReturn(instances).when(partition).getWorkingInstances();
      partitions.add(partition);
    }
    doReturn(partitions).when(partitionAssignment).getAllPartitions();
    return partitionAssignment;
  }

  public static HelixCustomizedViewOfflinePushRepository getCVRepository(
      String topic,
      String thisNodeId,
      PartitionAssignment partitionAssignment,
      int[] readyToServeReplicas) {
    HelixCustomizedViewOfflinePushRepository customizedViewOfflinePushRepository =
        mock(HelixCustomizedViewOfflinePushRepository.class);
    for (Partition p: partitionAssignment.getAllPartitions()) {
      List<ReplicaState> replicaStates = new ArrayList<>();
      int readyToServeReplicaCount = readyToServeReplicas[p.getId()];
      for (Instance i: p.getWorkingInstances()) {
        ReplicaState thisReplicaState = mock(ReplicaState.class);
        doReturn(i.getNodeId()).when(thisReplicaState).getParticipantId();
        if (thisNodeId.equals(i.getNodeId())) {
          doReturn(true).when(thisReplicaState).isReadyToServe();
          replicaStates.add(thisReplicaState);
          readyToServeReplicaCount -= 1;
          continue;
        }
        if (readyToServeReplicaCount > 0) {
          doReturn(true).when(thisReplicaState).isReadyToServe();
          readyToServeReplicaCount -= 1;
        } else {
          doReturn(false).when(thisReplicaState).isReadyToServe();
        }
        replicaStates.add(thisReplicaState);
      }
      when(customizedViewOfflinePushRepository.getReplicaStates(topic, p.getId())).thenReturn(replicaStates);
    }
    return customizedViewOfflinePushRepository;
  }
}
