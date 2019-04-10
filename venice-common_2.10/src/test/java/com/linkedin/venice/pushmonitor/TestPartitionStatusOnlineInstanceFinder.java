package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPartitionStatusOnlineInstanceFinder {
  private OfflinePushAccessor offlinePushAccessor = Mockito.mock(OfflinePushAccessor.class);
  private RoutingDataRepository routingDataRepo = Mockito.mock(RoutingDataRepository.class);

  private String testTopic = "testTopic";
  private int testPartition = 0;

  @Test
  public void testCanGetReadyToServeInstances() {
    PartitionStatusOnlineInstanceFinder finder = initFinder();
    List<Instance> onlineInstanceList = finder.getReadyToServeInstances(testTopic, testPartition);

    Assert.assertEquals(onlineInstanceList.size(), 1);
    Assert.assertEquals(onlineInstanceList.get(0).getNodeId(), "host1_1");
  }

  @Test
  public void testCanUpdateAccordingPartitionChanges() {
    PartitionStatusOnlineInstanceFinder finder = initFinder();

    PartitionStatus partitionStatus = new PartitionStatus(0);
    ReplicaStatus host0 = new ReplicaStatus("host0_1");
    ReplicaStatus host1 = new ReplicaStatus("host1_1");
    ReplicaStatus host2 = new ReplicaStatus("host2_1");
    host0.updateStatus(ExecutionStatus.COMPLETED);
    host1.updateStatus(ExecutionStatus.STARTED);
    host2.updateStatus(ExecutionStatus.COMPLETED);
    partitionStatus.setReplicaStatuses(Arrays.asList(host0, host1, host2));

    finder.onPartitionStatusChange(testTopic, ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));
    List<Instance> onlineInstanceList = finder.getReadyToServeInstances(testTopic, testPartition);

    //since host2 is not listed in RoutingDataRepo, it's supposed to be excluded from online instance list
    Assert.assertEquals(onlineInstanceList.size(), 1);
    Assert.assertEquals(onlineInstanceList.get(0).getNodeId(), "host0_1");

    host1.updateStatus(ExecutionStatus.COMPLETED);
    onlineInstanceList = finder.getReadyToServeInstances(testTopic, testPartition);
    Assert.assertEquals(onlineInstanceList.size(), 2);
  }

  private PartitionStatusOnlineInstanceFinder initFinder() {
    PartitionStatusOnlineInstanceFinder finder =
        new PartitionStatusOnlineInstanceFinder(offlinePushAccessor, routingDataRepo);
    Mockito.doReturn(getMockInstances()).when(routingDataRepo).getAllInstances(testTopic, 0);
    Mockito.doReturn(new PartitionAssignment(testTopic, 1)).when(routingDataRepo).getPartitionAssignments(testTopic);
    Mockito.doReturn(getMockPushStatus()).when(offlinePushAccessor).loadOfflinePushStatusesAndPartitionStatuses();

    finder.refresh();

    return finder;
  }

  private Map<String, List<Instance>> getMockInstances() {
    Map<String, List<Instance>> instanceMap = new HashMap<>();
    instanceMap.put(HelixState.LEADER_STATE, Arrays.asList(new Instance("host0_1", "host0", 1)));
    instanceMap.put(HelixState.STANDBY_STATE, Arrays.asList(new Instance("host1_1", "host1", 1)));


    return instanceMap;
  }

  private List<OfflinePushStatus> getMockPushStatus() {
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(testTopic,
        1, 2, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    PartitionStatus partitionStatus = new PartitionStatus(0);
    ReplicaStatus host0 = new ReplicaStatus("host0_1");
    ReplicaStatus host1 = new ReplicaStatus("host1_1");
    host1.updateStatus(ExecutionStatus.COMPLETED);
    partitionStatus.setReplicaStatuses(Arrays.asList(host0, host1));
    offlinePushStatus.setPartitionStatus(partitionStatus);

    return Arrays.asList(offlinePushStatus);
  }
}
