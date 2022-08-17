package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestStoreStatusDecider {
  private List<Store> storeList;
  private ResourceAssignment resourceAssignment;
  private PushMonitor mockPushMonitor = Mockito.mock(PushMonitor.class);
  private int replicationFactor = 3;

  @BeforeMethod
  public void setUp() {
    storeList = new ArrayList<>();
    resourceAssignment = new ResourceAssignment();

    OfflinePushStatus mockPushStatus = Mockito.mock(OfflinePushStatus.class);
    Mockito.doReturn(mockPushStatus).when(mockPushMonitor).getOfflinePushOrThrow(Mockito.any());
  }

  @Test
  public void testGetFullyReplicatedStoreStatus() {
    int partitionCount = 2;

    prepare(partitionCount, new int[] { replicationFactor, replicationFactor });
    for (String status: StoreStatusDecider.getStoreStatues(storeList, resourceAssignment, mockPushMonitor).values()) {
      Assert.assertEquals(status, StoreStatus.FULLLY_REPLICATED.toString(), "Store should be fully replicated.");
    }
  }

  @Test
  public void testGetUnavailableStoreStatus() {
    for (String status: StoreStatusDecider.getStoreStatues(storeList, resourceAssignment, mockPushMonitor).values()) {
      Assert.assertEquals(
          status,
          StoreStatus.UNAVAILABLE.toString(),
          "Store should be unavailable, because there is not version in that store.");
    }
  }

  @Test
  public void testGetDegradedStoreStatusMissingPartition() {
    int partitionCount = 3;

    prepare(partitionCount, new int[] { replicationFactor, replicationFactor });
    for (String status: StoreStatusDecider.getStoreStatues(storeList, resourceAssignment, mockPushMonitor).values()) {
      Assert.assertEquals(
          status,
          StoreStatus.DEGRADED.toString(),
          "Store should be degraded because missing one partition.");
    }
  }

  @Test
  public void testGetDegradedStoreStatusMissingReplicas() {
    int partitionCount = 2;

    prepare(partitionCount, new int[] { replicationFactor, 0 });
    for (String status: StoreStatusDecider.getStoreStatues(storeList, resourceAssignment, mockPushMonitor).values()) {
      Assert.assertEquals(
          status,
          StoreStatus.DEGRADED.toString(),
          "Store should be degraded because one partition does not have any online replica.");
    }
  }

  @Test
  public void testGetUnderReplicatedStoreStatus() {
    int partitionCount = 2;

    prepare(partitionCount, new int[] { replicationFactor - 1, replicationFactor - 1 });
    for (String status: StoreStatusDecider.getStoreStatues(storeList, resourceAssignment, mockPushMonitor).values()) {
      Assert.assertEquals(
          status,
          StoreStatus.UNDER_REPLICATED.toString(),
          "Store should be under replicated because each partition only has replicaFactor -1 replicas.");
    }
  }

  protected void prepare(int partitionCount, int[] onlineReplicasCounts) {
    int currentVersion = 10;
    int storeCount = 2;

    for (int i = 0; i < storeCount; i++) {
      Store store = TestUtils.createTestStore("testStore" + i, "test", System.currentTimeMillis());
      store.setCurrentVersion(currentVersion);
      storeList.add(store);
      String resource = Version.composeKafkaTopic("testStore" + i, currentVersion);
      PartitionAssignment partitionAssignment = createPartitions(resource, partitionCount, onlineReplicasCounts);
      resourceAssignment.setPartitionAssignment(resource, partitionAssignment);
    }
  }

  protected PartitionAssignment createPartitions(String resourceName, int partitionCount, int[] onlineReplicaCounts) {
    PartitionAssignment partitionAssignment = new PartitionAssignment(resourceName, partitionCount);
    for (int i = 0; i < onlineReplicaCounts.length; i++) {
      Partition p = Mockito.mock(Partition.class);
      Mockito.doReturn(i).when(p).getId();
      List<Instance> instanceList = Mockito.mock(List.class);
      Mockito.doReturn(onlineReplicaCounts[i]).when(instanceList).size();
      Mockito.doReturn(instanceList).when(mockPushMonitor).getReadyToServeInstances(partitionAssignment, i);
      partitionAssignment.addPartition(p);
    }
    return partitionAssignment;
  }
}
