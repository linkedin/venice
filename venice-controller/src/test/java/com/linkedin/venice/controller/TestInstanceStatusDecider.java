package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixRoutingDataRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushMonitor;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestInstanceStatusDecider {
  private VeniceHelixResources resources;
  private String clusterName;
  private HelixDataAccessor accessor;
  private HelixRoutingDataRepository routingDataRepository;
  private HelixReadWriteStoreRepository readWriteStoreRepository;
  private String storeName = "TestInstanceStatusDecider";
  private int version = 1;
  private String resourceName = Version.composeKafkaTopic(storeName, version);
  private OfflinePushMonitor mockMontior;

  @BeforeMethod
  public void setup() {
    clusterName = TestUtils.getUniqueString("TestInstanceStatusDecider");
    resources = Mockito.mock(VeniceHelixResources.class);
    routingDataRepository = Mockito.mock(HelixRoutingDataRepository.class);
    readWriteStoreRepository = Mockito.mock(HelixReadWriteStoreRepository.class);
    mockMontior = Mockito.mock(OfflinePushMonitor.class);
    HelixManager manager = Mockito.mock(HelixManager.class);
    accessor = Mockito.mock(HelixDataAccessor.class);
    Mockito.doReturn(routingDataRepository).when(resources).getRoutingDataRepository();
    Mockito.doReturn(readWriteStoreRepository).when(resources).getMetadataRepository();
    Mockito.doReturn(mockMontior).when(resources).getOfflinePushMonitor();
    Mockito.doReturn(manager).when(resources).getController();
    Mockito.doReturn(accessor).when(manager).getHelixDataAccessor();
    Mockito.doReturn(new LiveInstance("test")).when(accessor).getProperty(Mockito.any(PropertyKey.class));
  }

  @Test
  public void testIsRemovableNonLiveInstance() {
    Mockito.doReturn(null).when(accessor).getProperty(Mockito.any(PropertyKey.class));
    Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, "test", 1),
        "A non-alive instance should be removable from cluster");
  }

  /**
   * Test is instance removable for a ready to serve version. If removing would cause any data loss, we can not remove
   * that instance.
   */
  @Test
  public void testIsRemovableLossData() {
    String onlineInstanceId = "localhost_1";
    String bootstrapInstanceId = "localhost_2";
    PartitionAssignment partitionAssignment = prepareAssignments(resourceName);
    Map<String, List<Instance>> statusToInstancesMap = new HashMap<>();
    List<Instance> onlineInstances = new ArrayList<>();
    onlineInstances.add(Instance.fromNodeId(onlineInstanceId));
    List<Instance> bootstrapInstances = new ArrayList<>();
    bootstrapInstances.add(Instance.fromNodeId(bootstrapInstanceId));
    statusToInstancesMap.put(HelixState.ONLINE_STATE, onlineInstances);
    statusToInstancesMap.put(HelixState.BOOTSTRAP_STATE, bootstrapInstances);
    partitionAssignment.addPartition(new Partition(0, statusToInstancesMap));

    // Test the completed push.
    VersionStatus[] statuses = new VersionStatus[]{VersionStatus.ONLINE, VersionStatus.PUSHED};
    for (VersionStatus status : statuses) {
      prepareStoreAndVersion(storeName, version, status);
      Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, bootstrapInstanceId, 1),
          bootstrapInstanceId + "could be removed because it's not the last online copy.");
      Assert.assertFalse(InstanceStatusDecider.isRemovable(resources, clusterName, onlineInstanceId, 1),
          onlineInstanceId + "could NOT be removed because it the last online copy.");
    }
  }

  /**
   * Test is instance removable during the push. If removing would fail the running job, we can not remove that
   * instance.
   */
  @Test
  public void testIsRemovableFailJob() {
    String instanceId = "localhost_1";
    Map<String, List<Instance>> statusToInstancesMap = new HashMap<>();
    List<Instance> instances = new ArrayList<>();
    instances.add(Instance.fromNodeId(instanceId));
    statusToInstancesMap.put(HelixState.BOOTSTRAP_STATE, instances);
    PartitionAssignment partitionAssignment = prepareAssignments(resourceName);
    partitionAssignment.addPartition(new Partition(0, statusToInstancesMap));
    // Test for the running push
    prepareStoreAndVersion(storeName, version, VersionStatus.STARTED);

    Mockito.doReturn(true).when(mockMontior).wouldJobFail(Mockito.eq(resourceName), Mockito.any());
    Assert.assertFalse(InstanceStatusDecider.isRemovable(resources, clusterName, instanceId, 1),
        "Can not remove instance because job would fail.");

    Mockito.doReturn(false).when(mockMontior).wouldJobFail(Mockito.eq(resourceName), Mockito.any());
    Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, instanceId, 1),
        "Instance could be removed because it will not fail the job.");
  }

  /**
   * Test is instance removable for the ready to serve version. If removing would cause re-balance, we can remove that
   * instance.
   */
  @Test
  public void testIsRemovableTriggerRebalance() {
    int replicationFactor = 3;
    Map<String, List<Instance>> statusToInstancesMap = new HashMap<>();
    List<Instance> instances = new ArrayList<>();
    for (int i = 1; i <= replicationFactor; i++) {
      instances.add(Instance.fromNodeId("localhost_" + i));
    }
    statusToInstancesMap.put(HelixState.ONLINE_STATE, instances);
    PartitionAssignment partitionAssignment = prepareAssignments(resourceName);
    partitionAssignment.addPartition(new Partition(0, statusToInstancesMap));

    VersionStatus[] statuses = new VersionStatus[]{VersionStatus.ONLINE, VersionStatus.PUSHED};
    for (VersionStatus status : statuses) {
      prepareStoreAndVersion(storeName, version, status);
      Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_1", 2),
          "Instance should be removable because after removing one instance, there are still 2 active replicas, it will not trigger re-balance.");
    }

    // Remove one instance
    instances.remove(instances.size() - 1);
    statusToInstancesMap.put(HelixState.ONLINE_STATE, instances);
    partitionAssignment.addPartition(new Partition(0, statusToInstancesMap));
    for (VersionStatus status : statuses) {
      prepareStoreAndVersion(storeName, version, status);
      Assert.assertFalse(InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_1", 2),
          "Instance should NOT be removable because after removing one instance, there are only 1 active replica, it will not trigger re-balance.");
    }
  }

  private PartitionAssignment prepareAssignments(String resourceName) {
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    PartitionAssignment partitionAssignment = new PartitionAssignment(resourceName, 1);
    resourceAssignment.setPartitionAssignment(resourceName, partitionAssignment);
    Mockito.doReturn(resourceAssignment).when(routingDataRepository).getResourceAssignment();
    return partitionAssignment;
  }

  private void prepareStoreAndVersion(String storeName, int version, VersionStatus status) {
    Store store = TestUtils.createTestStore(storeName, "t", 0);
    Version version1 = new Version(storeName, version);
    version1.setStatus(status);
    store.addVersion(version1);
    Mockito.doReturn(store).when(readWriteStoreRepository).getStore(storeName);
  }
}
