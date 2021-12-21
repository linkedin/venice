package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixDataAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class TestInstanceStatusDecider {
  private HelixVeniceClusterResources resources;
  private String clusterName;
  private SafeHelixDataAccessor accessor;
  private HelixExternalViewRepository routingDataRepository;
  private HelixReadWriteStoreRepository readWriteStoreRepository;
  private String storeName = "TestInstanceStatusDecider";
  private int version = 1;
  private String resourceName = Version.composeKafkaTopic(storeName, version);
  private PushMonitorDelegator mockMonitor;

  @BeforeMethod
  public void setup() {
    clusterName = Utils.getUniqueString("TestInstanceStatusDecider");
    resources = mock(HelixVeniceClusterResources.class);
    routingDataRepository = mock(HelixExternalViewRepository.class);
    readWriteStoreRepository = mock(HelixReadWriteStoreRepository.class);
    mockMonitor = mock(PushMonitorDelegator.class);

    doAnswer(invocation -> {
      PartitionAssignment partitionAssignment = invocation.getArgument(0);
      int partitionId = invocation.getArgument(1);

      return partitionAssignment.getPartition(partitionId).getReadyToServeInstances();
    }).when(mockMonitor).getReadyToServeInstances(any(PartitionAssignment.class), anyInt());

    SafeHelixManager manager = mock(SafeHelixManager.class);

    accessor = mock(SafeHelixDataAccessor.class);
    doReturn(routingDataRepository).when(resources).getRoutingDataRepository();
    doReturn(readWriteStoreRepository).when(resources).getStoreMetadataRepository();
    doReturn(mockMonitor).when(resources).getPushMonitor();
    doReturn(manager).when(resources).getHelixManager();
    doReturn(accessor).when(manager).getHelixDataAccessor();
    doReturn(new LiveInstance("test")).when(accessor).getProperty(any(PropertyKey.class));
  }

  @Test
  public void testIsRemovableNonLiveInstance() {
    doReturn(null).when(accessor).getProperty(any(PropertyKey.class));
    Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, "test").isRemovable(),
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
    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 2);
    Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, bootstrapInstanceId).isRemovable(),
        bootstrapInstanceId + "could be removed because it's not the last online copy.");
    Assert.assertFalse(InstanceStatusDecider.isRemovable(resources, clusterName, onlineInstanceId).isRemovable(),
        onlineInstanceId + "could NOT be removed because it the last online copy.");

  }

  @Test
  public void testIsRemovableLossDataInstanceView() {
    int partitionCount = 2;
    String instance1 = "localhost_1";
    String instance2 = "localhost_2";
    PartitionAssignment partitionAssignment = prepareMultiPartitionAssignments(resourceName, partitionCount);
    Map<String, List<Instance>> statusToInstancesMap = new HashMap<>();
    // Partition 0 have 2 online replicas
    List<Instance> onlineInstances = new ArrayList<>();
    onlineInstances.add(Instance.fromNodeId(instance1));
    onlineInstances.add(Instance.fromNodeId(instance2));
    statusToInstancesMap.put(HelixState.ONLINE_STATE, onlineInstances);
    partitionAssignment.addPartition(new Partition(0, statusToInstancesMap));
    // Partition 1 only have 1 bootstrap replica but no online replica.
    List<Instance> bootstrapInstances = new ArrayList<>();
    bootstrapInstances.add(Instance.fromNodeId(instance2));
    statusToInstancesMap = new HashMap<>();
    statusToInstancesMap.put(HelixState.BOOTSTRAP_STATE, bootstrapInstances);
    partitionAssignment.addPartition(new Partition(1, statusToInstancesMap));

    // Test the completed push.
    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 2);
    Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, instance1, true).isRemovable(),
        instance1 + "could be removed because it's not the last online copy in the instance's point of view.");
    Assert.assertFalse(InstanceStatusDecider.isRemovable(resources, clusterName, instance2, true).isRemovable(),
        instance2 + "could NOT be removed because it the last online copy in the instance's point of view.");
    Assert.assertFalse(InstanceStatusDecider.isRemovable(resources, clusterName, instance1).isRemovable(),
        instance1 + "could NOT be removed because in the cluster's point of view, partition 1 does not have any online replica alive.");

  }

  /**
   * Test if instance is removable during the push. If removing would fail the running job, we can not remove that
   * instance.
   */
  @Test
  public void testIsRemovableInFlightJob() {
    String instanceId = "localhost_1";
    Map<String, List<Instance>> statusToInstancesMap = new HashMap<>();
    List<Instance> instances = new ArrayList<>();
    instances.add(Instance.fromNodeId(instanceId));
    statusToInstancesMap.put(HelixState.BOOTSTRAP_STATE, instances);
    PartitionAssignment partitionAssignment = prepareAssignments(resourceName);
    partitionAssignment.addPartition(new Partition(0, statusToInstancesMap));
    // Test for the running push
    prepareStoreAndVersion(storeName, version, VersionStatus.STARTED, false, 1);

    doReturn(false).when(mockMonitor).wouldJobFail(eq(resourceName), any());
    NodeRemovableResult result = InstanceStatusDecider.isRemovable(resources, clusterName, instanceId);
    Assert.assertTrue(result.isRemovable(),
        "Instance should be removable because ongoing push shouldn't be a blocker.");
  }

  /**
   * Test if instance is removable for the ready to serve version. If removing would cause re-balance, we can remove that
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

    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 3);
    Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_1").isRemovable(),
        "Instance should be removable because after removing one instance, there are still 2 active replicas, it will not trigger re-balance.");


    // Remove one instance
    instances.remove(instances.size() - 1);
    statusToInstancesMap.put(HelixState.ONLINE_STATE, instances);
    partitionAssignment.addPartition(new Partition(0, statusToInstancesMap));
    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 3);
    NodeRemovableResult result = InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_1");
    Assert.assertFalse(result.isRemovable(),
        "Instance should NOT be removable because after removing one instance, there are only 1 active replica, it will not trigger re-balance.");
    Assert.assertEquals(result.getBlockingReason(),
        NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE.toString(),
        "Instance should NOT be removable because after removing one instance, there are only 1 active replica, it will not trigger re-balance.");

  }

  @Test
  public void testIsRemovableTriggerRebalanceInstanceView() {
    int partitionCount = 2;
    int replicationFactor = 3;
    PartitionAssignment partitionAssignment = prepareMultiPartitionAssignments(resourceName, partitionCount);

    Map<String, List<Instance>> statusToInstancesMap = new HashMap<>();
    List<Instance> instances = new ArrayList<>();
    for (int i = 1; i <= replicationFactor; i++) {
      instances.add(Instance.fromNodeId("localhost_" + i));
    }
    statusToInstancesMap.put(HelixState.ONLINE_STATE, instances);
    partitionAssignment.addPartition(new Partition(0, statusToInstancesMap));

    statusToInstancesMap = new HashMap<>();
    instances = new ArrayList<>();
    instances.add(Instance.fromNodeId("localhost_1"));
    statusToInstancesMap.put(HelixState.ONLINE_STATE, instances);
    partitionAssignment.addPartition(new Partition(1, statusToInstancesMap));


    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 3);
    Assert.assertTrue(InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_3", true).isRemovable(),
        "Instance should be removable because after removing one instance, there are 2 active replicas in partition 0 in instance's point of view, it will not trigger re-balance.");
    Assert.assertFalse(InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_3").isRemovable(),
        "Instance should NOT be removable because after removing one instance, there are only 1 active replicas in partition 1 in cluster's point of view, it will trigger re-balance.");

  }

  private PartitionAssignment prepareAssignments(String resourceName) {
    return prepareMultiPartitionAssignments(resourceName, 1);
  }

  private PartitionAssignment prepareMultiPartitionAssignments(String resourceName, int partitionCount) {
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    PartitionAssignment partitionAssignment = new PartitionAssignment(resourceName, partitionCount);
    resourceAssignment.setPartitionAssignment(resourceName, partitionAssignment);
    doReturn(resourceAssignment).when(routingDataRepository).getResourceAssignment();
    return partitionAssignment;
  }

  private void prepareStoreAndVersion(String storeName, int version, VersionStatus status) {
    Store store = TestUtils.createTestStore(storeName, "t", 0);
    Version version1 = new VersionImpl(storeName, version);
    version1.setStatus(status);
    store.addVersion(version1);
    doReturn(store).when(readWriteStoreRepository).getStore(storeName);
  }

  private void prepareStoreAndVersion(String storeName, int version, VersionStatus status, boolean isCurrentVersion,
      int replicationFactor) {
    Store store = TestUtils.createTestStore(storeName, "t", 0);
    Version version1 = new VersionImpl(storeName, version);
    version1.setReplicationFactor(replicationFactor);
    store.addVersion(version1);

    //we need to add the version to the store first before
    //updating its RF since #addVersion would auto-fill some
    //fields in the version (which would be an override in this
    //case)
    version1.setReplicationFactor(replicationFactor);
    if (isCurrentVersion) {
      store.setCurrentVersion(version);
    }

    doReturn(store).when(readWriteStoreRepository).getStore(storeName);

  }
}
