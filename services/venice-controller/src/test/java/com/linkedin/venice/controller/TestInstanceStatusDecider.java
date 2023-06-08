package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
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
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


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
  public void setUp() {
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
    Assert.assertTrue(
        InstanceStatusDecider.isRemovable(resources, clusterName, "test").isRemovable(),
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
    List<Instance> onlineInstances = new ArrayList<>();
    onlineInstances.add(Instance.fromNodeId(onlineInstanceId));
    List<Instance> bootstrapInstances = new ArrayList<>();
    bootstrapInstances.add(Instance.fromNodeId(bootstrapInstanceId));

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
    helixStateToInstancesMap.put(HelixState.LEADER, onlineInstances);
    helixStateToInstancesMap.put(HelixState.STANDBY, bootstrapInstances);

    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMap.put(ExecutionStatus.COMPLETED, onlineInstances);
    executionStatusToInstancesMap.put(ExecutionStatus.STARTED, bootstrapInstances);
    partitionAssignment.addPartition(new Partition(0, helixStateToInstancesMap, executionStatusToInstancesMap));

    // Test the completed push.
    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 2);
    NodeRemovableResult bootstrapInstanceIsRemovable =
        InstanceStatusDecider.isRemovable(resources, clusterName, bootstrapInstanceId);
    Assert.assertTrue(
        bootstrapInstanceIsRemovable.isRemovable(),
        bootstrapInstanceId + " could be removed because it's not the last online copy: "
            + bootstrapInstanceIsRemovable.getDetails());
    NodeRemovableResult onlineInstanceIsRemovable =
        InstanceStatusDecider.isRemovable(resources, clusterName, onlineInstanceId);
    Assert.assertFalse(
        onlineInstanceIsRemovable.isRemovable(),
        onlineInstanceId + " could NOT be removed because it the last online copy: "
            + onlineInstanceIsRemovable.getDetails());

  }

  @Test
  public void testIsRemovableLossDataInstanceView() {
    int partitionCount = 2;
    String instance1Name = "localhost_1";
    String instance2Name = "localhost_2";
    Instance instance1 = Instance.fromNodeId(instance1Name);
    Instance instance2 = Instance.fromNodeId(instance2Name);

    PartitionAssignment partitionAssignment = prepareMultiPartitionAssignments(resourceName, partitionCount);

    // Partition 0 have 2 online replicas
    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP0 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP0.put(HelixState.LEADER, Arrays.asList(instance1));
    helixStateToInstancesMapForP0.put(HelixState.STANDBY, Arrays.asList(instance2));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP0 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP0.put(ExecutionStatus.COMPLETED, Arrays.asList(instance1, instance2));
    partitionAssignment
        .addPartition(new Partition(0, helixStateToInstancesMapForP0, executionStatusToInstancesMapForP0));

    // Partition 1 only have 1 bootstrap replica but no online replica.
    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP1 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP1.put(HelixState.LEADER, Arrays.asList(instance2));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP1 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP1.put(ExecutionStatus.STARTED, Arrays.asList(instance2));
    partitionAssignment
        .addPartition(new Partition(1, helixStateToInstancesMapForP1, executionStatusToInstancesMapForP1));

    // Test the completed push.
    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 2);
    Assert.assertTrue(
        InstanceStatusDecider.isRemovable(resources, clusterName, instance1Name, Collections.emptyList(), true)
            .isRemovable(),
        instance1Name + "could be removed because it's not the last online copy in the instance's point of view.");
    Assert.assertFalse(
        InstanceStatusDecider.isRemovable(resources, clusterName, instance2Name, Collections.emptyList(), true)
            .isRemovable(),
        instance2Name + "could NOT be removed because it the last online copy in the instance's point of view.");
    Assert.assertFalse(
        InstanceStatusDecider.isRemovable(resources, clusterName, instance1Name).isRemovable(),
        instance1Name
            + "could NOT be removed because in the cluster's point of view, partition 1 does not have any online replica alive.");
  }

  /**
   * Test if instance is removable during the push. If removing would fail the running job, we can not remove that
   * instance.
   */
  @Test
  public void testIsRemovableInFlightJob() {
    String instanceId = "localhost_1";
    List<Instance> instances = new ArrayList<>();
    instances.add(Instance.fromNodeId(instanceId));

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
    helixStateToInstancesMap.put(HelixState.LEADER, instances);
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMap.put(ExecutionStatus.STARTED, instances);
    PartitionAssignment partitionAssignment = prepareAssignments(resourceName);
    partitionAssignment.addPartition(new Partition(0, helixStateToInstancesMap, executionStatusToInstancesMap));
    // Test for the running push
    prepareStoreAndVersion(storeName, version, VersionStatus.STARTED, false, 1);

    NodeRemovableResult result = InstanceStatusDecider.isRemovable(resources, clusterName, instanceId);
    Assert
        .assertTrue(result.isRemovable(), "Instance should be removable because ongoing push shouldn't be a blocker.");
  }

  /**
   * Test if instance is removable for the ready to serve version. If removing would cause re-balance, we can remove that
   * instance.
   */
  @Test
  public void testIsRemovableTriggerRebalance() {
    int replicationFactor = 3;
    List<Instance> instances = new ArrayList<>();
    for (int i = 1; i <= replicationFactor; i++) {
      instances.add(Instance.fromNodeId("localhost_" + i));
    }

    PartitionAssignment partitionAssignment = prepareAssignments(resourceName);
    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP0 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP0.put(HelixState.LEADER, Arrays.asList(instances.get(0)));
    helixStateToInstancesMapForP0.put(HelixState.STANDBY, Arrays.asList(instances.get(1), instances.get(2)));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP0 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP0.put(ExecutionStatus.COMPLETED, instances);
    partitionAssignment
        .addPartition(new Partition(0, helixStateToInstancesMapForP0, executionStatusToInstancesMapForP0));

    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 3);
    Assert.assertTrue(
        InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_1").isRemovable(),
        "Instance should be removable because after removing one instance, there are still 2 active replicas, it will not trigger re-balance.");

    // Remove one instance
    helixStateToInstancesMapForP0 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP0.put(HelixState.LEADER, Arrays.asList(instances.get(0)));
    helixStateToInstancesMapForP0.put(HelixState.STANDBY, Arrays.asList(instances.get(1)));
    executionStatusToInstancesMapForP0 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP0
        .put(ExecutionStatus.COMPLETED, Arrays.asList(instances.get(0), instances.get(1)));
    partitionAssignment
        .addPartition(new Partition(0, helixStateToInstancesMapForP0, executionStatusToInstancesMapForP0));

    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 3);
    NodeRemovableResult result = InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_1");
    Assert.assertFalse(
        result.isRemovable(),
        "Instance should NOT be removable because after removing one instance, there are only 1 active replica, it will not trigger re-balance.");
    Assert.assertEquals(
        result.getBlockingReason(),
        NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE.toString(),
        "Instance should NOT be removable because after removing one instance, there are only 1 active replica, it will not trigger re-balance.");

  }

  @Test
  public void testIsRemovableTriggerRebalanceInstanceView() {
    int partitionCount = 2;
    int replicationFactor = 3;
    PartitionAssignment partitionAssignment = prepareMultiPartitionAssignments(resourceName, partitionCount);

    List<Instance> instances = new ArrayList<>();
    for (int i = 1; i <= replicationFactor; i++) {
      instances.add(Instance.fromNodeId("localhost_" + i));
    }

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP0 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP0.put(HelixState.LEADER, Arrays.asList(instances.get(0)));
    helixStateToInstancesMapForP0.put(HelixState.STANDBY, Arrays.asList(instances.get(1), instances.get(2)));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP0 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP0.put(ExecutionStatus.COMPLETED, instances);
    partitionAssignment
        .addPartition(new Partition(0, helixStateToInstancesMapForP0, executionStatusToInstancesMapForP0));

    EnumMap<HelixState, List<Instance>> helixStateToInstancesMapForP1 = new EnumMap<>(HelixState.class);
    helixStateToInstancesMapForP1.put(HelixState.LEADER, Arrays.asList(instances.get(0)));
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMapForP1 = new EnumMap<>(ExecutionStatus.class);
    executionStatusToInstancesMapForP1.put(ExecutionStatus.COMPLETED, Arrays.asList(instances.get(0)));
    partitionAssignment
        .addPartition(new Partition(1, helixStateToInstancesMapForP1, executionStatusToInstancesMapForP1));

    prepareStoreAndVersion(storeName, version, VersionStatus.ONLINE, true, 3);
    Assert.assertTrue(
        InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_3", Collections.emptyList(), true)
            .isRemovable(),
        "Instance should be removable because after removing one instance, there are 2 active replicas in partition 0 in instance's point of view, it will not trigger re-balance.");
    Assert.assertFalse(
        InstanceStatusDecider.isRemovable(resources, clusterName, "localhost_3").isRemovable(),
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

  private void prepareStoreAndVersion(
      String storeName,
      int version,
      VersionStatus status,
      boolean isCurrentVersion,
      int replicationFactor) {
    Store store = TestUtils.createTestStore(storeName, "t", 0);
    Version version1 = new VersionImpl(storeName, version);
    version1.setReplicationFactor(replicationFactor);
    store.addVersion(version1);

    // we need to add the version to the store first before
    // updating its RF since #addVersion would auto-fill some
    // fields in the version (which would be an override in this
    // case)
    version1.setReplicationFactor(replicationFactor);
    if (isCurrentVersion) {
      store.setCurrentVersion(version);
    }

    doReturn(store).when(readWriteStoreRepository).getStore(storeName);

  }
}
