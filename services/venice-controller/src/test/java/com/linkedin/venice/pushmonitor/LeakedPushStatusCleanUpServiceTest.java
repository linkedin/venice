package com.linkedin.venice.pushmonitor;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;


public class LeakedPushStatusCleanUpServiceTest {
  private static final long TEST_TIMEOUT = TimeUnit.SECONDS.toMillis(10);

  @Test
  public void testLeakedZKNodeShouldBeDeleted() throws Exception {
    String clusterName = "test-cluster";
    long sleepIntervalInMs = 10;
    long allowedLingerTimeInMs = 0;
    OfflinePushAccessor accessor = mock(OfflinePushAccessor.class);
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    AggPushStatusCleanUpStats aggPushStatusCleanUpStats = mock(AggPushStatusCleanUpStats.class);

    /**
     * Define good and leaked push statues
     */
    String storeName = "test_store";
    int leakedVersion1 = 1;
    int leakedVersion2 = 2;
    int currentVersion = 3;
    String leakedStoreVersion1 = Version.composeKafkaTopic(storeName, leakedVersion1);
    String leakedStoreVersion2 = Version.composeKafkaTopic(storeName, leakedVersion2);
    String goodStoreVersion = Version.composeKafkaTopic(storeName, currentVersion);
    List<String> loadedStoreVersionList = Arrays.asList(leakedStoreVersion1, leakedStoreVersion2, goodStoreVersion);
    doReturn(loadedStoreVersionList).when(accessor).loadOfflinePushStatusPaths();
    // Return empty creation time for the second leaked push status, so that it will be kept for debugging
    doReturn(Optional.empty()).when(accessor).getOfflinePushStatusCreationTime(leakedStoreVersion2);

    /**
     * Define the behavior of store config; the leaked version will not be in the version list of the store
     */
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(any());
    doReturn(currentVersion).when(mockStore).getCurrentVersion();
    doReturn(false).when(mockStore).containsVersion(leakedVersion1);
    doReturn(false).when(mockStore).containsVersion(leakedVersion2);

    /**
     * The actual test; the clean up service will try to delete the leaked push status
     */
    try (LeakedPushStatusCleanUpService cleanUpService = new LeakedPushStatusCleanUpService(
        clusterName,
        accessor,
        metadataRepository,
        mock(StoreCleaner.class),
        aggPushStatusCleanUpStats,
        sleepIntervalInMs,
        allowedLingerTimeInMs,
        null)) {
      cleanUpService.start();
      verify(accessor, timeout(TEST_TIMEOUT).atLeastOnce())
          .deleteOfflinePushStatusAndItsPartitionStatuses(leakedStoreVersion1);
      /**
       * At most {@link LeakedPushStatusCleanUpService#MAX_LEAKED_VERSION_TO_KEEP} leaked push statues before the current
       * version will be kept for debugging.
       */
      verify(accessor, never()).deleteOfflinePushStatusAndItsPartitionStatuses(leakedStoreVersion2);
    }

    /**
     * Return an old creation time for the second leaked push status, so that it will be deleted due to be lingering too long.
     */
    doReturn(Optional.of(0l)).when(accessor).getOfflinePushStatusCreationTime(leakedStoreVersion2);
    try (LeakedPushStatusCleanUpService cleanUpService = new LeakedPushStatusCleanUpService(
        clusterName,
        accessor,
        metadataRepository,
        mock(StoreCleaner.class),
        aggPushStatusCleanUpStats,
        sleepIntervalInMs,
        allowedLingerTimeInMs,
        null)) {
      cleanUpService.start();
      // Both leaked resources should be deleted.
      verify(accessor, timeout(TEST_TIMEOUT).atLeastOnce())
          .deleteOfflinePushStatusAndItsPartitionStatuses(leakedStoreVersion1);
      verify(accessor, timeout(TEST_TIMEOUT).atLeastOnce())
          .deleteOfflinePushStatusAndItsPartitionStatuses(leakedStoreVersion2);
    }
  }

  @Test
  public void testStaleReplicaStatusCleanup() throws Exception {
    String clusterName = "test-cluster";
    long sleepIntervalInMs = 10;
    long allowedLingerTimeInMs = 0;
    String storeName = "test_store";
    int version = 1;
    String kafkaTopic = Version.composeKafkaTopic(storeName, version);
    int numberOfPartitions = 2;
    int replicationFactor = 3;

    // Create mock dependencies
    OfflinePushAccessor accessor = mock(OfflinePushAccessor.class);
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    RoutingDataRepository routingDataRepository = mock(RoutingDataRepository.class);
    AggPushStatusCleanUpStats aggPushStatusCleanUpStats = mock(AggPushStatusCleanUpStats.class);

    // Setup store
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(storeName);
    doReturn(version).when(mockStore).getCurrentVersion();
    doReturn(true).when(mockStore).containsVersion(version);

    // Create instances - we'll have 3 current instances and 2 stale instance IDs
    Instance instance1 = new Instance("instance1", "host1", 9000);
    Instance instance2 = new Instance("instance2", "host2", 9000);
    Instance instance3 = new Instance("instance3", "host3", 9000);
    // instance4 and instance5 are stale - we only need their string IDs, not Instance objects

    // Create offline push status with partition statuses containing replicas
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartitions, replicationFactor, OfflinePushStrategy.WAIT_ALL_REPLICAS);

    // Partition 0: has replicas from instance1, instance2, instance3 (current) + instance4 (stale)
    PartitionStatus partition0Status = new PartitionStatus(0);
    partition0Status.updateReplicaStatus("instance1", ExecutionStatus.COMPLETED);
    partition0Status.updateReplicaStatus("instance2", ExecutionStatus.COMPLETED);
    partition0Status.updateReplicaStatus("instance3", ExecutionStatus.COMPLETED);
    partition0Status.updateReplicaStatus("instance4", ExecutionStatus.COMPLETED); // Stale - will be removed

    // Partition 1: has replicas from instance1, instance2, instance3 (current) + instance5 (stale)
    PartitionStatus partition1Status = new PartitionStatus(1);
    partition1Status.updateReplicaStatus("instance1", ExecutionStatus.COMPLETED);
    partition1Status.updateReplicaStatus("instance2", ExecutionStatus.COMPLETED);
    partition1Status.updateReplicaStatus("instance3", ExecutionStatus.PROGRESS);
    partition1Status.updateReplicaStatus("instance5", ExecutionStatus.ERROR); // Stale - will be removed

    offlinePushStatus.setPartitionStatus(partition0Status);
    offlinePushStatus.setPartitionStatus(partition1Status);

    // Setup accessor to return the push status with all replicas (including stale ones)
    doReturn(Arrays.asList(kafkaTopic)).when(accessor).loadOfflinePushStatusPaths();
    doReturn(offlinePushStatus).when(accessor).getOfflinePushStatusAndItsPartitionStatuses(kafkaTopic);

    // Setup routing data repository to return current partition assignments (WITHOUT stale instances)
    doReturn(true).when(routingDataRepository).containsKafkaTopic(kafkaTopic);

    // Create partition assignments with only current instances (instance1, instance2, instance3)
    PartitionAssignment partitionAssignment =
        createPartitionAssignment(kafkaTopic, numberOfPartitions, Arrays.asList(instance1, instance2, instance3));

    doReturn(partitionAssignment).when(routingDataRepository).getPartitionAssignments(kafkaTopic);

    // Start the cleanup service
    try (LeakedPushStatusCleanUpService cleanUpService = new LeakedPushStatusCleanUpService(
        clusterName,
        accessor,
        metadataRepository,
        mock(StoreCleaner.class),
        aggPushStatusCleanUpStats,
        sleepIntervalInMs,
        allowedLingerTimeInMs,
        routingDataRepository)) {
      cleanUpService.start();

      // Verify that removeStaleReplicasFromPartitionStatus was called with the correct stale instance IDs
      @SuppressWarnings("unchecked")
      ArgumentCaptor<Set<String>> staleIdsCaptor = ArgumentCaptor.forClass(Set.class);
      ArgumentCaptor<Integer> partitionIdCaptor = ArgumentCaptor.forClass(Integer.class);
      verify(accessor, timeout(TEST_TIMEOUT).atLeast(2)).removeStaleReplicasFromPartitionStatus(
          eq(kafkaTopic),
          partitionIdCaptor.capture(),
          staleIdsCaptor.capture());

      // Get all captured calls
      List<Integer> capturedPartitionIds = partitionIdCaptor.getAllValues();
      List<Set<String>> capturedStaleIds = staleIdsCaptor.getAllValues();
      assertTrue(capturedPartitionIds.size() >= 2, "Should have at least 2 calls");

      // Find the calls for partition 0 and partition 1
      Set<String> partition0StaleIds = null;
      Set<String> partition1StaleIds = null;
      for (int i = 0; i < capturedPartitionIds.size(); i++) {
        if (capturedPartitionIds.get(i) == 0) {
          partition0StaleIds = capturedStaleIds.get(i);
        } else if (capturedPartitionIds.get(i) == 1) {
          partition1StaleIds = capturedStaleIds.get(i);
        }
      }

      // Verify partition 0 had instance4 as stale
      assertTrue(partition0StaleIds != null, "Partition 0 should have been cleaned up");
      assertEquals(partition0StaleIds.size(), 1, "Partition 0 should have 1 stale replica");
      assertTrue(partition0StaleIds.contains("instance4"), "instance4 should be identified as stale");

      // Verify partition 1 had instance5 as stale
      assertTrue(partition1StaleIds != null, "Partition 1 should have been cleaned up");
      assertEquals(partition1StaleIds.size(), 1, "Partition 1 should have 1 stale replica");
      assertTrue(partition1StaleIds.contains("instance5"), "instance5 should be identified as stale");
    }
  }

  /**
   * Helper method to create a PartitionAssignment with given instances
   */
  private PartitionAssignment createPartitionAssignment(String topic, int numPartitions, List<Instance> instances) {
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      EnumMap<HelixState, List<Instance>> helixStateMap = new EnumMap<>(HelixState.class);
      EnumMap<ExecutionStatus, List<Instance>> executionStatusMap = new EnumMap<>(ExecutionStatus.class);

      // Distribute instances across partitions - for simplicity, all instances go to all partitions
      helixStateMap.put(HelixState.LEADER, Arrays.asList(instances.get(0)));
      List<Instance> standbyInstances = new ArrayList<>();
      for (int j = 1; j < instances.size(); j++) {
        standbyInstances.add(instances.get(j));
      }
      helixStateMap.put(HelixState.STANDBY, standbyInstances);

      // Set execution status - mark all as COMPLETED for this test
      executionStatusMap.put(ExecutionStatus.COMPLETED, new ArrayList<>(instances));

      Partition partition = new Partition(i, helixStateMap, executionStatusMap);
      partitionAssignment.addPartition(partition);
    }
    return partitionAssignment;
  }
}
