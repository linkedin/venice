package com.linkedin.venice.controller.stats;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import io.tehuti.metrics.Sensor;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionHealthStatsTest {
  private String topic = "test_v1";
  private static PushMonitor mockPushMonitor = Mockito.mock(PushMonitor.class);

  @Test
  public void testUnderReplicatedPartitionStats() {
    int replicationFactor = 3;
    int partitionCount = 3;

    ReadOnlyStoreRepository mockStoreRepo = Mockito.mock(ReadOnlyStoreRepository.class);
    Store mockStore = Mockito.mock(Store.class);
    Mockito.doReturn(VersionStatus.ONLINE).when(mockStore).getVersionStatus(Mockito.anyInt());
    Mockito.doReturn(replicationFactor).when(mockStore).getReplicationFactor();
    Mockito.doReturn(mockStore).when(mockStoreRepo).getStore(Mockito.anyString());

    MockPartitionHealthStats stats = new MockPartitionHealthStats(mockStoreRepo);
    PartitionAssignment assignment = new PartitionAssignment(topic, partitionCount);

    // Prepare both under replicated partition and full replicated partition.
    for (int i = 0; i < partitionCount - 1; i++) {
      assignment.addPartition(preparePartition(assignment, i, replicationFactor - 1));
    }
    assignment.addPartition(preparePartition(assignment, partitionCount - 1, replicationFactor));

    stats.onExternalViewChange(assignment);
    // Verify we have recorded the correct under replicated partition count
    Assert.assertEquals(
        stats.underReplicatedPartitionNumber,
        partitionCount - 1,
        "We give stats two under replicated partitions, but it did not recorded it correctly.");
    // On-going push.
    Mockito.doReturn(VersionStatus.STARTED).when(mockStore).getVersionStatus(Mockito.anyInt());
    // Reset stats.
    stats = new MockPartitionHealthStats(mockStoreRepo);
    stats.onExternalViewChange(assignment);
    // Verify we have recorded the correct under replicated partition count
    Assert.assertEquals(
        stats.underReplicatedPartitionNumber,
        0,
        "We should not count the under replicated partition in on-going push.");
  }

  private Partition preparePartition(PartitionAssignment partitionAssignment, int partitionId, int replicaCount) {
    Partition partition = Mockito.mock(Partition.class);
    Mockito.doReturn(partitionId).when(partition).getId();
    List<Instance> mockInstancesList = new ArrayList<>();
    for (int j = 0; j < replicaCount; j++) {
      mockInstancesList.add(Mockito.mock(Instance.class));
    }
    Mockito.doReturn(mockInstancesList)
        .when(mockPushMonitor)
        .getReadyToServeInstances(partitionAssignment, partitionId);
    return partition;
  }

  /**
   * Because {@link Sensor} is a final class so it can not be mocked by {@link Mockito}, so we create a sub-class of
   * {@link AggPartitionHealthStats} to get the internal state from it to verify out tests.
   */
  private static class MockPartitionHealthStats extends AggPartitionHealthStats {
    int underReplicatedPartitionNumber = 0;

    public MockPartitionHealthStats(ReadOnlyStoreRepository storeRepository) {
      super("testUnderReplicatedPartitionStats", storeRepository, mockPushMonitor);
    }

    @Override
    protected void reportUnderReplicatedPartition(String version, int underReplicatedPartitions) {
      this.underReplicatedPartitionNumber += underReplicatedPartitions;
    }
  }
}
