package com.linkedin.venice.controller.stats;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import io.tehuti.metrics.Sensor;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionHealthStatsTest {
  @Test
  public void testUnderReplicatedPartitionStats() {
    int replicationFactor = 3;
    int partitionCount = 3;

    MockPartitionHealthStats stats = new MockPartitionHealthStats(replicationFactor);
    PartitionAssignment assignment = new PartitionAssignment("test", partitionCount);
    // Prepare both under replicated partition and full replicated partition.
    for (int i = 0; i < partitionCount - 1; i++) {
      assignment.addPartition(preparePartition(i, replicationFactor - 1));
    }
    assignment.addPartition(preparePartition(partitionCount - 1, replicationFactor));

    stats.onRoutingDataChanged(assignment);
    // Verify we have recorded the correct under replicated partition count
    Assert.assertEquals(stats.underReplicatedPartitionNumber, partitionCount - 1,
        "We give stats two under replicated partitions, but it did not recorded it correctly.");
  }

  private Partition preparePartition(int partitionId, int replicaCount) {
    Partition partition = Mockito.mock(Partition.class);
    Mockito.doReturn(partitionId).when(partition).getId();
    List<Instance> mockInstancesList = new ArrayList<>();
    for (int j = 0; j < replicaCount; j++) {
      mockInstancesList.add(Mockito.mock(Instance.class));
    }
    Mockito.doReturn(mockInstancesList).when(partition).getReadyToServeInstances();
    return partition;
  }

  /**
   * Because {@link Sensor} is a final class so it can not be mocked by {@link Mockito}, so we create a sub-class of
   * {@link PartitionHealthStats} to get the internal state from it to verify out tests.
   */
  private class MockPartitionHealthStats extends PartitionHealthStats {
    int underReplicatedPartitionNumber = 0;

    public MockPartitionHealthStats(int requriedReplicaFactor) {
      super(requriedReplicaFactor);
    }

    @Override
    protected void reportUnderReplicatedPartition(String version, int underReplicatedPartitions) {
      this.underReplicatedPartitionNumber += underReplicatedPartitions;
    }
  }
}
