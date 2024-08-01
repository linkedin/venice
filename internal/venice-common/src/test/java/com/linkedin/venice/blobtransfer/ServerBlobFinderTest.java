package com.linkedin.venice.blobtransfer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerBlobFinderTest {
  private HelixCustomizedViewOfflinePushRepository mockCustomizedViewRepository;

  @BeforeMethod
  public void setUp() {
    mockCustomizedViewRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
  }

  @Test
  public void testFindBlob() {
    // Arrange
    String storeName = "test-store";
    int version = 2;
    int partitionId = 0;
    String topicName = Version.composeKafkaTopic(storeName, version);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topicName, 1);
    EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
    EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
    ExecutionStatus executionStatus1 = ExecutionStatus.NOT_CREATED;
    ExecutionStatus executionStatus2 = ExecutionStatus.COMPLETED;
    Instance instance1 = new Instance("host1", "host1", 1234);
    Instance instance2 = new Instance("host2", "host2", 1234);
    executionStatusToInstancesMap.put(executionStatus1, Collections.singletonList(instance1));
    executionStatusToInstancesMap.put(executionStatus2, Collections.singletonList(instance2));
    Partition partition = new Partition(partitionId, helixStateToInstancesMap, executionStatusToInstancesMap);
    partitionAssignment.addPartition(partition);
    doReturn(partitionAssignment).when(mockCustomizedViewRepository).getPartitionAssignments(topicName);

    // Act
    ServerBlobFinder serverBlobFinder =
        new ServerBlobFinder(CompletableFuture.completedFuture(mockCustomizedViewRepository));
    BlobPeersDiscoveryResponse resultBlobResponse = serverBlobFinder.discoverBlobPeers(storeName, version, partitionId);

    // Assert
    Assert.assertNotNull(resultBlobResponse);
    Assert.assertEquals(resultBlobResponse.getDiscoveryResult().size(), 1);
    Assert.assertEquals(resultBlobResponse.getDiscoveryResult().get(0), instance2.getHost());
  }
}
