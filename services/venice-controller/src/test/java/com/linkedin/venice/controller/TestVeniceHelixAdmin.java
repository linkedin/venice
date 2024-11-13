package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.stats.DisabledPartitionStats;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.HelixUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.InOrder;
import org.testng.annotations.Test;


public class TestVeniceHelixAdmin {
  @Test
  public void testDropResources() {
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    List<String> nodes = new ArrayList<>();
    String storeName = "abc";
    String instance = "node_1";
    String kafkaTopic = Version.composeKafkaTopic(storeName, 1);
    String clusterName = "venice-cluster";
    nodes.add(instance);
    Map<String, List<String>> listMap = new HashMap<>();
    List<String> partitions = new ArrayList<>(3);
    for (int partitionId = 0; partitionId < 3; partitionId++) {
      partitions.add(HelixUtils.getPartitionName(kafkaTopic, partitionId));
    }
    listMap.put(kafkaTopic, partitions);
    HelixAdminClient adminClient = mock(HelixAdminClient.class);
    HelixVeniceClusterResources veniceClusterResources = mock(HelixVeniceClusterResources.class);
    HelixExternalViewRepository repository = mock(HelixExternalViewRepository.class);
    PartitionAssignment partitionAssignment = mock(PartitionAssignment.class);
    doReturn(adminClient).when(veniceHelixAdmin).getHelixAdminClient();
    doReturn(listMap).when(adminClient).getDisabledPartitionsMap(clusterName, instance);
    doReturn(3).when(partitionAssignment).getExpectedNumberOfPartitions();
    doReturn(veniceClusterResources).when(veniceHelixAdmin).getHelixVeniceClusterResources(anyString());
    doReturn(repository).when(veniceClusterResources).getRoutingDataRepository();
    doReturn(nodes).when(veniceHelixAdmin).getStorageNodes(anyString());
    doReturn(partitionAssignment).when(repository).getPartitionAssignments(anyString());
    doReturn(mock(DisabledPartitionStats.class)).when(veniceHelixAdmin).getDisabledPartitionStats(anyString());
    doCallRealMethod().when(veniceHelixAdmin).deleteHelixResource(anyString(), anyString());

    veniceHelixAdmin.deleteHelixResource(clusterName, kafkaTopic);
    verify(veniceHelixAdmin, times(1)).enableDisabledPartition(clusterName, kafkaTopic, false);

  }

  /**
   * This test verify that in function {@link VeniceHelixAdmin#setUpMetaStoreAndMayProduceSnapshot},
   * meta store RT topic creation has to happen before any writings to meta store's rt topic.
   * As of today, topic creation and checks to make sure that RT exists are handled in function
   * {@link VeniceHelixAdmin#getRealTimeTopic}. On the other hand, as {@link VeniceHelixAdmin#storeMetadataUpdate}
   * writes to the same RT topic, it should happen after the above function. The following test enforces
   * such order at the statement level.
   *
   * Notice that if function semantics change over time, as long as the above invariant can be obtained,
   * it is okay to relax on the ordering enforcement or delete the unit test if necessary.
   */
  @Test
  public void enforceRealTimeTopicCreationBeforeWriting() {
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doReturn("test_rt").when(veniceHelixAdmin).getRealTimeTopic(anyString(), anyString(), any());
    doCallRealMethod().when(veniceHelixAdmin).setUpMetaStoreAndMayProduceSnapshot(anyString(), anyString());

    InOrder inorder = inOrder(veniceHelixAdmin);

    HelixVeniceClusterResources veniceClusterResources = mock(HelixVeniceClusterResources.class);
    ReadWriteStoreRepository repo = mock(ReadWriteStoreRepository.class);
    Store store = mock(Store.class);

    doReturn(veniceClusterResources).when(veniceHelixAdmin).getHelixVeniceClusterResources(anyString());
    doReturn(repo).when(veniceClusterResources).getStoreMetadataRepository();
    doReturn(store).when(repo).getStore(anyString());
    doReturn(Boolean.FALSE).when(store).isDaVinciPushStatusStoreEnabled();

    veniceHelixAdmin.setUpMetaStoreAndMayProduceSnapshot(anyString(), anyString());

    // Enforce that getRealTimeTopic happens before storeMetadataUpdate. See the above comments for the reasons.
    inorder.verify(veniceHelixAdmin).getRealTimeTopic(anyString(), anyString(), any());
    inorder.verify(veniceHelixAdmin).storeMetadataUpdate(anyString(), anyString(), any());
  }

  @Test
  public void testGetOverallPushStatus() {
    ExecutionStatus veniceStatus = ExecutionStatus.COMPLETED;
    ExecutionStatus daVinciStatus = ExecutionStatus.COMPLETED;
    ExecutionStatus overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);

    assertEquals(overallStatus, ExecutionStatus.COMPLETED);

    veniceStatus = ExecutionStatus.ERROR;
    daVinciStatus = ExecutionStatus.COMPLETED;
    overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);
    assertEquals(overallStatus, ExecutionStatus.ERROR);

    veniceStatus = ExecutionStatus.ERROR;
    daVinciStatus = ExecutionStatus.ERROR;
    overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);
    assertEquals(overallStatus, ExecutionStatus.ERROR);

    veniceStatus = ExecutionStatus.COMPLETED;
    daVinciStatus = ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
    overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);
    assertEquals(overallStatus, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);

    veniceStatus = ExecutionStatus.ERROR;
    daVinciStatus = ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
    overallStatus = VeniceHelixAdmin.getOverallPushStatus(veniceStatus, daVinciStatus);
    assertEquals(overallStatus, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
  }
}
