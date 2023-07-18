package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.HelixUtils;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;


public class TestVeniceHelixAdmin {
  @Test
  public void testDropResources() {
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    List<String> nodes = new ArrayList<>();
    String storeName = "abc";
    String clusterName = "venice-cluster";
    nodes.add("node_1");
    HelixAdminClient adminClient = mock(HelixAdminClient.class);
    HelixVeniceClusterResources veniceClusterResources = mock(HelixVeniceClusterResources.class);
    HelixExternalViewRepository repository = mock(HelixExternalViewRepository.class);
    PartitionAssignment partitionAssignment = mock(PartitionAssignment.class);
    doReturn(adminClient).when(veniceHelixAdmin).getHelixAdminClient();
    doReturn(3).when(partitionAssignment).getExpectedNumberOfPartitions();
    doReturn(veniceClusterResources).when(veniceHelixAdmin).getHelixVeniceClusterResources(anyString());
    doReturn(repository).when(veniceClusterResources).getRoutingDataRepository();
    doReturn(nodes).when(veniceHelixAdmin).getStorageNodes(anyString());
    doReturn(partitionAssignment).when(repository).getPartitionAssignments(anyString());
    doCallRealMethod().when(veniceHelixAdmin).deleteHelixResource(anyString(), anyString());
    String kafkaTopic = Version.composeKafkaTopic(storeName, 1);
    List<String> partitions = new ArrayList<>(3);
    for (int partitionId = 0; partitionId < 3; partitionId++) {
      partitions.add(HelixUtils.getPartitionName(kafkaTopic, partitionId));
    }

    veniceHelixAdmin.deleteHelixResource(clusterName, kafkaTopic);
    verify(adminClient, times(1)).enablePartition(true, clusterName, "node_1", kafkaTopic, partitions);

  }
}
