package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZkAdminTopicMetadataAccessor {
  private ZkClient zkClient;
  private HelixAdapterSerializer adapterSerializer;
  private ZkAdminTopicMetadataAccessor zkAdminTopicMetadataAccessor;

  @BeforeMethod
  public void setUp() {
    zkClient = mock(ZkClient.class);
    adapterSerializer = mock(HelixAdapterSerializer.class);
    zkAdminTopicMetadataAccessor = new ZkAdminTopicMetadataAccessor(zkClient, adapterSerializer);
  }

  @Test
  public void testUpdateMetadata() {
    String clusterName = "test-cluster";

    // Original metadata
    Map<String, Long> currentMetadata = AdminTopicMetadataAccessor.generateMetadataMap(1, -1, 1, 18);

    // New metadata
    Map<String, Long> newMetadata = new HashMap<>();
    newMetadata.put("offset", 100L);

    // Updated metadata with new metadata
    Map<String, Long> updatedMetadata = AdminTopicMetadataAccessor.generateMetadataMap(100, -1, 1, 18);

    String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);
    try (MockedStatic<DataTree> dataTreeMockedStatic = Mockito.mockStatic(DataTree.class, RETURNS_SMART_NULLS)) {
      dataTreeMockedStatic.when(() -> DataTree.copyStat(any(), any())).thenAnswer(invocation -> null);
      Stat readStat = new Stat();

      when(zkClient.readData(metadataPath, readStat)).thenReturn(null) // Case 1: when there is no metadata
          .thenReturn(currentMetadata); // Case 2: the metadata is not null

      // Case 1: when there is no metadata - null
      zkAdminTopicMetadataAccessor.updateMetadata(clusterName, newMetadata);
      verify(zkClient, times(1)).writeDataGetStat(metadataPath, newMetadata, 0);

      // Case 2: the metadata is not null
      zkAdminTopicMetadataAccessor.updateMetadata(clusterName, newMetadata);
      verify(zkClient, times(1)).writeDataGetStat(metadataPath, updatedMetadata, 0);

      // Verify that the metadata path got read 2 times
      verify(zkClient, times(2)).readData(metadataPath, readStat);
    }
  }

  @Test
  public void testGetMetadata() {
    String clusterName = "test-cluster";
    Map<String, Long> currentMetadata = AdminTopicMetadataAccessor.generateMetadataMap(1, -1, 1, 18);
    String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);

    when(zkClient.readData(metadataPath, null)).thenReturn(null).thenReturn(currentMetadata);

    // Case 1: when there is no metadata
    Map<String, Long> metadata = zkAdminTopicMetadataAccessor.getMetadata(clusterName);
    assertEquals(metadata, new HashMap<>());

    // Case 2: the metadata is not null
    metadata = zkAdminTopicMetadataAccessor.getMetadata(clusterName);
    assertEquals(metadata, currentMetadata);
  }
}
