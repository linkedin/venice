package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
  public void testUpdateMetadataWhenMetadataIsEmpty() {
    String clusterName = "test-cluster";

    // metadata that we are trying to update
    Map<String, Long> metadataDelta = new HashMap<>();
    metadataDelta.put("offset", 100L);

    String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);
    try (MockedStatic<DataTree> dataTreeMockedStatic = Mockito.mockStatic(DataTree.class)) {
      dataTreeMockedStatic.when(() -> DataTree.copyStat(any(), any())).thenAnswer(invocation -> null);
      Stat readStat = new Stat();

      // Mock the metadata on prod - null
      when(zkClient.readData(metadataPath, readStat)).thenReturn(null);

      // Update the metadata
      zkAdminTopicMetadataAccessor.updateMetadata(clusterName, metadataDelta);

      // Verify that the metadata path got read 1 time
      verify(zkClient, times(1)).readData(metadataPath, readStat);

      // Verify that the metadata path got read 1 time with the metadataDelta map
      // When the metadata is empty, the metadataDelta should be written as is
      verify(zkClient, times(1)).writeDataGetStat(metadataPath, metadataDelta, 0);
    }
  }

  @Test
  public void testUpdateMetadataWithFullMetadata() {
    String clusterName = "test-cluster";
    Long originalOffset = 1L;
    Long newOffset = 100L;

    // Original metadata
    Map<String, Long> currentMetadata = AdminTopicMetadataAccessor
        .generateMetadataMap(Optional.of(originalOffset), Optional.of(-1L), Optional.of(1L), Optional.of(18L));

    // metadata that we are trying to update
    Map<String, Long> metadataDelta = new HashMap<>();
    metadataDelta.put("offset", newOffset);

    String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);
    try (MockedStatic<DataTree> dataTreeMockedStatic = Mockito.mockStatic(DataTree.class)) {
      dataTreeMockedStatic.when(() -> DataTree.copyStat(any(), any())).thenAnswer(invocation -> null);
      Stat readStat = new Stat();

      when(zkClient.readData(metadataPath, readStat)).thenReturn(currentMetadata); // Case 2: the metadata is not null

      // Update the metadata on prod with new offset
      zkAdminTopicMetadataAccessor.updateMetadata(clusterName, metadataDelta);

      // The updated metadata should be the original metadata with the offset updated
      Map<String, Long> updatedMetadata = AdminTopicMetadataAccessor
          .generateMetadataMap(Optional.of(newOffset), Optional.of(-1L), Optional.of(1L), Optional.of(18L));

      // Verify that the metadata path got read 1 times
      verify(zkClient, times(1)).readData(metadataPath, readStat);

      // Verify that the metadata path got written with the correct updated metadata
      verify(zkClient, times(1)).writeDataGetStat(metadataPath, updatedMetadata, 0);
    }
  }

  @Test
  public void testGetMetadata() {
    String clusterName = "test-cluster";
    Map<String, Long> currentMetadata = AdminTopicMetadataAccessor
        .generateMetadataMap(Optional.of(1L), Optional.of(-1L), Optional.of(1L), Optional.of(18L));
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
