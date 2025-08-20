package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit test for {@link ZkAdminTopicMetadataAccessor}
 */
public class TestZkAdminTopicMetadataAccessor {
  private ZkClient zkClient;
  private HelixAdapterSerializer adapterSerializer;
  private ZkAdminTopicMetadataAccessor zkAdminTopicMetadataAccessor;
  ApacheKafkaOffsetPosition position = ApacheKafkaOffsetPosition.of(12345L);

  @BeforeMethod
  public void setUp() {
    zkClient = mock(ZkClient.class);
    adapterSerializer = mock(HelixAdapterSerializer.class);
    zkAdminTopicMetadataAccessor =
        new ZkAdminTopicMetadataAccessor(zkClient, adapterSerializer, mock(VeniceControllerMultiClusterConfig.class));
  }

  @Test
  public void testUpdateMetadataWhenMetadataIsEmpty() {
    String clusterName = "test-cluster";

    // metadata that we are trying to update
    Map<String, Object> metadataDelta = new HashMap<>();
    metadataDelta.put("offset", 100L);
    metadataDelta.put(AdminTopicMetadataAccessor.POSITION_KEY, position);
    AdminMetadata adminMetadata = new AdminMetadata(metadataDelta);

    String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);
    String v2MetadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicV2MetadataNodePath(clusterName);

    try (MockedStatic<DataTree> dataTreeMockedStatic = Mockito.mockStatic(DataTree.class)) {
      dataTreeMockedStatic.when(() -> DataTree.copyStat(any(), any())).thenAnswer(invocation -> null);
      Stat readStat = new Stat();

      // Mock the metadata on prod - null
      when(zkClient.readData(metadataPath, readStat)).thenReturn(null);
      when(zkClient.readData(v2MetadataPath, readStat)).thenReturn(null);

      // Update the metadata
      zkAdminTopicMetadataAccessor.updateMetadata(clusterName, adminMetadata);

      // Verify that the metadata path got read 1 time
      verify(zkClient, times(1)).readData(eq(metadataPath), eq(readStat));
      verify(zkClient, times(1)).readData(eq(v2MetadataPath), eq(readStat));

      // Verify that the metadata path got read 1 time with the metadataDelta map and 1 time legacy metadata map and 1
      // time with new admin metadata
      verify(zkClient, times(1)).writeDataGetStat(eq(metadataPath), eq(adminMetadata.toLegacyMap()), eq(0));
      verify(zkClient, times(1)).writeDataGetStat(eq(v2MetadataPath), eq(adminMetadata), eq(0));

      assertEquals(adminMetadata.toLegacyMap().size(), 1);
      assertEquals(adminMetadata.toLegacyMap().get("offset").longValue(), 100L);
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
    AdminMetadata currentV2Metadata = AdminMetadata.fromLegacyMap(currentMetadata);

    // metadata that we are trying to update
    Map<String, Long> metadataDelta = new HashMap<>();
    metadataDelta.put("offset", newOffset);
    AdminMetadata v2MetadataDelta = AdminMetadata.fromLegacyMap(metadataDelta);
    v2MetadataDelta.setUpstreamPubSubPosition(position);

    String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);
    String v2MetadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicV2MetadataNodePath(clusterName);

    try (MockedStatic<DataTree> dataTreeMockedStatic = Mockito.mockStatic(DataTree.class)) {
      dataTreeMockedStatic.when(() -> DataTree.copyStat(any(), any())).thenAnswer(invocation -> null);
      Stat readStat = new Stat();

      when(zkClient.readData(metadataPath, readStat)).thenReturn(currentMetadata); // Case 2: the metadata is not null
      when(zkClient.readData(v2MetadataPath, readStat)).thenReturn(currentV2Metadata);

      // Update the metadata on prod with new offset
      zkAdminTopicMetadataAccessor.updateMetadata(clusterName, v2MetadataDelta);

      // The updated metadata should be the original metadata with the offset/position updated
      Map<String, Long> updatedMetadata = AdminTopicMetadataAccessor
          .generateMetadataMap(Optional.of(newOffset), Optional.of(-1L), Optional.of(1L), Optional.of(18L));
      AdminMetadata updatedV2Metadata = AdminMetadata.fromLegacyMap(updatedMetadata);
      updatedV2Metadata.setUpstreamPubSubPosition(position);

      // Verify that the metadata path got read 1 times
      verify(zkClient, times(1)).readData(eq(metadataPath), eq(readStat));
      verify(zkClient, times(1)).readData(eq(v2MetadataPath), eq(readStat));

      // Verify that the metadata path got written with the correct updated metadata
      verify(zkClient, times(1)).writeDataGetStat(eq(metadataPath), eq(updatedMetadata), eq(0));
      verify(zkClient, times(1)).writeDataGetStat(eq(v2MetadataPath), argThat(metadata -> {
        if (!(metadata instanceof AdminMetadata)) {
          return false;
        }
        AdminMetadata adminMetadata = (AdminMetadata) metadata;
        Map<String, Object> actualMap = adminMetadata.toMap();
        Map<String, Object> expectedMap = updatedV2Metadata.toMap();
        return actualMap.equals(expectedMap);
      }), eq(0));
    }
  }

  @Test
  public void testGetMetadata() {
    String clusterName = "test-cluster";
    Map<String, Long> currentMetadata = AdminTopicMetadataAccessor
        .generateMetadataMap(Optional.of(1L), Optional.of(-1L), Optional.of(1L), Optional.of(18L));
    AdminMetadata currentV2Metadata = AdminMetadata.fromLegacyMap(currentMetadata);
    currentV2Metadata.setPubSubPosition(position);

    String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);
    String v2MetadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicV2MetadataNodePath(clusterName);

    when(zkClient.readData(metadataPath, null)).thenReturn(null).thenReturn(currentMetadata);
    when(zkClient.readData(v2MetadataPath, null)).thenReturn(null).thenReturn(currentV2Metadata);

    // Case 1: when there is no metadata
    AdminMetadata metadata = zkAdminTopicMetadataAccessor.getMetadata(clusterName);
    AdminMetadata v2Metadata = zkAdminTopicMetadataAccessor.getV2AdminMetadata(clusterName);

    assertTrue(metadata.toMap().values().stream().allMatch(Objects::isNull), "All values should be null");
    assertTrue(v2Metadata.toMap().values().stream().allMatch(Objects::isNull), "All values should be null");

    // Case 2: the metadata is not null
    metadata = zkAdminTopicMetadataAccessor.getMetadata(clusterName);
    v2Metadata = zkAdminTopicMetadataAccessor.getV2AdminMetadata(clusterName);

    assertEquals(metadata.toLegacyMap(), currentMetadata);
    assertEquals(v2Metadata, currentV2Metadata);
  }
}
