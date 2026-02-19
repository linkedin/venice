package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import java.util.HashMap;
import java.util.Map;
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
  InMemoryPubSubPosition position = InMemoryPubSubPosition.of(12345L);

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
    metadataDelta.put(AdminTopicMetadataAccessor.POSITION_KEY, position);
    AdminMetadata adminMetadata = new AdminMetadata(metadataDelta);

    String v2MetadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicV2MetadataNodePath(clusterName);

    try (MockedStatic<DataTree> dataTreeMockedStatic = Mockito.mockStatic(DataTree.class)) {
      dataTreeMockedStatic.when(() -> DataTree.copyStat(any(), any())).thenAnswer(invocation -> null);
      Stat readStat = new Stat();

      // Mock the metadata on prod - null
      when(zkClient.readData(v2MetadataPath, readStat)).thenReturn(null);

      // Update the metadata
      zkAdminTopicMetadataAccessor.updateMetadata(clusterName, adminMetadata);

      // Verify that the metadata path got read 1 time
      verify(zkClient, times(1)).readData(eq(v2MetadataPath), eq(readStat));

      // Verify that the metadata path got written with the admin metadata
      verify(zkClient, times(1)).writeDataGetStat(eq(v2MetadataPath), eq(adminMetadata), eq(0));

      // Verify position is set correctly
      assertEquals(((InMemoryPubSubPosition) adminMetadata.getPosition()).getInternalOffset(), 12345L);
    }
  }

  @Test
  public void testUpdateMetadataWithFullMetadata() {
    String clusterName = "test-cluster";
    InMemoryPubSubPosition originalPosition = InMemoryPubSubPosition.of(1L);
    InMemoryPubSubPosition newPosition = InMemoryPubSubPosition.of(100L);

    // Original metadata
    AdminMetadata currentMetadata = new AdminMetadata();
    currentMetadata.setPubSubPosition(originalPosition);
    currentMetadata.setExecutionId(1L);
    currentMetadata.setAdminOperationProtocolVersion(18L);

    // metadata that we are trying to update
    AdminMetadata metadataDelta = new AdminMetadata();
    metadataDelta.setPubSubPosition(newPosition);
    metadataDelta.setUpstreamPubSubPosition(position);

    String v2MetadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicV2MetadataNodePath(clusterName);

    try (MockedStatic<DataTree> dataTreeMockedStatic = Mockito.mockStatic(DataTree.class)) {
      dataTreeMockedStatic.when(() -> DataTree.copyStat(any(), any())).thenAnswer(invocation -> null);
      Stat readStat = new Stat();
      // Case 2: the metadata is not null
      when(zkClient.readData(v2MetadataPath, readStat)).thenReturn(currentMetadata);

      // Update the metadata on prod with new position
      zkAdminTopicMetadataAccessor.updateMetadata(clusterName, metadataDelta);

      // The updated metadata should be the original metadata with the position updated
      AdminMetadata updatedMetadata = new AdminMetadata();
      updatedMetadata.setPubSubPosition(newPosition);
      updatedMetadata.setUpstreamPubSubPosition(position);
      updatedMetadata.setExecutionId(1L);
      updatedMetadata.setAdminOperationProtocolVersion(18L);

      // Verify that the metadata path got read 1 time
      verify(zkClient, times(1)).readData(eq(v2MetadataPath), eq(readStat));

      // Verify that the metadata path got written with the correct updated metadata
      verify(zkClient, times(1)).writeDataGetStat(eq(v2MetadataPath), eq(updatedMetadata), eq(0));
    }
  }

  @Test
  public void testGetMetadata() {
    String clusterName = "test-cluster";
    AdminMetadata currentV2Metadata = new AdminMetadata();
    currentV2Metadata.setExecutionId(1L);
    currentV2Metadata.setAdminOperationProtocolVersion(18L);
    currentV2Metadata.setPubSubPosition(position);
    currentV2Metadata.setUpstreamPubSubPosition(PubSubSymbolicPosition.EARLIEST);

    String v2MetadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicV2MetadataNodePath(clusterName);

    when(zkClient.readData(v2MetadataPath, null)).thenReturn(null).thenReturn(currentV2Metadata);

    // Case 1: when there is no metadata, should return empty AdminMetadata with default positions
    AdminMetadata metadata = zkAdminTopicMetadataAccessor.getMetadata(clusterName);

    // executionId and adminOperationProtocolVersion should be null, positions should be EARLIEST (default)
    assertEquals(metadata.getExecutionId(), AdminTopicMetadataAccessor.UNDEFINED_VALUE);
    assertEquals(metadata.getAdminOperationProtocolVersion(), AdminTopicMetadataAccessor.UNDEFINED_VALUE);
    assertEquals(metadata.getPosition(), PubSubSymbolicPosition.EARLIEST);
    assertEquals(metadata.getUpstreamPosition(), PubSubSymbolicPosition.EARLIEST);

    // Case 2: the metadata is not null
    metadata = zkAdminTopicMetadataAccessor.getMetadata(clusterName);

    assertEquals(metadata, currentV2Metadata);
  }
}
