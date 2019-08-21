package com.linkedin.venice.replication;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.PartitionInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class LeaderStorageNodeReplicatorTest {
  private TopicReplicator leaderStorageNodeReplicator;
  private TopicManager mockTopicManager;
  private VeniceWriterFactory mockVeniceWriterFactory;

  @BeforeMethod
  public void setup() {
    mockTopicManager = mock(TopicManager.class);
    mockVeniceWriterFactory = mock(VeniceWriterFactory.class);
    Properties properties = new Properties();
    properties.put(ConfigKeys.ENABLE_TOPIC_REPLICATOR, true);
    // filler bootstrap servers
    properties.put(ConfigKeys.ENABLE_TOPIC_REPLICATOR_SSL, true);
    properties.put(TopicReplicator.TOPIC_REPLICATOR_SOURCE_SSL_KAFKA_CLUSTER, "test-replicator-source-kafka-cluster");
    Optional<TopicReplicator> replicator =
        TopicReplicator.getTopicReplicator(LeaderStorageNodeReplicator.class.getName(), mockTopicManager,
            new VeniceProperties(properties), mockVeniceWriterFactory);
    if (!replicator.isPresent()) {
      fail("Failed to construct a " + LeaderStorageNodeReplicator.class.getName());
    }
    leaderStorageNodeReplicator = replicator.get();
  }

  @Test
  public void testPrepareAndStartReplication() {
    String srcTopic = "srcTestTopic";
    String destTopic = "destTestTopic";
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(VeniceWriter.class);

    doReturn(true).when(mockStore).isHybrid();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(true).when(mockTopicManager).containsTopic(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopic(destTopic);
    doReturn(partitionInfos).when(mockTopicManager).getPartitions(srcTopic);
    doReturn(partitionInfos).when(mockTopicManager).getPartitions(destTopic);
    doReturn(mockVeniceWriter).when(mockVeniceWriterFactory).getBasicVeniceWriter(any(), any());

    leaderStorageNodeReplicator.prepareAndStartReplication(srcTopic, destTopic, mockStore);

    verify(mockVeniceWriter).broadcastTopicSwitch(any(), anyString(), anyLong(), any());
  }
}
