package com.linkedin.venice.ingestion.control;

import static com.linkedin.venice.meta.BufferReplayPolicy.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.PartitionInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RealTimeTopicSwitcherTest {
  private RealTimeTopicSwitcher leaderStorageNodeReplicator;
  private TopicManager mockTopicManager;
  private VeniceWriterFactory mockVeniceWriterFactory;
  private String aggregateRealTimeSourceKafkaUrl = "aggregate-real-time-source-kafka-url";

  @BeforeMethod
  public void setUp() {
    mockTopicManager = mock(TopicManager.class);
    mockVeniceWriterFactory = mock(VeniceWriterFactory.class);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "dummy");
    // filler bootstrap servers
    leaderStorageNodeReplicator =
        new RealTimeTopicSwitcher(mockTopicManager, mockVeniceWriterFactory, new VeniceProperties(properties));
  }

  @Test
  public void testPrepareAndStartReplication() {
    String srcTopic = "testTopic_rt";
    String destTopic = "testTopic_v1";
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(VeniceWriter.class);

    doReturn(true).when(mockStore).isHybrid();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    Version version = new VersionImpl(Version.parseStoreFromKafkaTopicName(destTopic), 1, "test-id");
    doReturn(Optional.of(version)).when(mockStore).getVersion(Version.parseVersionFromKafkaTopicName(destTopic));
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockHybridConfig).getBufferReplayPolicy();
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(destTopic);
    doReturn(partitionInfos).when(mockTopicManager).partitionsFor(srcTopic);
    doReturn(partitionInfos).when(mockTopicManager).partitionsFor(destTopic);
    doReturn(mockVeniceWriter).when(mockVeniceWriterFactory)
        .createBasicVeniceWriter(anyString(), any(Time.class), any(VenicePartitioner.class), anyInt());

    leaderStorageNodeReplicator.switchToRealTimeTopic(
        srcTopic,
        destTopic,
        mockStore,
        aggregateRealTimeSourceKafkaUrl,
        Collections.emptyList());

    verify(mockVeniceWriter).broadcastTopicSwitch(any(), anyString(), anyLong(), any());
  }

  @Test
  public void testPrepareAndStartReplicationWithNativeReplication() {
    String srcTopic = "testTopic_rt";
    String destTopic = "testTopic_v1";
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(VeniceWriter.class);

    doReturn(true).when(mockStore).isHybrid();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    Version version = new VersionImpl(Version.parseStoreFromKafkaTopicName(destTopic), 1, "test-id");
    version.setNativeReplicationEnabled(true);
    doReturn(Optional.of(version)).when(mockStore).getVersion(Version.parseVersionFromKafkaTopicName(destTopic));
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockHybridConfig).getBufferReplayPolicy();
    doReturn(DataReplicationPolicy.AGGREGATE).when(mockHybridConfig).getDataReplicationPolicy();
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(destTopic);
    doReturn(partitionInfos).when(mockTopicManager).partitionsFor(srcTopic);
    doReturn(partitionInfos).when(mockTopicManager).partitionsFor(destTopic);
    doReturn(mockVeniceWriter).when(mockVeniceWriterFactory)
        .createBasicVeniceWriter(anyString(), any(Time.class), any(VenicePartitioner.class), anyInt());

    leaderStorageNodeReplicator.switchToRealTimeTopic(
        srcTopic,
        destTopic,
        mockStore,
        aggregateRealTimeSourceKafkaUrl,
        Collections.emptyList());

    List<CharSequence> expectedSourceClusters = new ArrayList<>();
    expectedSourceClusters.add(aggregateRealTimeSourceKafkaUrl);
    verify(mockVeniceWriter).broadcastTopicSwitch(eq(expectedSourceClusters), eq(srcTopic), anyLong(), any());
  }
}
