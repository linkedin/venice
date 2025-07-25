package com.linkedin.venice.ingestion.control;

import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_EOP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RealTimeTopicSwitcherTest {
  private static final int KAFKA_RF_FOR_RT_TOPICS = 6;
  private static final int KAFKA_MIN_ISR_FOR_RT_TOPICS = 4;
  private RealTimeTopicSwitcher leaderStorageNodeReplicator;
  private TopicManager mockTopicManager;
  private VeniceWriterFactory mockVeniceWriterFactory;
  private String aggregateRealTimeSourceKafkaUrl = "aggregate-real-time-source-kafka-url";

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod
  public void setUp() {
    mockTopicManager = mock(TopicManager.class);
    mockVeniceWriterFactory = mock(VeniceWriterFactory.class);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, "dummy");
    properties.put(ConfigKeys.KAFKA_REPLICATION_FACTOR, "3");
    properties.put(ConfigKeys.KAFKA_REPLICATION_FACTOR_RT_TOPICS, Integer.toString(KAFKA_RF_FOR_RT_TOPICS));
    properties.put(ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS, Integer.toString(KAFKA_MIN_ISR_FOR_RT_TOPICS));
    // filler bootstrap servers
    leaderStorageNodeReplicator = new RealTimeTopicSwitcher(
        mockTopicManager,
        mockVeniceWriterFactory,
        new VeniceProperties(properties),
        pubSubTopicRepository);
  }

  @Test
  public void testPrepareAndStartReplication() {
    PubSubTopic srcTopic = pubSubTopicRepository.getTopic("testTopic_rt");
    PubSubTopic destTopic = pubSubTopicRepository.getTopic("testTopic_v1");
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(VeniceWriter.class);

    doReturn(true).when(mockStore).isHybrid();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    Version version = new VersionImpl(destTopic.getStoreName(), 1, "test-id");
    doReturn(version).when(mockStore).getVersionOrThrow(Version.parseVersionFromKafkaTopicName(destTopic.getName()));
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockHybridConfig).getBufferReplayPolicy();
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(destTopic);
    doReturn(mockVeniceWriter).when(mockVeniceWriterFactory).createVeniceWriter(any(VeniceWriterOptions.class));

    leaderStorageNodeReplicator.switchToRealTimeTopic(
        srcTopic.getName(),
        destTopic.getName(),
        mockStore,
        aggregateRealTimeSourceKafkaUrl,
        Collections.emptyList());

    verify(mockVeniceWriter).broadcastTopicSwitch(any(), anyString(), anyLong(), any());
  }

  @Test
  public void testPrepareAndStartReplicationWithNativeReplication() {
    PubSubTopic srcTopic = pubSubTopicRepository.getTopic("testTopic_rt");
    PubSubTopic destTopic = pubSubTopicRepository.getTopic("testTopic_v1");
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(VeniceWriter.class);

    doReturn(true).when(mockStore).isHybrid();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    Version version = new VersionImpl(destTopic.getStoreName(), 1, "test-id");
    version.setNativeReplicationEnabled(true);
    doReturn(version).when(mockStore).getVersionOrThrow(Version.parseVersionFromKafkaTopicName(destTopic.getName()));
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockHybridConfig).getBufferReplayPolicy();
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(destTopic);
    doReturn(mockVeniceWriter).when(mockVeniceWriterFactory).createVeniceWriter(any(VeniceWriterOptions.class));

    leaderStorageNodeReplicator.switchToRealTimeTopic(
        srcTopic.getName(),
        destTopic.getName(),
        mockStore,
        aggregateRealTimeSourceKafkaUrl,
        Collections.emptyList());

    List<CharSequence> expectedSourceClusters = new ArrayList<>();
    expectedSourceClusters.add("dummy");
    verify(mockVeniceWriter).broadcastTopicSwitch(eq(expectedSourceClusters), eq(srcTopic.getName()), anyLong(), any());
  }

  @Test
  public void testSendVersionSwap() {
    String storeName = "TestStore";

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(storeName);
    Version version1 = mock(Version.class, RETURNS_DEEP_STUBS);
    Version version2 = mock(Version.class, RETURNS_DEEP_STUBS);
    Version version3 = mock(Version.class, RETURNS_DEEP_STUBS);
    when(version1.isHybrid()).thenReturn(true);
    when(version2.isHybrid()).thenReturn(true);
    when(version3.isHybrid()).thenReturn(true);
    String realTimeTopicName = Utils.composeRealTimeTopic(storeName, 1);
    when(version1.getNumber()).thenReturn(1);
    when(version2.getNumber()).thenReturn(2);
    when(version3.getNumber()).thenReturn(3);
    when(version1.getStoreName()).thenReturn(storeName);
    when(version2.getStoreName()).thenReturn(storeName);
    when(version3.getStoreName()).thenReturn(storeName);
    when(version1.kafkaTopicName()).thenReturn(Version.composeKafkaTopic(storeName, 1));
    when(version2.kafkaTopicName()).thenReturn(Version.composeKafkaTopic(storeName, 2));
    when(version3.kafkaTopicName()).thenReturn(Version.composeKafkaTopic(storeName, 3));
    when(version1.getHybridStoreConfig().getRealTimeTopicName()).thenReturn(realTimeTopicName);
    when(version2.getHybridStoreConfig().getRealTimeTopicName()).thenReturn(realTimeTopicName);
    when(version3.getHybridStoreConfig().getRealTimeTopicName()).thenReturn(realTimeTopicName);
    PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(realTimeTopicName);

    when(mockStore.getVersionOrThrow(1)).thenReturn(version1);
    when(mockStore.getVersionOrThrow(2)).thenReturn(version2);
    when(mockStore.getVersionOrThrow(3)).thenReturn(version3);
    when(mockStore.getVersionOrThrow(4)).thenReturn(null);

    TopicManager mockTopicManager = mock(TopicManager.class);
    when(mockTopicManager.containsTopic(realTimeTopic)).thenReturn(true);

    VeniceWriter mockVeniceWriter = mock(VeniceWriter.class);

    VeniceWriterFactory mockWriterFactory = mock(VeniceWriterFactory.class);
    ArgumentCaptor<VeniceWriterOptions> vwOptionsArgumentCaptor = ArgumentCaptor.forClass(VeniceWriterOptions.class);
    when(mockWriterFactory.createVeniceWriter(any(VeniceWriterOptions.class))).thenReturn(mockVeniceWriter);

    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    when(mockVeniceProperties.getString(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS))
        .thenReturn(aggregateRealTimeSourceKafkaUrl);

    RealTimeTopicSwitcher realTimeTopicSwitcher =
        new RealTimeTopicSwitcher(mockTopicManager, mockWriterFactory, mockVeniceProperties, pubSubTopicRepository);

    // VSM should not be broadcasted when swapping between NON_EXISTING_VERSION and an existing version
    realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, Store.NON_EXISTING_VERSION, 1);
    verify(mockVeniceWriter, never()).broadcastVersionSwap(anyString(), anyString(), anyMap());
    realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, 1, Store.NON_EXISTING_VERSION);
    verify(mockVeniceWriter, never()).broadcastVersionSwap(anyString(), anyString(), anyMap());

    // Verify VSM should be broadcasted
    realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, 1, 2);
    verify(mockVeniceWriter).broadcastVersionSwap(anyString(), anyString(), anyMap());

    // Version 4 doesn't exist. In this case DON'T transmit a version swap message, and throw an exception to boot
    Assert.assertThrows(() -> realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, 3, 4));

    // Version 2 and 3 exist, so we should transmit a version swap message
    realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, 2, 3);
    verify(mockWriterFactory, times(2)).createVeniceWriter(vwOptionsArgumentCaptor.capture());
    VeniceWriterOptions capturedVwo = vwOptionsArgumentCaptor.getValue();
    Assert.assertEquals(capturedVwo.getTopicName(), realTimeTopic.getName());

    ArgumentCaptor<String> oldVersionCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> newVersionCaptor = ArgumentCaptor.forClass(String.class);

    // Verify version swap message arguments look right
    verify(mockVeniceWriter, times(2))
        .broadcastVersionSwap(oldVersionCaptor.capture(), newVersionCaptor.capture(), anyMap());
    Assert.assertEquals(oldVersionCaptor.getValue(), version2.kafkaTopicName());
    Assert.assertEquals(newVersionCaptor.getValue(), version3.kafkaTopicName());

    // VSM should be broadcasted to both versions when there doesn't exist an RT topic
    clearInvocations(mockVeniceWriter);
    when(mockTopicManager.containsTopic(realTimeTopic)).thenReturn(false);
    realTimeTopicSwitcher.transmitVersionSwapMessage(mockStore, 2, 3);
    verify(mockVeniceWriter, times(2)).broadcastVersionSwap(anyString(), anyString(), anyMap());
  }

  @Test
  public void testEnsurePreconditions() {
    PubSubTopic srcTopic = pubSubTopicRepository.getTopic("testTopic_rt");
    PubSubTopic destTopic = pubSubTopicRepository.getTopic("testTopic_v1");
    Store mockStore = mock(Store.class);
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    doReturn(3600L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockHybridConfig).getBufferReplayPolicy();
    doReturn(mockHybridConfig).when(mockStore).getHybridStoreConfig();
    doReturn(true).when(mockStore).isHybrid();

    Version version = new VersionImpl(destTopic.getStoreName(), 1, "test-id");
    // Mock version-level hybrid store config with a different rewind time
    HybridStoreConfig mockVersionLevelHybridConfig = mock(HybridStoreConfig.class);
    doReturn(7200L).when(mockVersionLevelHybridConfig).getRewindTimeInSeconds();
    doReturn(REWIND_FROM_EOP).when(mockVersionLevelHybridConfig).getBufferReplayPolicy();
    version.setHybridStoreConfig(mockVersionLevelHybridConfig);
    doReturn(version).when(mockStore).getVersion(Version.parseVersionFromKafkaTopicName(destTopic.getName()));

    doReturn(false).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(srcTopic);
    doReturn(true).when(mockTopicManager).containsTopicAndAllPartitionsAreOnline(destTopic);

    leaderStorageNodeReplicator.ensurePreconditions(srcTopic, destTopic, mockStore);

    long retentionTime = StoreUtils.getExpectedRetentionTimeInMs(mockStore, mockStore.getHybridStoreConfig());

    verify(mockTopicManager).createTopic(
        eq(srcTopic),
        anyInt(),
        eq(KAFKA_RF_FOR_RT_TOPICS),
        eq(retentionTime),
        eq(false),
        eq(Optional.of(KAFKA_MIN_ISR_FOR_RT_TOPICS)),
        eq(false));
  }
}
