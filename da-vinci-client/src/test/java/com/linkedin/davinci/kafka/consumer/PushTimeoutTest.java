package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.notifier.LogNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.CommonClientConfigs;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class PushTimeoutTest {
  @Test
  public void testPushTimeoutForLeaderFollowerStores() {
    String storeName = TestUtils.getUniqueString("store");
    String versionTopic = Version.composeKafkaTopic(storeName, 1);

    VeniceStoreConfig mockVeniceStoreConfig = mock(VeniceStoreConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreConfig).getStoreName();

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(false).when(mockVeniceServerConfig).isHybridQuotaEnabled();
    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    doReturn(true).when(mockVeniceProperties).isEmpty();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    KafkaClientFactory mockKafkaClientFactory = mock(KafkaClientFactory.class);
    KafkaConsumerWrapper mockKafkaConsumerWrapper = mock(KafkaConsumerWrapper.class);
    doReturn(mockKafkaConsumerWrapper).when(mockKafkaClientFactory).getConsumer(any());

    ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockReadOnlyStoreRepository).getStoreOrThrow(eq(storeName));
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    // Set timeout threshold to 0 so that push timeout error will happen immediately after a partition subscription.
    doReturn(0).when(mockStore).getBootstrapToOnlineTimeoutInHours();

    StorageMetadataService mockStorageMetadataService = mock(StorageMetadataService.class);
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(Collections.emptyMap()).when(mockOffsetRecord).getProducerPartitionStateMap();
    doReturn(mockOffsetRecord).when(mockStorageMetadataService).getLastOffset(eq(versionTopic), eq(0));

    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));

    ExceptionCaptorNotifier exceptionCaptorNotifier = new ExceptionCaptorNotifier();
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(exceptionCaptorNotifier);

    int partitionCount = 1;
    VenicePartitioner partitioner = new DefaultVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(1);

    Version version = new VersionImpl(storeName, 1, "1", partitionCount);

    version.setPartitionerConfig(partitionerConfig);
    doReturn(partitionerConfig).when(mockStore).getPartitionerConfig();

    version.setLeaderFollowerModelEnabled(true);
    doReturn(true).when(mockStore).isLeaderFollowerModelEnabled();

    version.setIncrementalPushEnabled(false);
    doReturn(false).when(mockStore).isIncrementalPushEnabled();

    version.setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);
    doReturn(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC).when(mockStore).getIncrementalPushPolicy();

    version.setHybridStoreConfig(null);
    doReturn(null).when(mockStore).getHybridStoreConfig();
    doReturn(false).when(mockStore).isHybrid();

    version.setBufferReplayEnabledForHybrid(true);

    version.setNativeReplicationEnabled(false);
    doReturn(false).when(mockStore).isNativeReplicationEnabled();

    version.setPushStreamSourceAddress("");
    doReturn("").when(mockStore).getPushStreamSourceAddress();

    doReturn(false).when(mockStore).isWriteComputationEnabled();

    doReturn(partitionCount).when(mockStore).getPartitionCount();

    doReturn(-1).when(mockStore).getCurrentVersion();

    doReturn(Optional.of(version)).when(mockStore).getVersion(anyInt());

    LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask = new LeaderFollowerStoreIngestionTask(mockStore, version, mock(
        VeniceWriterFactory.class), mockKafkaClientFactory, mockKafkaConsumerProperties, mock(StorageEngineRepository.class),
        mockStorageMetadataService, notifiers, mock(EventThrottler.class), mock(EventThrottler.class), mock(EventThrottler.class),
        mock(EventThrottler.class), mock(ReadOnlySchemaRepository.class), mockReadOnlyStoreRepository, mock(TopicManagerRepository.class),
        mock(TopicManagerRepository.class), mock(AggStoreIngestionStats.class), mock(AggVersionedDIVStats.class),
        mock(AggVersionedStorageIngestionStats.class), mock(StoreBufferService.class), mock(BooleanSupplier.class),
        mockVeniceStoreConfig, mock(DiskUsage.class), mock(RocksDBMemoryStats.class), mock(AggKafkaConsumerService.class),
        mockVeniceServerConfig, 0, mock(ExecutorService.class), 0, mock(InternalAvroSpecificSerializer.class), false,
        mock(StorageEngineBackedCompressorFactory.class), Optional.empty());
    leaderFollowerStoreIngestionTask.subscribePartition(versionTopic, 0);
    leaderFollowerStoreIngestionTask.run();

    // Verify that push timeout happens
    Assert.assertTrue(exceptionCaptorNotifier.getLatestException() instanceof VeniceTimeoutException);
  }

  @Test
  public void testReportIfCatchUpBaseTopicOffsetRouteWillNotMakePushTimeout() {
    String storeName = TestUtils.getUniqueString("store");
    String versionTopic = Version.composeKafkaTopic(storeName, 1);

    VeniceStoreConfig mockVeniceStoreConfig = mock(VeniceStoreConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreConfig).getStoreName();

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(false).when(mockVeniceServerConfig).isHybridQuotaEnabled();
    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    doReturn(true).when(mockVeniceProperties).isEmpty();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    KafkaClientFactory mockKafkaClientFactory = mock(KafkaClientFactory.class);
    KafkaConsumerWrapper mockKafkaConsumerWrapper = mock(KafkaConsumerWrapper.class);
    doReturn(mockKafkaConsumerWrapper).when(mockKafkaClientFactory).getConsumer(any());

    ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockReadOnlyStoreRepository).getStoreOrThrow(eq(storeName));
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    /**
     * Set timeout threshold to 0; however, after the ingestion start, COMPLETED will be
     * reported inside {@link StoreIngestionTask#reportIfCatchUpBaseTopicOffset(PartitionConsumptionState)},
     * so that timeout will not be checked for a completed push.
     */
    doReturn(0).when(mockStore).getBootstrapToOnlineTimeoutInHours();

    StorageMetadataService mockStorageMetadataService = mock(StorageMetadataService.class);
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(Collections.emptyMap()).when(mockOffsetRecord).getProducerPartitionStateMap();
    /**
     * After restart, report EOP already received, in order to trigger a call into
     * {@link StoreIngestionTask#reportIfCatchUpBaseTopicOffset(PartitionConsumptionState)}
     */
    doReturn(true).when(mockOffsetRecord).isEndOfPushReceived();
    doReturn(Version.composeRealTimeTopic(storeName)).when(mockOffsetRecord).getLeaderTopic();
    /**
     * Explicitly return a low leader offset and a high real-time topic max offset, so that RT lag is
     * still high and thus {@link StoreIngestionTask#checkConsumptionStateWhenStart(OffsetRecord, PartitionConsumptionState)}
     * will not report COMPLETED, in order to invoke reportIfCatchUpBaseTopicOffset
     */
    doReturn(1L).when(mockOffsetRecord).getLeaderOffset(anyString());
    /**]
     * Return 0 as the max offset for VT and 1 as the overall consume progress, so reportIfCatchUpBaseTopicOffset()
     * will determine that base topic is caught up.
     */
    doReturn(1L).when(mockOffsetRecord).getLocalVersionTopicOffset();
    doReturn(mockOffsetRecord).when(mockStorageMetadataService).getLastOffset(eq(versionTopic), eq(0));

    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));

    TopicManagerRepository mockTopicManagerRepository = mock(TopicManagerRepository.class);
    TopicManager mockTopicManager = mock(TopicManager.class);
    // Return 0 as the max offset, so the CATCH_UP_BASE_TOPIC_OFFSET_LAG is guaranteed to be reported since lag is 0
    doReturn(0L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(eq(versionTopic), anyInt(), anyInt());
    doReturn(1000L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(eq(Version.composeRealTimeTopic(storeName)), anyInt(), anyInt());
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getTopicManager();

    // Make the test store a hybrid store and build a high RT offset lag, so that to force the logic of checking base version topic
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100, 100,
        -1, DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);

    ExceptionCaptorNotifier exceptionCaptorNotifier = new ExceptionCaptorNotifier();
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(exceptionCaptorNotifier);

    int partitionCount = 1;
    VenicePartitioner partitioner = new DefaultVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(1);

    Version version = new VersionImpl(storeName, 1, "1", partitionCount);

    version.setPartitionerConfig(partitionerConfig);
    doReturn(partitionerConfig).when(mockStore).getPartitionerConfig();

    version.setLeaderFollowerModelEnabled(true);
    doReturn(true).when(mockStore).isLeaderFollowerModelEnabled();

    version.setIncrementalPushEnabled(false);
    doReturn(false).when(mockStore).isIncrementalPushEnabled();

    version.setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);
    doReturn(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC).when(mockStore).getIncrementalPushPolicy();

    version.setHybridStoreConfig(hybridStoreConfig);
    doReturn(hybridStoreConfig).when(mockStore).getHybridStoreConfig();
    doReturn(true).when(mockStore).isHybrid();

    version.setBufferReplayEnabledForHybrid(true);

    version.setNativeReplicationEnabled(false);
    doReturn(false).when(mockStore).isNativeReplicationEnabled();

    version.setPushStreamSourceAddress("");
    doReturn("").when(mockStore).getPushStreamSourceAddress();

    doReturn(false).when(mockStore).isWriteComputationEnabled();

    doReturn(partitionCount).when(mockStore).getPartitionCount();

    doReturn(-1).when(mockStore).getCurrentVersion();

    doReturn(Optional.of(version)).when(mockStore).getVersion(anyInt());

    LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask = new LeaderFollowerStoreIngestionTask(mockStore, version, mock(
        VeniceWriterFactory.class), mockKafkaClientFactory, mockKafkaConsumerProperties, mock(StorageEngineRepository.class),
        mockStorageMetadataService, notifiers, mock(EventThrottler.class), mock(EventThrottler.class), mock(EventThrottler.class),
        mock(EventThrottler.class), mock(ReadOnlySchemaRepository.class), mockReadOnlyStoreRepository, mockTopicManagerRepository,
        mock(TopicManagerRepository.class), mock(AggStoreIngestionStats.class), mock(AggVersionedDIVStats.class),
        mock(AggVersionedStorageIngestionStats.class), mock(StoreBufferService.class), () -> true,
        mockVeniceStoreConfig, mock(DiskUsage.class), mock(RocksDBMemoryStats.class), mock(AggKafkaConsumerService.class),
        mockVeniceServerConfig, 0, mock(ExecutorService.class), 0, mock(InternalAvroSpecificSerializer.class), false,
        mock(StorageEngineBackedCompressorFactory.class), Optional.empty());

    leaderFollowerStoreIngestionTask.subscribePartition(versionTopic, 0);
    /**
     * Since the mock consumer would show 0 subscription, the ingestion task will close after a few iteration.
     */
    leaderFollowerStoreIngestionTask.run();

    Assert.assertNull(exceptionCaptorNotifier.getLatestException());
  }

  private static class ExceptionCaptorNotifier extends LogNotifier {
    private Exception latestException;

    @Override
    public void error(String kafkaTopic, int partitionId, String message, Exception ex) {
      this.latestException = ex;
      super.error(kafkaTopic, partitionId, message, ex);
    }

    public Exception getLatestException() {
      return this.latestException;
    }
  }
}