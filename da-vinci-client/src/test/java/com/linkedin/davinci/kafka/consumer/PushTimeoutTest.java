package com.linkedin.davinci.kafka.consumer;

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
import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
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
    LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask = new LeaderFollowerStoreIngestionTask(mock(
        VeniceWriterFactory.class), mockKafkaClientFactory, mockKafkaConsumerProperties, mock(StorageEngineRepository.class),
        mockStorageMetadataService, notifiers, mock(EventThrottler.class), mock(EventThrottler.class), mock(EventThrottler.class),
        mock(EventThrottler.class), mock(ReadOnlySchemaRepository.class), mockReadOnlyStoreRepository, mock(TopicManagerRepository.class),
        mock(TopicManagerRepository.class), mock(AggStoreIngestionStats.class), mock(AggVersionedDIVStats.class),
        mock(AggVersionedStorageIngestionStats.class), mock(StoreBufferService.class), mock(BooleanSupplier.class),
        Optional.empty(), false, IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC, mockVeniceStoreConfig, mock(DiskUsage.class),
        mock(RocksDBMemoryStats.class), true, mock(AggKafkaConsumerService.class), mockVeniceServerConfig,
        false, null, 0, mock(ExecutorService.class), 0,
        mock(InternalAvroSpecificSerializer.class), false, mock(VenicePartitioner.class), 1, false, 1,
        mock(GenericRecordChunkingAdapter.class));
    leaderFollowerStoreIngestionTask.subscribePartition(versionTopic, 0);
    leaderFollowerStoreIngestionTask.run();

    // Verify that push timeout happens
    Assert.assertTrue(exceptionCaptorNotifier.getLatestException() instanceof VeniceTimeoutException);
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
