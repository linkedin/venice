package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ExceptionCaptorNotifier;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.function.BooleanSupplier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PushTimeoutTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testPushTimeoutForLeaderFollowerStores() {
    String storeName = Utils.getUniqueString("store");
    int versionNumber = 1;

    ExceptionCaptorNotifier exceptionCaptorNotifier = new ExceptionCaptorNotifier();
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(exceptionCaptorNotifier);
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    AggHostLevelIngestionStats mockAggStoreIngestionStats = mock(AggHostLevelIngestionStats.class);
    HostLevelIngestionStats mockStoreIngestionStats = mock(HostLevelIngestionStats.class);
    doReturn(mockStoreIngestionStats).when(mockAggStoreIngestionStats).getStoreStats(anyString());

    StoreIngestionTaskFactory.Builder builder = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setLeaderFollowerNotifiersQueue(notifiers)
        .setServerConfig(mockVeniceServerConfig)
        .setHostLevelIngestionStats(mockAggStoreIngestionStats)
        .setPubSubTopicRepository(pubSubTopicRepository);

    StorageService storageService = mock(StorageService.class);
    doReturn(new ReferenceCounted<>(mock(StorageEngine.class), se -> {})).when(storageService)
        .getRefCountedStorageEngine(anyString());

    Store mockStore = builder.getMetadataRepo().getStoreOrThrow(storeName);
    Version version = mockStore.getVersion(versionNumber);

    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(KAFKA_BOOTSTRAP_SERVERS));

    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    String versionTopic = version.kafkaTopicName();
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();

    LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask = new LeaderFollowerStoreIngestionTask(
        storageService,
        builder,
        mockStore,
        version,
        mockKafkaConsumerProperties,
        mock(BooleanSupplier.class),
        mockVeniceStoreVersionConfig,
        0,
        false,
        Optional.empty(),
        null,
        null);

    leaderFollowerStoreIngestionTask
        .subscribePartition(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(versionTopic), 0));
    leaderFollowerStoreIngestionTask.run();

    // Verify that push timeout happens
    Exception latestException = exceptionCaptorNotifier.getLatestException();
    Assert.assertNotNull(latestException, "Latest exception should not be null.");
    Assert.assertTrue(
        latestException instanceof VeniceTimeoutException,
        "Should have caught an instance of " + VeniceTimeoutException.class.getSimpleName() + "but instead got: "
            + latestException.getClass().getSimpleName() + ".");
  }

  @Test
  public void testReportIfCatchUpBaseTopicOffsetRouteWillNotMakePushTimeout() {
    String storeName = Utils.getUniqueString("store");
    int versionNumber = 1;

    ExceptionCaptorNotifier exceptionCaptorNotifier = new ExceptionCaptorNotifier();
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(exceptionCaptorNotifier);

    StorageMetadataService mockStorageMetadataService = mock(StorageMetadataService.class);
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(VeniceProperties.empty()).when(mockVeniceServerConfig).getClusterProperties();
    doReturn(VeniceProperties.empty()).when(mockVeniceServerConfig).getKafkaConsumerConfigsForRemoteConsumption();
    doReturn(VeniceProperties.empty()).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    AggHostLevelIngestionStats mockAggStoreIngestionStats = mock(AggHostLevelIngestionStats.class);
    HostLevelIngestionStats mockStoreIngestionStats = mock(HostLevelIngestionStats.class);
    doReturn(mockStoreIngestionStats).when(mockAggStoreIngestionStats).getStoreStats(anyString());

    StoreIngestionTaskFactory.Builder builder = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setLeaderFollowerNotifiersQueue(notifiers)
        .setStorageMetadataService(mockStorageMetadataService)
        .setServerConfig(mockVeniceServerConfig)
        .setHostLevelIngestionStats(mockAggStoreIngestionStats)
        .setPubSubTopicRepository(pubSubTopicRepository);

    StorageService storageService = mock(StorageService.class);
    doReturn(new ReferenceCounted<>(mock(StorageEngine.class), se -> {})).when(storageService)
        .getRefCountedStorageEngine(anyString());
    Store mockStore = builder.getMetadataRepo().getStoreOrThrow(storeName);
    Version version = mockStore.getVersion(versionNumber);

    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(KAFKA_BOOTSTRAP_SERVERS));

    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    String versionTopic = version.kafkaTopicName();
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(Collections.emptyMap()).when(mockOffsetRecord).getProducerPartitionStateMap();
    /**
     * After restart, report EOP already received, in order to trigger a call into
     * {@link StoreIngestionTask#reportIfCatchUpVersionTopicOffset(PartitionConsumptionState)}
     */
    doReturn(true).when(mockOffsetRecord).isEndOfPushReceived();
    doReturn(Utils.getRealTimeTopicName(mockStore)).when(mockOffsetRecord).getLeaderTopic();
    /**
     * Return 0 as the max offset for VT and 1 as the overall consume progress, so reportIfCatchUpVersionTopicOffset()
     * will determine that base topic is caught up.
     */
    doReturn(1L).when(mockOffsetRecord).getLocalVersionTopicOffset();
    doReturn(mockOffsetRecord).when(mockStorageMetadataService).getLastOffset(eq(versionTopic), eq(0));

    LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask = new LeaderFollowerStoreIngestionTask(
        storageService,
        builder,
        mockStore,
        version,
        mockKafkaConsumerProperties,
        mock(BooleanSupplier.class),
        mockVeniceStoreVersionConfig,
        0,
        false,
        Optional.empty(),
        null,
        null);

    leaderFollowerStoreIngestionTask
        .subscribePartition(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(versionTopic), 0));
    /**
     * Since the mock consumer would show 0 subscription, the ingestion task will close after a few iteration.
     */
    leaderFollowerStoreIngestionTask.run();

    Assert.assertNull(exceptionCaptorNotifier.getLatestException());
  }
}
