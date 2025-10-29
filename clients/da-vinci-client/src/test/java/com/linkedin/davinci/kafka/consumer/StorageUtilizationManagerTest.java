package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.avro.Schema;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class StorageUtilizationManagerTest {
  private final static long storeQuotaInBytes = 100L;
  private final static long newStoreQuotaInBytes = 200L;
  private final static int storePartitionCount = 10;
  private final static String storeName = "TestTopic";
  private final static PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private final static PubSubTopic VERSION_TOPIC = TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 1));
  private final static PubSubTopic REAL_TIME_TOPIC =
      TOPIC_REPOSITORY.getTopic(Utils.composeRealTimeTopic(storeName) + Utils.SEPARATE_TOPIC_SUFFIX);
  private final static int STORE_VERSION = Version.parseVersionFromKafkaTopicName(VERSION_TOPIC.getName());

  private ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private ConcurrentMap<Integer, PartitionConsumptionState> hybridPartitionConsumptionStateMap;
  private StorageEngine storageEngine;
  private IngestionNotificationDispatcher ingestionNotificationDispatcher;
  private Store store;
  private Version version;
  private StorageUtilizationManager quotaEnforcer;
  private StorageUtilizationManager hybridQuotaEnforcer;
  private PubSubContext pubSubContext;

  @BeforeClass
  public void setUp() {
    storageEngine = mock(StorageEngine.class);
    store = mock(Store.class);
    version = mock(Version.class);
    pubSubContext = DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
  }

  @BeforeMethod
  public void buildNewQuotaEnforcer() {
    ingestionNotificationDispatcher = mock(IngestionNotificationDispatcher.class);
    partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();
    hybridPartitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    for (int i = 1; i <= storePartitionCount; i++) {
      PartitionConsumptionState pcs = new PartitionConsumptionState(
          new PubSubTopicPartitionImpl(VERSION_TOPIC, i),
          mock(OffsetRecord.class),
          pubSubContext,
          true,
          Schema.create(Schema.Type.STRING));
      partitionConsumptionStateMap.put(i, pcs);
    }

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    when(mockOffsetRecord.getLeaderTopic()).thenReturn(REAL_TIME_TOPIC.getName());
    for (int i = 1; i <= storePartitionCount; i++) {
      PartitionConsumptionState pcs = new PartitionConsumptionState(
          new PubSubTopicPartitionImpl(VERSION_TOPIC, i),
          mockOffsetRecord,
          pubSubContext,
          true,
          Schema.create(Schema.Type.STRING));
      pcs.setLeaderFollowerState(LEADER);
      hybridPartitionConsumptionStateMap.put(i, pcs);
    }

    when(store.getName()).thenReturn(storeName);
    when(store.getStorageQuotaInByte()).thenReturn(storeQuotaInBytes);
    when(store.getPartitionCount()).thenReturn(storePartitionCount);
    when(store.getVersion(STORE_VERSION)).thenReturn(version);
    when(store.isHybridStoreDiskQuotaEnabled()).thenReturn(true);
    when(version.getStatus()).thenReturn(VersionStatus.STARTED);

    quotaEnforcer = new StorageUtilizationManager(
        storageEngine,
        store,
        VERSION_TOPIC.getName(),
        storePartitionCount,
        partitionConsumptionStateMap,
        true,
        true,
        false,
        ingestionNotificationDispatcher,
        (t, p) -> {},
        (t, p) -> {});

    hybridQuotaEnforcer = new StorageUtilizationManager(
        storageEngine,
        store,
        VERSION_TOPIC.getName(),
        storePartitionCount,
        hybridPartitionConsumptionStateMap,
        true,
        true,
        true,
        ingestionNotificationDispatcher,
        (t, p) -> {},
        (t, p) -> {});
  }

  @Test
  public void testDataUpdatedWithStoreChangeListener() throws Exception {
    when(store.getStorageQuotaInByte()).thenReturn(newStoreQuotaInBytes);
    when(version.getStatus()).thenReturn(VersionStatus.ONLINE);

    // handleStoreChanged should have changed isVersionOnline, storeQuotaInBytes, and diskQuotaPerPartition
    quotaEnforcer.handleStoreChanged(store);
    Assert.assertTrue(quotaEnforcer.isVersionOnline());
    Assert.assertEquals(quotaEnforcer.getStoreQuotaInBytes(), newStoreQuotaInBytes);
    Assert.assertEquals(quotaEnforcer.getPartitionQuotaInBytes(), newStoreQuotaInBytes / storePartitionCount);

    // Quota is reported as not violated at the current stage.
    for (int i = 1; i <= storePartitionCount; i++) {
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(ingestionNotificationDispatcher, times(1)).reportQuotaNotViolated(partitionConsumptionState);
    }

    // Trigger quota violation to pause these partitions.
    verify(ingestionNotificationDispatcher, times(0)).reportQuotaViolated(any());
    addUsageToAllPartitions(20);
    verify(ingestionNotificationDispatcher, times(storePartitionCount * 2)).reportQuotaViolated(any());
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
      Assert.assertTrue(hybridQuotaEnforcer.isPartitionPausedIngestion(i));
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(ingestionNotificationDispatcher).reportQuotaViolated(partitionConsumptionState);
    }

    // handleStoreChanged should get these paused partitions back, when feature is disabled.
    when(store.isHybridStoreDiskQuotaEnabled()).thenReturn(false);
    quotaEnforcer.handleStoreChanged(store);
    hybridQuotaEnforcer.handleStoreChanged(store);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
      Assert.assertFalse(hybridQuotaEnforcer.isPartitionPausedIngestion(i));
      // Expect a new round of QuotaNotViolate are reported.
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(ingestionNotificationDispatcher, times(2)).reportQuotaNotViolated(partitionConsumptionState);
    }
  }

  @Test
  public void testHybridStoreBatchPushExceededQuota() throws Exception {
    // should not throw exception as the partitions size break write quota
    addUsageToAllPartitions(10);
  }

  @Test
  public void testHybridStorePushNotExceededQuota() throws Exception {
    // expect no exceptions
    addUsageToAllPartitions(5);
  }

  @Test
  public void testRTJobNotExceededQuota() throws Exception {
    setUpOnlineVersion();
    addUsageToAllPartitions(5);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
      Assert.assertFalse(hybridQuotaEnforcer.isPartitionPausedIngestion(i));
    }
  }

  @Test
  public void testRTJobExceededQuota() throws Exception {
    setUpOnlineVersion();

    // these partitions should be paused for exceeding write quota
    addUsageToAllPartitions(10);
    verify(ingestionNotificationDispatcher, times(storePartitionCount * 2)).reportQuotaViolated(any());
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(ingestionNotificationDispatcher).reportQuotaViolated(partitionConsumptionState);
    }

    // The later same partitions consumptions should not be paused too even with size zero
    addUsageToAllPartitions(0);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
    }

    // check after store change and bumping quota, paused partition should be resumed
    when(store.getStorageQuotaInByte()).thenReturn(newStoreQuotaInBytes);
    quotaEnforcer.handleStoreChanged(store);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
    }
  }

  @Test
  public void testReportCompletionForOnlineVersion() throws Exception {
    setUpOnlineVersion();
    addUsageToAllPartitions(10);
    for (int i = 1; i <= storePartitionCount; i++) {
      PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
      // The version is online but completion is not reported. We should expect the enforcer report completion.
      when(pcs.isCompletionReported()).thenReturn(false);
      when(pcs.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.STANDBY);
      partitionConsumptionStateMap.put(i, pcs);
    }
    verify(ingestionNotificationDispatcher, times(storePartitionCount)).reportCompleted(any());
    for (int i = 1; i <= storePartitionCount; i++) {
      // Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
      verify(ingestionNotificationDispatcher).reportCompleted(argThat(new PartitionNumberMatcher(i)));
    }
  }

  private static class PartitionNumberMatcher implements ArgumentMatcher<PartitionConsumptionState> {
    private final int expectedPartition;

    public PartitionNumberMatcher(int expectedPartition) {
      this.expectedPartition = expectedPartition;
    }

    @Override
    public boolean matches(PartitionConsumptionState argument) {
      return argument.getPartition() == expectedPartition;
    }
  }

  private void addUsageToAllPartitions(int partitionSize) {
    for (int i = 1; i <= storePartitionCount; i++) {
      quotaEnforcer.enforcePartitionQuota(i, partitionSize);
      hybridQuotaEnforcer.enforcePartitionQuota(i, partitionSize);
    }
  }

  private void setUpOnlineVersion() {
    when(version.getStatus()).thenReturn(VersionStatus.ONLINE);
    quotaEnforcer.handleStoreChanged(store);
  }
}
