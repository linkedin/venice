package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.StorageUtilizationManager.PausedConsumptionReason;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test
public class StorageUtilizationManagerTest {
  private final static long storeQuotaInBytes = 100L;
  private final static long newStoreQuotaInBytes = 200L;
  private final static int maxRecordSizeBytes = 2 * BYTES_PER_MB;
  private final static int newMaxRecordSizeBytes = 4 * BYTES_PER_MB;
  private final static int storePartitionCount = 10;
  private final static String storeName = "TestTopic";
  private final static String topic = Version.composeKafkaTopic(storeName, 1);
  private final static int storeVersion = Version.parseVersionFromKafkaTopicName(topic);

  private ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private AbstractStorageEngine storageEngine;
  private IngestionNotificationDispatcher ingestionNotificationDispatcher;
  private Store store;
  private Version version;
  private Set<Integer> pausedPartitions;
  private Set<Integer> resumedPartitions;
  private StorageUtilizationManager quotaEnforcer;

  @BeforeClass
  public void setUp() {
    storageEngine = mock(AbstractStorageEngine.class);
    store = mock(Store.class);
    version = mock(Version.class);
  }

  @BeforeMethod
  public void buildNewQuotaEnforcer() {
    ingestionNotificationDispatcher = mock(IngestionNotificationDispatcher.class);
    pausedPartitions = new HashSet<>();
    resumedPartitions = new HashSet<>();
    partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    for (int i = 1; i <= storePartitionCount; i++) {
      PartitionConsumptionState pcs =
          new PartitionConsumptionState(Utils.getReplicaId(topic, i), i, mock(OffsetRecord.class), true);
      partitionConsumptionStateMap.put(i, pcs);
    }

    when(store.getName()).thenReturn(storeName);
    when(store.getStorageQuotaInByte()).thenReturn(storeQuotaInBytes);
    when(store.getMaxRecordSizeBytes()).thenReturn(maxRecordSizeBytes);
    when(store.getPartitionCount()).thenReturn(storePartitionCount);
    when(store.getVersion(storeVersion)).thenReturn(version);
    when(store.isHybridStoreDiskQuotaEnabled()).thenReturn(true);
    when(version.getStatus()).thenReturn(VersionStatus.STARTED);

    quotaEnforcer = new StorageUtilizationManager(
        storageEngine,
        store,
        topic,
        storePartitionCount,
        partitionConsumptionStateMap,
        true,
        true,
        ingestionNotificationDispatcher,
        (t, p) -> {
          pausedPartitions.add(p);
        },
        (t, p) -> {
          resumedPartitions.add(p);
        });
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
    verify(ingestionNotificationDispatcher, times(storePartitionCount)).reportQuotaViolated(any());
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertTrue(quotaEnforcer.isPartitionPausedForQuotaExceeded(i));
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(ingestionNotificationDispatcher).reportQuotaViolated(partitionConsumptionState);
    }

    // handleStoreChanged should get these paused partitions back, when feature is disabled.
    when(store.isHybridStoreDiskQuotaEnabled()).thenReturn(false);
    quotaEnforcer.handleStoreChanged(store);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedForQuotaExceeded(i));
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
      Assert.assertFalse(quotaEnforcer.isPartitionPausedForQuotaExceeded(i));
    }
  }

  @Test
  public void testRTJobExceededQuota() throws Exception {
    setUpOnlineVersion();

    // these partitions should be paused for exceeding write quota
    addUsageToAllPartitions(10);
    verify(ingestionNotificationDispatcher, times(storePartitionCount)).reportQuotaViolated(any());
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertTrue(quotaEnforcer.isPartitionPausedForQuotaExceeded(i));
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(ingestionNotificationDispatcher).reportQuotaViolated(partitionConsumptionState);
    }

    // The later same partitions consumptions should be paused too even with size zero
    addUsageToAllPartitions(0);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertTrue(quotaEnforcer.isPartitionPausedForQuotaExceeded(i));
    }

    // check after store change and bumping quota, paused partition should be resumed
    when(store.getStorageQuotaInByte()).thenReturn(newStoreQuotaInBytes);
    quotaEnforcer.handleStoreChanged(store);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedForQuotaExceeded(i));
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
      Assert.assertTrue(quotaEnforcer.isPartitionPausedForQuotaExceeded(i));
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
    }
  }

  private void setUpOnlineVersion() {
    when(version.getStatus()).thenReturn(VersionStatus.ONLINE);
    quotaEnforcer.handleStoreChanged(store);
  }

  /**
   * Pause a partition for record too large and resume it after the size limit is increased.
   */
  @Test
  public void testPausePartitionForRecordTooLarge() throws Exception {
    int pausedPartition = 1;
    Assert.assertFalse(quotaEnforcer.isPartitionPausedForRecordTooLarge(pausedPartition));
    quotaEnforcer.pausePartitionForRecordTooLarge(pausedPartition, topic);
    for (int partition = 1; partition <= storePartitionCount; partition++) {
      boolean isPaused = quotaEnforcer.isPartitionPausedForRecordTooLarge(partition);
      if (partition == pausedPartition) {
        Assert.assertTrue(isPaused, "The one selected partition should be paused for record too large");
      } else {
        Assert.assertFalse(isPaused, "Every other partition should not remain paused");
      }
    }

    // After increasing the size limit, handleStoreChanged() should resume consumption for the partition
    when(store.getMaxRecordSizeBytes()).thenReturn(newMaxRecordSizeBytes);
    quotaEnforcer.handleStoreChanged(store);
    for (int partition = 1; partition <= storePartitionCount; partition++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedForRecordTooLarge(partition), "No partition should be paused");
    }
  }

  @DataProvider(name = "PausedConsumptionReasons")
  public static Object[][] pausedConsumptionReasonProvider() {
    return new Object[][] { { PausedConsumptionReason.QUOTA_EXCEEDED, PausedConsumptionReason.RECORD_TOO_LARGE },
        { PausedConsumptionReason.RECORD_TOO_LARGE, PausedConsumptionReason.QUOTA_EXCEEDED } };
  }

  /**
   * Partitions can only resume consumption when they are not paused for any reason.
   * This test method verifies this by pausing a subset of partitions for one reason and then attempting to resume them
   * for another reason. The subset of partitions that were paused for the first reason should not be resumed.
   */
  // TODO: possibly better naming?
  @Test(dataProvider = "PausedConsumptionReasons")
  public void testPartialResumePartitions(
      PausedConsumptionReason partialPauseReason,
      PausedConsumptionReason fullPauseReason) throws Exception {

    setUpOnlineVersion();

    // Pause only a subset of all partitions for the first reason
    HashSet<Integer> pausedPartitionsSubset = new HashSet<>(Arrays.asList(1, 3, 5, 7, 9));
    for (int partition = 1; partition <= storePartitionCount; partition++) {
      if (pausedPartitionsSubset.contains(partition)) {
        quotaEnforcer.pausePartition(partition, topic, partialPauseReason);
        Assert.assertTrue(quotaEnforcer.isPartitionPausedForReason(partition, partialPauseReason));
      } else {
        Assert.assertFalse(quotaEnforcer.isPartitionPausedForReason(partition, partialPauseReason));
      }
    }

    // Pause all partitions for the second reason
    for (int partition = 1; partition <= storePartitionCount; partition++) {
      quotaEnforcer.pausePartition(partition, topic, fullPauseReason);
      Assert.assertTrue(quotaEnforcer.isPartitionPausedForReason(partition, fullPauseReason));
    }

    Assert.assertTrue(resumedPartitions.isEmpty(), "No partitions should have been resumed yet.");

    // Resume consumption for all partitions for the second reason, which should call resumeAllPartitionsIfPossible()
    switch (fullPauseReason) {
      case QUOTA_EXCEEDED:
        when(store.isHybridStoreDiskQuotaEnabled()).thenReturn(false);
        break;
      case RECORD_TOO_LARGE:
        when(store.getMaxRecordSizeBytes()).thenReturn(newMaxRecordSizeBytes);
        break;
      default:
        Assert.fail("Invalid PausedConsumptionReason provided.");
    }
    quotaEnforcer.handleStoreChanged(store);

    // No partitions should remain paused for the second reason, but all partitions remain paused for the first reason
    for (int partition = 1; partition <= storePartitionCount; partition++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedForReason(partition, fullPauseReason));
      boolean isResumed = resumedPartitions.contains(partition);
      boolean isPaused = quotaEnforcer.isPartitionPausedForReason(partition, partialPauseReason);
      if (pausedPartitionsSubset.contains(partition)) {
        Assert.assertFalse(isResumed, "Partition should not have been resumed by handleStoreChanged()");
        Assert.assertTrue(isPaused, "Partition should remain paused for the first reason");
      } else {
        Assert.assertTrue(isResumed, "Partition should have been resumed by handleStoreChanged()");
        Assert.assertFalse(isPaused, "Partition should no longer be paused after bumping the setting");
      }
    }
  }
}
