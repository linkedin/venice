package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.*;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class StorageUtilizationManagerTest {
  private final long storeQuotaInBytes = 100l;
  private final long newStoreQuotaInBytes = 200l;
  private final int storePartitionCount = 10;
  private final String storeName = "TestTopic";
  private final String topic = Version.composeKafkaTopic(storeName, 1);
  private final int storeVersion = Version.parseVersionFromKafkaTopicName(topic);

  private ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private AbstractStorageEngine storageEngine;
  private ReportStatusAdapter reportStatusAdapter;
  private Store store;
  private Version version;
  private StorageUtilizationManager quotaEnforcer;

  @BeforeClass
  public void setUp() {
    storageEngine = mock(AbstractStorageEngine.class);
    store = mock(Store.class);
    version = mock(Version.class);
  }

  @BeforeMethod
  private void buildNewQuotaEnforcer() {
    reportStatusAdapter = mock(ReportStatusAdapter.class);
    partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    for (int i = 1; i <= storePartitionCount; i++) {
      PartitionConsumptionState pcs = new PartitionConsumptionState(
          i,
          1,
          mock(OffsetRecord.class),
          true,
          true,
          IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);
      partitionConsumptionStateMap.put(i, pcs);
    }

    when(store.getName()).thenReturn(storeName);
    when(store.getStorageQuotaInByte()).thenReturn(storeQuotaInBytes);
    when(store.getPartitionCount()).thenReturn(storePartitionCount);
    when(store.getVersion(storeVersion)).thenReturn(Optional.of(version));
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
        reportStatusAdapter,
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
      verify(reportStatusAdapter, times(1)).reportQuotaNotViolated(partitionConsumptionState);
    }

    // Trigger quota violation to pause these partitions.
    verify(reportStatusAdapter, times(0)).reportQuotaViolated(any());
    addUsageToAllPartitions(20);
    verify(reportStatusAdapter, times(storePartitionCount)).reportQuotaViolated(any());
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(reportStatusAdapter).reportQuotaViolated(partitionConsumptionState);
    }

    // handleStoreChanged should get these paused partitions back, when feature is disabled.
    when(store.isHybridStoreDiskQuotaEnabled()).thenReturn(false);
    quotaEnforcer.handleStoreChanged(store);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
      // Expect a new round of QuotaNotViolate are reported.
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(reportStatusAdapter, times(2)).reportQuotaNotViolated(partitionConsumptionState);
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
    }
  }

  @Test
  public void testRTJobExceededQuota() throws Exception {
    setUpOnlineVersion();

    // these partitions should be paused for exceeding write quota
    addUsageToAllPartitions(10);
    verify(reportStatusAdapter, times(storePartitionCount)).reportQuotaViolated(any());
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(i);
      verify(reportStatusAdapter).reportQuotaViolated(partitionConsumptionState);
    }

    // The later same partitions consumptions should be paused too even with size zero
    addUsageToAllPartitions(0);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
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
    verify(reportStatusAdapter, times(storePartitionCount)).reportCompleted(any());
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
      verify(reportStatusAdapter).reportCompleted(argThat(new PartitionNumberMatcher(i)));
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
}
