package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.utils.StoragePartitionDiskUsage;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

@Test
public class HybridStoreQuotaEnforcementTest {

  private final long storeQuotaInBytes = 100l;
  private final long newStoreQuotaInBytes = 200l;
  private final int storePartitionCount = 10;
  private final String storeName = "TestTopic";
  private final String topic = Version.composeKafkaTopic(storeName, 1);
  private final int storeVersion = Version.parseVersionFromKafkaTopicName(topic);

  private StoreIngestionTask storeIngestionTask;
  private ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private AbstractStorageEngine storageEngine;
  private StoreIngestionTask.ReportStatusAdapter reportStatusAdapter;
  private Store store;
  private Version version;
  private Map<Integer, Integer> subscribedPartitionToSize;
  private HybridStoreQuotaEnforcement quotaEnforcer;

  @BeforeClass
  public void setUp() {
    storageEngine = mock(AbstractStorageEngine.class);
    store = mock(Store.class);
    version = mock(Version.class);
  }

  @BeforeMethod
  private void buildNewQuotaEnforcer() {
    storeIngestionTask = mock(StoreIngestionTask.class);
    reportStatusAdapter = mock(StoreIngestionTask.ReportStatusAdapter.class);
    partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    for (int i = 1; i <= storePartitionCount; i++) {
      PartitionConsumptionState pcs = new PartitionConsumptionState(i, 1, mock(OffsetRecord.class),true, true,
          IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);
      partitionConsumptionStateMap.put(i, pcs);
    }

    when(store.getName()).thenReturn(storeName);
    when(store.getStorageQuotaInByte()).thenReturn(storeQuotaInBytes);
    when(store.getPartitionCount()).thenReturn(storePartitionCount);
    when(store.getVersion(storeVersion)).thenReturn(Optional.of(version));
    when(store.isHybridStoreDiskQuotaEnabled()).thenReturn(true);
    when(version.getStatus()).thenReturn(VersionStatus.STARTED);
    when(storeIngestionTask.isMetricsEmissionEnabled()).thenReturn(false);
    when(storeIngestionTask.getReportStatusAdapter()).thenReturn(reportStatusAdapter);

    Map<Integer, StoragePartitionDiskUsage> partitionConsumptionSizeMap = new HashMap<>();
    quotaEnforcer = new HybridStoreQuotaEnforcement(storeIngestionTask,
                                                    storageEngine,
                                                    store,
                                                    topic,
                                                    storePartitionCount,
                                                    partitionConsumptionSizeMap,
                                                    partitionConsumptionStateMap);
  }

  @Test
  public void testDataUpdatedWithStoreChangeListener() throws Exception{
    when(store.getStorageQuotaInByte()).thenReturn(newStoreQuotaInBytes);
    when(version.getStatus()).thenReturn(VersionStatus.ONLINE);

    // handleStoreChanged should have changed isVersionOnline, storeQuotaInBytes, and diskQuotaPerPartition
    quotaEnforcer.handleStoreChanged(store);
    Assert.assertTrue(quotaEnforcer.isVersionOnline());
    Assert.assertEquals(quotaEnforcer.getStoreQuotaInBytes(), newStoreQuotaInBytes);
    Assert.assertEquals(quotaEnforcer.getPartitionQuotaInBytes(), newStoreQuotaInBytes/storePartitionCount);

    // Quota is reported as not violated at the current stage.
    for (int i = 1; i <= storePartitionCount; i++) {
      verify(storeIngestionTask, times(1)).reportQuotaNotViolated(i);
    }

    // Trigger quota violation to pause these partitions.
    buildDummyPartitionToSizeMap(20);
    runTest(() -> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
        verify(storeIngestionTask, times(1)).reportQuotaViolated(i);
      }
    });

    // handleStoreChanged should get these paused partitions back, when feature is disabled.
    when(store.isHybridStoreDiskQuotaEnabled()).thenReturn(false);
    quotaEnforcer.handleStoreChanged(store);
    for (int i = 1; i <= storePartitionCount; i++) {
      Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
      // Expect a new round of QuotaNotViolate are reported.
      verify(storeIngestionTask, times(2)).reportQuotaNotViolated(i);
    }
  }

  @Test
  public void testHybridStoreBatchPushExceededQuota() throws Exception {
    // should not throw exception as the partitions size break write quota
    buildDummyPartitionToSizeMap(10);
    runTest(() -> {});
  }

  @Test
  public void testHybridStorePushNotExceededQuota() throws Exception {
    // expect no exceptions
    buildDummyPartitionToSizeMap(5);
    runTest(() -> {});
  }

  @Test
  public void testRTJobNotExceededQuota() throws Exception {
    setUpOnlineVersion();
    buildDummyPartitionToSizeMap(5);
    runTest(() -> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
      }
    });
  }

  @Test
  public void testRTJobExceededQuota() throws Exception {
    setUpOnlineVersion();

    // these partitions should be paused for exceeding write quota
    buildDummyPartitionToSizeMap(10);
    runTest(() -> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
        verify(storeIngestionTask, times(1)).reportQuotaViolated(i);
      }
    });

    // The later same partitions consumptions should be paused too even with size zero
    buildDummyPartitionToSizeMap(0);
    runTest(() -> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
      }
    });

    // check after store change and bumping quota, paused partition should be resumed
    when(store.getStorageQuotaInByte()).thenReturn(newStoreQuotaInBytes);
    quotaEnforcer.handleStoreChanged(store);
    runTest(()-> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertFalse(quotaEnforcer.isPartitionPausedIngestion(i));
      }
    });
  }

  @Test
  public void testReportCompletionForOnlineVersion() throws Exception {
    setUpOnlineVersion();
    buildDummyPartitionToSizeMap(10);
    for (int i = 1; i <= storePartitionCount; i++) {
      PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
      // The version is online but completion is not reported. We should expect the enforcer report completion.
      when(pcs.isCompletionReported()).thenReturn(false);
      when(pcs.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.STANDBY);
      partitionConsumptionStateMap.put(i, pcs);
    }
    runTest(() -> {
      for (int i = 1; i <= storePartitionCount; i++) {
        verify(reportStatusAdapter, times(10)).reportCompleted(any());
        Assert.assertTrue(quotaEnforcer.isPartitionPausedIngestion(i));
      }
    });
  }

  private void runTest(Runnable assertions) throws Exception {
    quotaEnforcer.checkPartitionQuota(subscribedPartitionToSize);
    assertions.run();
  }

  private void buildDummyPartitionToSizeMap(int partitionSize) {
    subscribedPartitionToSize = new HashMap<>();
    for (int i = 1; i <= storePartitionCount; i++) {
      subscribedPartitionToSize.put(i, partitionSize);
    }
  }

  private void setUpOnlineVersion() {
    KafkaConsumerWrapper consumer = mock(KafkaConsumerWrapper.class);
    List<KafkaConsumerWrapper> consumerList = new ArrayList<>();
    consumerList.add(consumer);
    when(storeIngestionTask.getConsumers()).thenReturn(consumerList);
    when(version.getStatus()).thenReturn(VersionStatus.ONLINE);
    quotaEnforcer.handleStoreChanged(store);
  }
}
