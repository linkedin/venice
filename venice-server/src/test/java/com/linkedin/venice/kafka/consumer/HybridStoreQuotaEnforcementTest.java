package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.StorageQuotaExceededException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.store.AbstractStorageEngine;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
  private AbstractStorageEngine storageEngine;
  private Store store;
  private Version version;
  private Map<Integer, Integer> subscribedPartitionToSize;
  private HybridStoreQuotaEnforcement quotaEnforcer;

  @BeforeClass
  public void setup() {
    storeIngestionTask = mock(StoreIngestionTask.class);
    storageEngine = mock(AbstractStorageEngine.class);
    store = mock(Store.class);
    version = mock(Version.class);
  }

  @BeforeMethod
  private void buildNewQuotaEnforcer() {
    when(store.getName()).thenReturn(storeName);
    when(store.getStorageQuotaInByte()).thenReturn(storeQuotaInBytes);
    when(store.getPartitionCount()).thenReturn(storePartitionCount);
    when(store.getVersion(storeVersion)).thenReturn(Optional.of(version));
    when(version.getStatus()).thenReturn(VersionStatus.STARTED);
    when(storeIngestionTask.isMetricsEmissionEnabled()).thenReturn(false);

    quotaEnforcer = new HybridStoreQuotaEnforcement(storeIngestionTask,
                                                    storageEngine,
                                                    store,
                                                    topic,
                                                    storePartitionCount);
  }

  @Test
  public void testDataUpdatedWithStoreChangeListener() {
    when(store.getStorageQuotaInByte()).thenReturn(newStoreQuotaInBytes);
    when(version.getStatus()).thenReturn(VersionStatus.ONLINE);

    // handleStoreChanged should have changed isRTJob, storeQuotaInBytes, and diskQuotaPerPartition
    quotaEnforcer.handleStoreChanged(store);
    Assert.assertTrue(quotaEnforcer.isRTJob());
    Assert.assertEquals(quotaEnforcer.getStoreQuotaInBytes(), newStoreQuotaInBytes);
    Assert.assertEquals(quotaEnforcer.getPartitionQuotaInBytes(), newStoreQuotaInBytes/storePartitionCount);
  }

  @Test(expectedExceptions = StorageQuotaExceededException.class)
  public void testHybridStoreBatchPushExceededQuota() throws Exception {
    // should throw exception as the partitions size break write quota
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
    setUpRTJob();
    buildDummyPartitionToSizeMap(5);
    runTest(() -> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertFalse(quotaEnforcer.isPartitionOutOfQuota(i));
      }
    });
  }

  @Test
  public void testRTJobExceededQuota() throws Exception {
    setUpRTJob();

    // these partitions should be paused for exceeding write quota
    buildDummyPartitionToSizeMap(10);
    runTest(() -> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertTrue(quotaEnforcer.isPartitionOutOfQuota(i));
      }
    });

    // The later same partitions consumptions should be paused too even with size zero
    buildDummyPartitionToSizeMap(0);
    runTest(() -> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertTrue(quotaEnforcer.isPartitionOutOfQuota(i));
      }
    });

    // check after store change and bumping quota, paused partition should be resumed
    when(store.getStorageQuotaInByte()).thenReturn(newStoreQuotaInBytes);
    quotaEnforcer.handleStoreChanged(store);
    runTest(()-> {
      for (int i = 1; i <= storePartitionCount; i++) {
        Assert.assertFalse(quotaEnforcer.isPartitionOutOfQuota(i));
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

  private void setUpRTJob() {
    KafkaConsumerWrapper consumer = mock(KafkaConsumerWrapper.class);
    when(storeIngestionTask.getConsumer()).thenReturn(consumer);
    when(version.getStatus()).thenReturn(VersionStatus.ONLINE);
    quotaEnforcer.handleStoreChanged(store);
  }
}
