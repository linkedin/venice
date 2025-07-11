package com.linkedin.venice.controller.logcompaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.repush.RepushJobRequest;
import com.linkedin.venice.controller.repush.RepushOrchestrator;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestCompactionManager {
  private static final long TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD = 24; // 24 hours ago
  private static final String TEST_STORE_NAME_PREFIX = "test-store";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private CompactionManager testCompactionManager;
  private RepushOrchestrator mockRepushOrchestrator;

  @BeforeClass
  public void setUp() {
    mockRepushOrchestrator = mock(RepushOrchestrator.class);
    testCompactionManager = new CompactionManager(
        mockRepushOrchestrator,
        TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD));
  }

  /**
   * Expected result
     * store1 (eligible): isHybrid = true, lastCompactionTime >= threshold, AA store
     * store2 (eligible): isHybrid = true, lastCompactionTime >= threshold, AA store
     * store3 (ineligible): isHybrid = false (ineligiblity reason), lastCompactionTime <= threshold, AA store
     * store4 (ineligible): isHybrid = true, lastCompactionTime >= threshold, AA store, Note: has 2 versions;
      * ongoing push version: lastCompactionTime <= threshold (ineligiblity reason)
      * current version: lastCompactionTime >= threshold
     * store5 (ineligible): isHybrid = false, lastCompactionTime <= threshold, non-AA store (ineligiblity reason)
   *
   */
  @Test
  public void testFilterStoresForCompaction() {
    ArrayList<StoreInfo> storeInfoList = new ArrayList<>();

    // setup test store Version mocks
    int currentVersionNumber = 2;
    int ongoingPushVersionNumber = 3;
    Version version1 = mock(Version.class);
    Version version2 = mock(Version.class);
    Version version3 = mock(Version.class);
    Version version4 = mock(Version.class);
    Version ongoingPushVersion = mock(Version.class);
    Version version5 = mock(Version.class);

    // set version number for Version mocks
    when(version1.getNumber()).thenReturn(currentVersionNumber);
    when(version2.getNumber()).thenReturn(currentVersionNumber);
    when(version3.getNumber()).thenReturn(currentVersionNumber);
    when(version4.getNumber()).thenReturn(currentVersionNumber);
    when(ongoingPushVersion.getNumber()).thenReturn(ongoingPushVersionNumber);
    when(version5.getNumber()).thenReturn(currentVersionNumber);

    // set createTime for Version mocks
    long version1CreationTime =
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD + 1);
    when(version1.getCreatedTime()).thenReturn(version1CreationTime); // 25hrs ago
    when(version2.getCreatedTime()).thenReturn(
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD * 2)); // 48hrs
                                                                                                                   // ago
    when(version3.getCreatedTime()).thenReturn(
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD - 1)); // 23hrs
                                                                                                                   // ago
    when(version4.getCreatedTime()).thenReturn(
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD * 2)); // 48hrs
                                                                                                                   // ago
    when(ongoingPushVersion.getCreatedTime()).thenReturn(
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD - 1)); // 23hrs

    when(version5.getCreatedTime()).thenReturn(
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD - 1)); // 23hrs
                                                                                                                   // ago

    // Mock StoreInfo instances
    StoreInfo store1 = new StoreInfo();
    StoreInfo store2 = new StoreInfo();
    StoreInfo store3 = new StoreInfo();
    StoreInfo store4 = new StoreInfo();
    StoreInfo store5 = new StoreInfo();
    StoreInfo store6 = new StoreInfo();

    // Set store names
    store1.setName(TEST_STORE_NAME_PREFIX + "1");
    store2.setName(TEST_STORE_NAME_PREFIX + "2");
    store3.setName(TEST_STORE_NAME_PREFIX + "3");
    store4.setName(TEST_STORE_NAME_PREFIX + "4");
    store5.setName(TEST_STORE_NAME_PREFIX + "5");
    store6.setName(TEST_STORE_NAME_PREFIX + "6");

    // Return Version mocks when getVersion() is called
    store1.setVersions(Collections.singletonList(version1));
    store2.setVersions(Collections.singletonList(version2));
    store3.setVersions(Collections.singletonList(version3));
    store4.setVersions(Arrays.asList(version4, ongoingPushVersion));
    store5.setVersions(Collections.singletonList(version5));
    store6.setVersions(Collections.singletonList(version1));

    // Mock HybridStoreConfig for the first two StoreInfo instances
    store1.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store2.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store4.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store5.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store6.setHybridStoreConfig(mock(HybridStoreConfig.class));

    // Set isActiveActiveReplicationEnabled for the first two StoreInfo instances
    store1.setActiveActiveReplicationEnabled(true);
    store2.setActiveActiveReplicationEnabled(true);
    store3.setActiveActiveReplicationEnabled(true);
    store4.setActiveActiveReplicationEnabled(true);
    store5.setActiveActiveReplicationEnabled(false);
    store6.setActiveActiveReplicationEnabled(true);

    // Set compaction enabled for all but store6
    store1.setCompactionEnabled(true);
    store2.setCompactionEnabled(true);
    store3.setCompactionEnabled(true);
    store4.setCompactionEnabled(true);
    store5.setCompactionEnabled(true);

    // Add StoreInfo instances to the list
    storeInfoList.add(store1);
    storeInfoList.add(store2);
    storeInfoList.add(store3);
    storeInfoList.add(store4);
    storeInfoList.add(store5);
    storeInfoList.add(store6);

    // Verify stores compaction-ready status
    Assert.assertTrue(testCompactionManager.isCompactionReady(store1)); // compacted more than threshold time (>24hrs)
    Assert.assertTrue(testCompactionManager.isCompactionReady(store2)); // compacted more than threshold time (>24hrs)
    Assert.assertFalse(testCompactionManager.isCompactionReady(store3)); // compacted within threshold time (<24hrs)
    Assert.assertFalse(testCompactionManager.isCompactionReady(store4)); // ongoing push version within threshold time
                                                                         // (<24hrs)
    Assert.assertFalse(testCompactionManager.isCompactionReady(store5)); // non-AA store
    Assert.assertFalse(testCompactionManager.isCompactionReady(store6)); // config false

    // Test
    List<StoreInfo> compactionReadyStores = testCompactionManager.filterStoresForCompaction(storeInfoList);

    // Test validation
    assertEquals(compactionReadyStores.size(), 2); // change if the number of eligible test stores in the list changes
    assertTrue(compactionReadyStores.contains(store1));
    assertTrue(compactionReadyStores.contains(store2));
    assertFalse(compactionReadyStores.contains(store3));
    assertFalse(compactionReadyStores.contains(store4));
    assertFalse(compactionReadyStores.contains(store5));
    assertFalse(compactionReadyStores.contains(store6));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testRepushStoreWithNullResponse() throws Exception {
    // Setup a mocked RepushOrchestrator that returns null on repush()
    when(mockRepushOrchestrator.repush(any())).thenReturn(null);

    // Create a RepushJobRequest
    RepushJobRequest repushJobRequest = new RepushJobRequest(
        TEST_CLUSTER_NAME,
        Utils.getUniqueString(TEST_STORE_NAME_PREFIX),
        RepushJobRequest.MANUAL_TRIGGER);

    // Call the repushStore method and expect a VeniceException
    testCompactionManager.repushStore(repushJobRequest);
  }
}
