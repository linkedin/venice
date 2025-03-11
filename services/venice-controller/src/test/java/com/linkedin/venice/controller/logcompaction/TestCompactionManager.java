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
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestCompactionManager {
  private static final long TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD = 24; // 24 hours ago
  private CompactionManager testCompactionManager;
  private RepushOrchestrator mockRepushOrchestrator;

  @BeforeClass
  public void setUp() {
    mockRepushOrchestrator = mock(RepushOrchestrator.class);
    testCompactionManager = new CompactionManager(
        mockRepushOrchestrator,
        TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD));
  }

  @Test
  public void testFilterStoresForCompaction() {
    ArrayList<StoreInfo> storeInfoList = new ArrayList<>();

    // Mock StoreInfo instances
    StoreInfo store1 = new StoreInfo();
    StoreInfo store2 = new StoreInfo();
    StoreInfo store3 = new StoreInfo();

    // Mock HybridStoreConfig for the first two StoreInfo instances
    HybridStoreConfig hybridStoreConfig1 = mock(HybridStoreConfig.class);
    HybridStoreConfig hybridStoreConfig2 = mock(HybridStoreConfig.class);
    store1.setHybridStoreConfig(hybridStoreConfig1);
    store2.setHybridStoreConfig(hybridStoreConfig2);
    // when(store3.getHybridStoreConfig()).thenReturn(null);

    // Mock version numbers with random numbers
    int store1VersionNumber = 1;
    int store2VersionNumber = 2;
    int store3VersionNumber = 3;
    store1.setCurrentVersion(store1VersionNumber);
    store2.setCurrentVersion(store2VersionNumber);
    store3.setCurrentVersion(store3VersionNumber);

    // Mock Version instances
    Version version1 = new VersionImpl(Utils.getUniqueString("store"), store1VersionNumber);
    Version version2 = new VersionImpl(Utils.getUniqueString("store"), store2VersionNumber);
    Version version3 = new VersionImpl(Utils.getUniqueString("store"), store3VersionNumber);

    // Return Version mocks when getVersion() is called
    store1.setVersions(new ArrayList<>(Collections.singletonList(version1)));
    store2.setVersions(new ArrayList<>(Collections.singletonList(version2)));
    store3.setVersions(new ArrayList<>(Collections.singletonList(version3)));

    // Set createTime for Version mocks
    version1.setAge(Duration.ofDays(TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD + 1))); // 25
                                                                                                                   // hours
                                                                                                                   // ago
    version2.setAge(Duration.ofDays(TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD * 2))); // 48
                                                                                                                   // hours
                                                                                                                   // ago
    version3.setAge(Duration.ofDays(TimeUnit.HOURS.toMillis(TEST_HOURS_SINCE_LAST_LOG_COMPACTION_THRESHOLD - 1))); // 23
                                                                                                                   // hours
                                                                                                                   // ago

    // Add StoreInfo instances to the list
    storeInfoList.add(store1);
    storeInfoList.add(store2);
    storeInfoList.add(store3);

    // Verify stores compaction-ready status
    Assert.assertTrue(testCompactionManager.isCompactionReady(store1));
    Assert.assertTrue(testCompactionManager.isCompactionReady(store2));
    Assert.assertFalse(testCompactionManager.isCompactionReady(store3));

    // Test
    List<StoreInfo> compactionReadyStores = testCompactionManager.filterStoresForCompaction(storeInfoList);

    // Test validation
    assertEquals(compactionReadyStores.size(), 2); // change if the number of eligible test stores in the list changes
    assertTrue(compactionReadyStores.contains(store1));
    assertTrue(compactionReadyStores.contains(store2));
    assertFalse(compactionReadyStores.contains(store3));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testCompactStoreWithNullResponse() throws Exception {
    // Setup a mocked RepushOrchestrator that returns null on repush()
    when(mockRepushOrchestrator.repush(any())).thenReturn(null);

    // Create a RepushJobRequest
    RepushJobRequest repushJobRequest =
        new RepushJobRequest(Utils.getUniqueString("store"), RepushJobRequest.MANUAL_TRIGGER);

    // Call the compactStore method and expect a VeniceException
    testCompactionManager.compactStore(repushJobRequest);
  }
}
