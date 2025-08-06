package com.linkedin.venice.controller.logcompaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.repush.RepushJobRequest;
import com.linkedin.venice.controller.repush.RepushOrchestrator;
import com.linkedin.venice.controller.stats.LogCompactionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.dimensions.RepushStoreTriggerSource;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CompactionManagerTest {
  private static final long COMPACTION_THRESHOLD = 24; // 24 hours ago
  private static final String TEST_STORE_NAME_PREFIX = "test-store";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private CompactionManager testCompactionManager;
  private RepushOrchestrator mockRepushOrchestrator;
  private LogCompactionStats mockLogCompactionStats;

  @BeforeClass
  public void setUp() {
    mockRepushOrchestrator = mock(RepushOrchestrator.class);
    mockLogCompactionStats = mock(LogCompactionStats.class);

    testCompactionManager = new CompactionManager(
        mockRepushOrchestrator,
        TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD),
        Collections.singletonMap(TEST_CLUSTER_NAME, mockLogCompactionStats));
  }

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
    Version version6 = mock(Version.class);
    Version version7 = mock(Version.class);
    Version version8 = mock(Version.class);

    // set version number for Version mocks
    when(version1.getNumber()).thenReturn(currentVersionNumber);
    when(version2.getNumber()).thenReturn(currentVersionNumber);
    when(version3.getNumber()).thenReturn(currentVersionNumber);
    when(version4.getNumber()).thenReturn(currentVersionNumber);
    when(ongoingPushVersion.getNumber()).thenReturn(ongoingPushVersionNumber);
    when(version5.getNumber()).thenReturn(currentVersionNumber);
    when(version6.getNumber()).thenReturn(currentVersionNumber);
    when(version7.getNumber()).thenReturn(currentVersionNumber);
    when(version8.getNumber()).thenReturn(currentVersionNumber);

    // set createTime for Version mocks

    // 25 hours
    when(version1.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD + 1));

    // 48 hours
    when(version2.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD * 2));

    // 23 hours
    when(version3.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD - 1));

    // 48 hours
    when(version4.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD * 2));

    // 23 hours
    when(ongoingPushVersion.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD - 1));

    // 23 hours
    when(version5.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD - 1));

    // 25 hours
    when(version6.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD + 1));

    // 25 hours
    when(version7.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD + 1));

    // 25 hours
    when(version8.getCreatedTime())
        .thenReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD + 1));

    // Mock StoreInfo instances
    StoreInfo store1 = new StoreInfo();
    StoreInfo store2 = new StoreInfo();
    StoreInfo store3 = new StoreInfo();
    StoreInfo store4 = new StoreInfo();
    StoreInfo store5 = new StoreInfo();
    StoreInfo store6 = new StoreInfo();
    StoreInfo store7 = new StoreInfo();
    StoreInfo store8 = new StoreInfo();

    // Set store names
    store1.setName(TEST_STORE_NAME_PREFIX + "1");
    store2.setName(TEST_STORE_NAME_PREFIX + "2");
    store3.setName(TEST_STORE_NAME_PREFIX + "3");
    store4.setName(TEST_STORE_NAME_PREFIX + "4");
    store5.setName(TEST_STORE_NAME_PREFIX + "5");
    store6.setName(TEST_STORE_NAME_PREFIX + "6");
    store7.setName(TEST_STORE_NAME_PREFIX + "7");
    store8.setName(TEST_STORE_NAME_PREFIX + "8");

    // Return Version mocks when getVersion() is called
    store1.setVersions(Collections.singletonList(version1));
    store2.setVersions(Collections.singletonList(version2));
    store3.setVersions(Collections.singletonList(version3));
    store4.setVersions(Arrays.asList(version4, ongoingPushVersion));
    store5.setVersions(Collections.singletonList(version5));
    store6.setVersions(Collections.singletonList(version6));
    store7.setVersions(Collections.singletonList(version7));
    store8.setVersions(Collections.singletonList(version8));

    // Mock HybridStoreConfig for the first two StoreInfo instances
    store1.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store2.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store4.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store5.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store6.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store7.setHybridStoreConfig(mock(HybridStoreConfig.class));
    store8.setHybridStoreConfig(mock(HybridStoreConfig.class));

    // Set isActiveActiveReplicationEnabled for the first two StoreInfo instances
    store1.setActiveActiveReplicationEnabled(true);
    store2.setActiveActiveReplicationEnabled(true);
    store3.setActiveActiveReplicationEnabled(true);
    store4.setActiveActiveReplicationEnabled(true);
    store5.setActiveActiveReplicationEnabled(false);
    store6.setActiveActiveReplicationEnabled(true);
    store7.setActiveActiveReplicationEnabled(true);
    store8.setActiveActiveReplicationEnabled(true);

    // Set compaction enabled for all but store6
    store1.setCompactionEnabled(true);
    store2.setCompactionEnabled(true);
    store3.setCompactionEnabled(true);
    store4.setCompactionEnabled(true);
    store5.setCompactionEnabled(true);
    store6.setCompactionEnabled(false);
    store7.setCompactionEnabled(true);
    store8.setCompactionEnabled(true);

    // Set store-level compaction threshold
    store1.setCompactionThreshold(-1);
    store2.setCompactionThreshold(-1);
    store3.setCompactionThreshold(-1);
    store4.setCompactionThreshold(-1);
    store5.setCompactionThreshold(-1);
    store6.setCompactionThreshold(-1);
    store7.setCompactionThreshold(TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD) * 2);
    store8.setCompactionThreshold(TimeUnit.HOURS.toMillis(COMPACTION_THRESHOLD) / 2);

    // Add StoreInfo instances to the list
    storeInfoList.add(store1);
    storeInfoList.add(store2);
    storeInfoList.add(store3);
    storeInfoList.add(store4);
    storeInfoList.add(store5);
    storeInfoList.add(store6);
    storeInfoList.add(store7);
    storeInfoList.add(store8);

    // Verify stores compaction-ready status
    assertTrue(testCompactionManager.isCompactionReady(store1)); // compacted more than threshold (>24hrs)
    assertTrue(testCompactionManager.isCompactionReady(store2)); // compacted more than threshold (>24hrs)
    assertFalse(testCompactionManager.isCompactionReady(store3)); // compacted within threshold (<24hrs)
    assertFalse(testCompactionManager.isCompactionReady(store4)); // ongoing push version threshold (<24hrs)
    assertFalse(testCompactionManager.isCompactionReady(store5)); // non-AA store
    assertFalse(testCompactionManager.isCompactionReady(store6)); // Store level compaction disabled
    assertFalse(testCompactionManager.isCompactionReady(store7)); // Store level threshold not reached
    assertTrue(testCompactionManager.isCompactionReady(store8)); // Store level threshold reached

    // Test
    List<StoreInfo> compactionReadyStores =
        testCompactionManager.filterStoresForCompaction(storeInfoList, TEST_CLUSTER_NAME);

    // Validate recordStoreNominatedForCompactionCount metric emission
    verify(mockLogCompactionStats, Mockito.times(1)).recordStoreNominatedForCompactionCount(store1.getName());
    verify(mockLogCompactionStats, Mockito.times(1)).recordStoreNominatedForCompactionCount(store2.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).recordStoreNominatedForCompactionCount(store3.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).recordStoreNominatedForCompactionCount(store4.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).recordStoreNominatedForCompactionCount(store5.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).recordStoreNominatedForCompactionCount(store6.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).recordStoreNominatedForCompactionCount(store7.getName());
    verify(mockLogCompactionStats, Mockito.times(1)).recordStoreNominatedForCompactionCount(store8.getName());

    // Validate setCompactionEligible metric emission
    verify(mockLogCompactionStats, Mockito.times(1)).setCompactionEligible(store1.getName());
    verify(mockLogCompactionStats, Mockito.times(1)).setCompactionEligible(store2.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).setCompactionEligible(store3.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).setCompactionEligible(store4.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).setCompactionEligible(store5.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).setCompactionEligible(store6.getName());
    verify(mockLogCompactionStats, Mockito.times(0)).setCompactionEligible(store7.getName());
    verify(mockLogCompactionStats, Mockito.times(1)).setCompactionEligible(store8.getName());

    // Test validation
    assertEquals(compactionReadyStores.size(), 3);
    assertTrue(compactionReadyStores.contains(store1));
    assertTrue(compactionReadyStores.contains(store2));
    assertFalse(compactionReadyStores.contains(store3));
    assertFalse(compactionReadyStores.contains(store4));
    assertFalse(compactionReadyStores.contains(store5));
    assertFalse(compactionReadyStores.contains(store6));
    assertFalse(compactionReadyStores.contains(store7));
    assertTrue(compactionReadyStores.contains(store8));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testRepushStoreWithNullResponse() throws Exception {
    // Setup a mocked RepushOrchestrator that returns null on repush()
    when(mockRepushOrchestrator.repush(any())).thenReturn(null);

    // Create a RepushJobRequest
    RepushJobRequest repushJobRequest = new RepushJobRequest(
        TEST_CLUSTER_NAME,
        Utils.getUniqueString(TEST_STORE_NAME_PREFIX),
        RepushStoreTriggerSource.MANUAL);

    // Call the repushStore method and expect a VeniceException
    testCompactionManager.repushStore(repushJobRequest);
  }
}
