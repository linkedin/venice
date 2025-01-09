package com.linkedin.venice.controller;

import static org.mockito.Mockito.*;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDeferredVersionSwapService {
  private VeniceParentHelixAdmin admin;
  private VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private static final String clusterName = "testCluster";

  private static final String region1 = "test1";
  private static final String region2 = "test2";

  @BeforeMethod
  public void setUp() {
    admin = mock(VeniceParentHelixAdmin.class);
    veniceControllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);

    Set<String> clusters = new HashSet<>();
    clusters.add(clusterName);
    doReturn(clusters).when(veniceControllerMultiClusterConfig).getClusters();
    doReturn(10L).when(veniceControllerMultiClusterConfig).getDeferredVersionSwapSleepMs();
  }

  private Store mockStore(
      int currentVersion,
      int waitTime,
      String targetRegions,
      Map<Integer, VersionStatus> versions,
      String name) {
    Store store = mock(Store.class);
    doReturn(waitTime).when(store).getTargetSwapRegionWaitTime();
    doReturn(targetRegions).when(store).getTargetSwapRegion();
    doReturn(currentVersion).when(store).getCurrentVersion();

    List<Version> versionList = new ArrayList<>();
    versions.forEach((n, s) -> {
      Version v = mock(Version.class);
      doReturn(n).when(v).getNumber();
      doReturn(s).when(v).getStatus();
      versionList.add(v);
    });
    doReturn(versionList).when(store).getVersions();
    doReturn(name).when(store).getName();

    return store;
  }

  private Admin.OfflinePushStatusInfo getOfflinePushStatusInfo(
      String region1Status,
      String region2Status,
      Long region1Timestamp,
      Long region2Timestamp) {
    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put(region1, region1Status);
    extraInfo.put(region2, region2Status);

    Map<String, Long> extraInfoUpdateTimestamp = new HashMap<>();
    extraInfoUpdateTimestamp.put(region1, region1Timestamp);
    extraInfoUpdateTimestamp.put(region2, region2Timestamp);

    return new Admin.OfflinePushStatusInfo(
        ExecutionStatus.COMPLETED,
        123L,
        extraInfo,
        null,
        null,
        extraInfoUpdateTimestamp);
  }

  @Test
  public void testDeferredVersionSwap() throws Exception {
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    String storeName1 = "testStore";
    String storeName2 = "testStore2";
    String storeName3 = "testStore3";
    String storeName4 = "testStore4";
    Store store1 = mockStore(2, 60, region1, versions, storeName1);
    Store store2 = mockStore(2, 60, region1, versions, storeName2);
    Store store3 = mockStore(2, 60, region1, versions, storeName3);
    Store store4 = mockStore(1, 60, region1, versions, storeName4);

    Time time = new SystemTime();
    List<Store> storeList = new ArrayList<>();
    storeList.add(store1);
    storeList.add(store2);
    storeList.add(store3);
    storeList.add(store4);
    doReturn(storeList).when(admin).getAllStores(clusterName);

    Version davinciVersion = new VersionImpl(storeName2, 2);
    Version normalVersion = new VersionImpl(storeName1, 2);
    davinciVersion.setIsDavinciHeartbeatReported(true);
    doReturn(davinciVersion).when(store2).getVersion(2);
    doReturn(normalVersion).when(store1).getVersion(2);
    doReturn(normalVersion).when(store3).getVersion(2);
    doReturn(normalVersion).when(store4).getVersion(1);

    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, 2);
    coloToVersions.put(region2, 1);
    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName1);
    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName2);
    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName3);
    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName4);

    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithWaitTimeElapsed = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time.getMilliseconds() - TimeUnit.MINUTES.toMillis(90),
        time.getMilliseconds() - TimeUnit.MINUTES.toMillis(30));
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithoutWaitTimeElapsed = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time.getMilliseconds() - TimeUnit.MINUTES.toMillis(30),
        time.getMilliseconds() - TimeUnit.MINUTES.toMillis(30));
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithOngoingPush = getOfflinePushStatusInfo(
        ExecutionStatus.STARTED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time.getMilliseconds() - TimeUnit.MINUTES.toMillis(30),
        time.getMilliseconds() - TimeUnit.MINUTES.toMillis(30));

    String kafkaTopicName1 = Version.composeKafkaTopic(storeName1, 2);
    String kafkaTopicName2 = Version.composeKafkaTopic(storeName2, 2);
    String kafkaTopicName3 = Version.composeKafkaTopic(storeName3, 2);
    String kafkaTopicName4 = Version.composeKafkaTopic(storeName4, 1);
    doReturn(offlinePushStatusInfoWithWaitTimeElapsed).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName1);
    doReturn(offlinePushStatusInfoWithWaitTimeElapsed).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName2);
    doReturn(offlinePushStatusInfoWithoutWaitTimeElapsed).when(admin)
        .getOffLinePushStatus(clusterName, kafkaTopicName3);
    doReturn(offlinePushStatusInfoWithOngoingPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName4);

    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, time);

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // push completed in target region & wait time elapsed
      verify(admin, atLeast(1)).rollForwardToFutureVersion(clusterName, storeName1, region2);

      // davinci store
      verify(admin, never()).rollForwardToFutureVersion(clusterName, storeName2, region2);

      // push completed in target region & wait time NOT elapsed
      verify(admin, never()).rollForwardToFutureVersion(clusterName, storeName3, region2);

      // push not completed in target region
      verify(admin, never()).rollForwardToFutureVersion(clusterName, storeName4, region2);
    });
  }

  @Test
  public void testTargetRegionSwapNotEnabled() {
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(1, VersionStatus.ONLINE);
    versions.put(2, VersionStatus.ONLINE);
    String storeName = "testStore";
    Store store = mockStore(2, 60, null, versions, storeName);

    Set<String> clusters = new HashSet<>();
    clusters.add(clusterName);
    doReturn(clusters).when(veniceControllerMultiClusterConfig).getClusters();
    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(storeName);

    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> verify(admin, never()).rollForwardToFutureVersion(any(), any(), any()));
  }
}
