package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.stats.DeferredVersionSwapStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
      doReturn(v).when(store).getVersion(n);
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
    int targetVersionNum = 3;
    int davinciVersionNum = 2;
    int completedVersionNum = 1;
    versions.put(completedVersionNum, VersionStatus.ONLINE);
    versions.put(davinciVersionNum, VersionStatus.PUSHED);
    versions.put(targetVersionNum, VersionStatus.PUSHED);
    String storeName1 = "testStore";
    String storeName2 = "testStore2";
    String storeName3 = "testStore3";
    String storeName4 = "testStore4";
    String storeName5 = "testStore5";
    Store store1 = mockStore(davinciVersionNum, 60, region1, versions, storeName1);
    Store store2 = mockStore(completedVersionNum, 60, region1, versions, storeName2);
    Store store3 = mockStore(davinciVersionNum, 60, region1, versions, storeName3);
    Store store4 = mockStore(davinciVersionNum, 60, region1, versions, storeName4);
    Store store5 = mockStore(completedVersionNum, 60, region1, versions, storeName5);

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    List<Store> storeList = new ArrayList<>();
    storeList.add(store1);
    storeList.add(store2);
    storeList.add(store3);
    storeList.add(store4);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(3).when(store1).getLargestUsedVersionNumber();
    doReturn(2).when(store2).getLargestUsedVersionNumber();
    doReturn(3).when(store3).getLargestUsedVersionNumber();
    doReturn(3).when(store4).getLargestUsedVersionNumber();
    doReturn(1).when(store5).getLargestUsedVersionNumber();

    Version davinciVersion = new VersionImpl(storeName2, davinciVersionNum);
    Version targetVersion = new VersionImpl(storeName1, targetVersionNum);
    davinciVersion.setIsDavinciHeartbeatReported(true);

    ControllerClient controllerClient = mock(ControllerClient.class);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    controllerClientMap.put(region1, controllerClient);
    StoreResponse storeResponse = new StoreResponse();
    StoreInfo storeInfo = new StoreInfo();
    List<Version> versionList = new ArrayList<>();
    versionList.add(targetVersion);
    versionList.add(davinciVersion);
    storeInfo.setVersions(versionList);
    storeResponse.setStore(storeInfo);

    doReturn(storeResponse).when(controllerClient).getStore(any());
    doReturn(veniceHelixAdmin).when(admin).getVeniceHelixAdmin();
    doReturn(controllerClientMap).when(veniceHelixAdmin).getControllerClientMap(clusterName);

    Map<String, Integer> coloToVersions = new HashMap<>();
    Map<String, Integer> davinciColoToVersions = new HashMap<>();
    coloToVersions.put(region1, 3);
    coloToVersions.put(region2, 2);
    davinciColoToVersions.put(region1, 2);
    davinciColoToVersions.put(region2, 1);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName1);
    doReturn(davinciColoToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName2);
    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName3);
    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName4);

    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithWaitTimeElapsed = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30));
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithoutWaitTimeElapsed = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithOngoingPush = getOfflinePushStatusInfo(
        ExecutionStatus.STARTED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName1 = Version.composeKafkaTopic(storeName1, targetVersionNum);
    String kafkaTopicName2 = Version.composeKafkaTopic(storeName2, davinciVersionNum);
    String kafkaTopicName3 = Version.composeKafkaTopic(storeName3, targetVersionNum);
    String kafkaTopicName4 = Version.composeKafkaTopic(storeName4, targetVersionNum);
    doReturn(offlinePushStatusInfoWithWaitTimeElapsed).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName1);
    doReturn(offlinePushStatusInfoWithWaitTimeElapsed).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName2);
    doReturn(offlinePushStatusInfoWithoutWaitTimeElapsed).when(admin)
        .getOffLinePushStatus(clusterName, kafkaTopicName3);
    doReturn(offlinePushStatusInfoWithOngoingPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName4);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, mock(DeferredVersionSwapStats.class));

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

      // push is complete in all regions
      verify(admin, never()).rollForwardToFutureVersion(clusterName, storeName5, region2);
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
