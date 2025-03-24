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
import com.linkedin.venice.meta.ReadWriteStoreRepository;
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
  private static final String region3 = "test3";

  @BeforeMethod
  public void setUp() {
    admin = mock(VeniceParentHelixAdmin.class);
    veniceControllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);

    Set<String> clusters = new HashSet<>();
    clusters.add(clusterName);
    doReturn(clusters).when(veniceControllerMultiClusterConfig).getClusters();
    doReturn(10L).when(veniceControllerMultiClusterConfig).getDeferredVersionSwapSleepMs();
    doReturn(true).when(veniceControllerMultiClusterConfig).isDeferredVersionSwapServiceEnabled();
    doReturn(true).when(veniceControllerMultiClusterConfig).isSkipDeferredVersionSwapForDVCEnabled();
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
      doReturn(targetRegions).when(v).getTargetSwapRegion();
      doReturn(waitTime).when(v).getTargetSwapRegionWaitTime();
      versionList.add(v);
    });
    doReturn(versionList).when(store).getVersions();
    doReturn(name).when(store).getName();

    return store;
  }

  private Admin.OfflinePushStatusInfo getOfflinePushStatusInfo(
      String region1Status,
      String region2Status,
      String region3Status,
      Long region1Timestamp,
      Long region2Timestamp,
      Long region3Timestamp) {
    Admin.OfflinePushStatusInfo pushStatusInfo =
        getOfflinePushStatusInfo(region1Status, region2Status, region1Timestamp, region2Timestamp);
    pushStatusInfo.getExtraInfoUpdateTimestamp().put(region3, region3Timestamp);
    pushStatusInfo.getExtraInfo().put(region3, region3Status);
    return pushStatusInfo;
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
    String storeName6 = "testStore5";
    Store store1 = mockStore(davinciVersionNum, 60, region1, versions, storeName1);
    Store store2 = mockStore(completedVersionNum, 60, region1, versions, storeName2);
    Store store3 = mockStore(davinciVersionNum, 60, region1, versions, storeName3);
    Store store4 = mockStore(davinciVersionNum, 60, region1, versions, storeName4);
    Store store5 = mockStore(completedVersionNum, 60, region1, versions, storeName5);
    Store store6 = mockStore(davinciVersionNum, 60, region1, versions, storeName6);

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    List<Store> storeList = new ArrayList<>();
    storeList.add(store1);
    storeList.add(store2);
    storeList.add(store3);
    storeList.add(store4);
    storeList.add(store5);
    storeList.add(store6);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(3).when(store1).getLargestUsedVersionNumber();
    doReturn(2).when(store2).getLargestUsedVersionNumber();
    doReturn(3).when(store3).getLargestUsedVersionNumber();
    doReturn(3).when(store4).getLargestUsedVersionNumber();
    doReturn(1).when(store5).getLargestUsedVersionNumber();
    doReturn(3).when(store5).getLargestUsedVersionNumber();

    Version davinciVersion = new VersionImpl(storeName2, davinciVersionNum);
    Version targetVersion = new VersionImpl(storeName1, targetVersionNum);
    davinciVersion.setIsDavinciHeartbeatReported(true);

    ControllerClient controllerClient = mock(ControllerClient.class);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    controllerClientMap.put(region1, controllerClient);
    controllerClientMap.put(region2, controllerClient);
    StoreResponse storeResponse = new StoreResponse();
    StoreInfo storeInfo = new StoreInfo();
    List<Version> versionList = new ArrayList<>();
    versionList.add(targetVersion);
    versionList.add(davinciVersion);
    storeInfo.setVersions(versionList);
    storeResponse.setStore(storeInfo);

    doReturn(storeResponse).when(controllerClient).getStore(any());
    doReturn(veniceHelixAdmin).when(admin).getVeniceHelixAdmin();

    HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    doReturn(repository).when(resources).getStoreMetadataRepository();
    doReturn(resources).when(veniceHelixAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(store1).when(repository).getStore(storeName1);
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
    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName6);

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
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithFailedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.ERROR.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName1 = Version.composeKafkaTopic(storeName1, targetVersionNum);
    String kafkaTopicName2 = Version.composeKafkaTopic(storeName2, davinciVersionNum);
    String kafkaTopicName3 = Version.composeKafkaTopic(storeName3, targetVersionNum);
    String kafkaTopicName4 = Version.composeKafkaTopic(storeName4, targetVersionNum);
    String kafkaTopicName6 = Version.composeKafkaTopic(storeName6, targetVersionNum);
    doReturn(offlinePushStatusInfoWithWaitTimeElapsed).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName1);
    doReturn(offlinePushStatusInfoWithWaitTimeElapsed).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName2);
    doReturn(offlinePushStatusInfoWithoutWaitTimeElapsed).when(admin)
        .getOffLinePushStatus(clusterName, kafkaTopicName3);
    doReturn(offlinePushStatusInfoWithOngoingPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName4);
    doReturn(offlinePushStatusInfoWithFailedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName6);
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

      // push failed in non target region
      verify(admin, never()).rollForwardToFutureVersion(clusterName, storeName6, region2);
    });
  }

  @Test
  public void testDeferredVersionSwapNonTargetRegionStatuses() throws Exception {
    Map<Integer, VersionStatus> versions = new HashMap<>();
    int targetVersionNum = 3;
    int davinciVersionNum = 2;
    int completedVersionNum = 1;
    versions.put(completedVersionNum, VersionStatus.ONLINE);
    versions.put(davinciVersionNum, VersionStatus.PUSHED);
    versions.put(targetVersionNum, VersionStatus.PUSHED);
    Map<Integer, VersionStatus> killedVersionsList = new HashMap<>();
    killedVersionsList.put(davinciVersionNum, VersionStatus.PUSHED);
    killedVersionsList.put(targetVersionNum, VersionStatus.KILLED);
    String storeName1 = "testStore";
    String storeName2 = "testStore2";
    String storeName3 = "testStore3";
    String storeName4 = "testStore4";
    String storeName5 = "testStore5";
    String storeName6 = "testStore6";
    Store store1 = mockStore(davinciVersionNum, 60, region1, versions, storeName1);
    Store store2 = mockStore(davinciVersionNum, 60, region1, versions, storeName2);
    Store store3 = mockStore(davinciVersionNum, 60, region1, killedVersionsList, storeName3);
    Store store4 = mockStore(davinciVersionNum, 60, region1, killedVersionsList, storeName4);
    Store store5 = mockStore(davinciVersionNum, 60, region1, killedVersionsList, storeName5);
    Store store6 = mockStore(davinciVersionNum, 60, region1 + "," + region2, killedVersionsList, storeName6);

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    List<Store> storeList = new ArrayList<>();
    storeList.add(store1);
    storeList.add(store2);
    storeList.add(store3);
    storeList.add(store4);
    storeList.add(store5);
    storeList.add(store6);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(3).when(store1).getLargestUsedVersionNumber();
    doReturn(3).when(store2).getLargestUsedVersionNumber();
    doReturn(3).when(store3).getLargestUsedVersionNumber();
    doReturn(3).when(store4).getLargestUsedVersionNumber();
    doReturn(3).when(store5).getLargestUsedVersionNumber();
    doReturn(3).when(store6).getLargestUsedVersionNumber();

    Version targetVersion = new VersionImpl(storeName1, targetVersionNum);

    ControllerClient controllerClient = mock(ControllerClient.class);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    controllerClientMap.put(region1, controllerClient);
    controllerClientMap.put(region2, controllerClient);
    controllerClientMap.put(region3, controllerClient);
    StoreResponse storeResponse = new StoreResponse();
    StoreInfo storeInfo = new StoreInfo();
    List<Version> versionList = new ArrayList<>();
    versionList.add(targetVersion);
    storeInfo.setVersions(versionList);
    storeResponse.setStore(storeInfo);

    doReturn(storeResponse).when(controllerClient).getStore(any());
    doReturn(veniceHelixAdmin).when(admin).getVeniceHelixAdmin();

    HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    doReturn(repository).when(resources).getStoreMetadataRepository();
    doReturn(resources).when(veniceHelixAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(store1).when(repository).getStore(storeName1);
    doReturn(controllerClientMap).when(veniceHelixAdmin).getControllerClientMap(clusterName);

    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, 3);
    coloToVersions.put(region2, 2);
    coloToVersions.put(region3, 2);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithOneOngoingPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.STARTED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithMultipleOngoingPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.STARTED.toString(),
        ExecutionStatus.STARTED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithOneFailedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.ERROR.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithTwoFailedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.ERROR.toString(),
        ExecutionStatus.ERROR.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithOngoingFailedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.STARTED.toString(),
        ExecutionStatus.ERROR.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    Admin.OfflinePushStatusInfo offlinePushStatusInfoCompletedFailedTargetRegions = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.ERROR.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName1 = Version.composeKafkaTopic(storeName1, targetVersionNum);
    String kafkaTopicName2 = Version.composeKafkaTopic(storeName2, targetVersionNum);
    String kafkaTopicName3 = Version.composeKafkaTopic(storeName3, targetVersionNum);
    String kafkaTopicName4 = Version.composeKafkaTopic(storeName4, targetVersionNum);
    String kafkaTopicName5 = Version.composeKafkaTopic(storeName5, targetVersionNum);
    String kafkaTopicName6 = Version.composeKafkaTopic(storeName6, targetVersionNum);
    doReturn(offlinePushStatusInfoWithOneOngoingPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName1);
    doReturn(offlinePushStatusInfoWithMultipleOngoingPush).when(admin)
        .getOffLinePushStatus(clusterName, kafkaTopicName2);
    doReturn(offlinePushStatusInfoWithOneFailedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName3);
    doReturn(offlinePushStatusInfoWithTwoFailedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName4);
    doReturn(offlinePushStatusInfoWithOngoingFailedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName5);
    doReturn(offlinePushStatusInfoCompletedFailedTargetRegions).when(admin)
        .getOffLinePushStatus(clusterName, kafkaTopicName6);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, mock(DeferredVersionSwapStats.class));

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // one target region, 2 non target regions: one succeeded, one still in progress -> do not swap
      verify(admin, never())
          .rollForwardToFutureVersion(clusterName, storeName1, String.join(",\\s*", region2, region3));

      // one target region, 2 non target regions: both still in progress -> do not swap
      verify(admin, never())
          .rollForwardToFutureVersion(clusterName, storeName2, String.join(",\\s*", region2, region3));

      // one target region, 2 non target regions: one succeeded, one failed -> swap
      verify(admin, atLeast(1)).rollForwardToFutureVersion(clusterName, storeName3, region2);
      verify(store3, atLeast(1)).updateVersionStatus(3, VersionStatus.PARTIALLY_ONLINE);

      // one target region, 2 non target regions: both failed -> do not swap
      verify(store4, atLeast(1)).updateVersionStatus(3, VersionStatus.PARTIALLY_ONLINE);

      // one target region, 2 non target regions: one failed, one in progress -> do not swap
      verify(admin, never())
          .rollForwardToFutureVersion(clusterName, storeName5, String.join(",\\s*", region2, region3));

      // two target regions: 1 completed, 1 failed, 1 completed non target region -> swap
      verify(admin, atLeast(1)).rollForwardToFutureVersion(clusterName, storeName6, region3);
      verify(store6, atLeast(1)).updateVersionStatus(3, VersionStatus.PARTIALLY_ONLINE);
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
