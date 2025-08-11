package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.stats.DeferredVersionSwapStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.LifecycleHooksRecordImpl;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDeferredVersionSwapService {
  private VeniceParentHelixAdmin admin;
  private VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private static final String clusterName = "testCluster";

  private static final String region1 = "test1";
  private static final String region2 = "test2";
  private static final String region3 = "test3";
  private static final int versionOne = 1;
  private static final int versionTwo = 2;

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

    List clustersList = Arrays.asList(clusterName);
    doReturn(clustersList).when(admin).getClustersLeaderOf();
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

  private StoreResponse getStoreResponse(List<Version> versions) {
    StoreResponse storeResponse = new StoreResponse();
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setVersions(versions);
    storeResponse.setStore(storeInfo);

    return storeResponse;
  }

  private Map<String, ControllerClient> mockControllerClients(List<Version> versions) {
    ControllerClient controllerClient1 = mock(ControllerClient.class);
    ControllerClient controllerClient2 = mock(ControllerClient.class);
    ControllerClient controllerClient3 = mock(ControllerClient.class);

    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    controllerClientMap.put(region1, controllerClient1);
    controllerClientMap.put(region2, controllerClient2);
    controllerClientMap.put(region3, controllerClient3);

    return controllerClientMap;
  }

  private void mockVeniceHelixAdmin(Map<String, ControllerClient> controllerClientMap) {
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doReturn(veniceHelixAdmin).when(admin).getVeniceHelixAdmin();
    HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    doReturn(repository).when(resources).getStoreMetadataRepository();
    doReturn(resources).when(veniceHelixAdmin).getHelixVeniceClusterResources(clusterName);

    doReturn(controllerClientMap).when(veniceHelixAdmin).getControllerClientMap(clusterName);
  }

  @Test
  public void testDeferredVersionSwap() throws Exception {
    String storeName = "testStore";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.PUSHED);
    Store store = mockStore(versionOne, 60, region1, versions, storeName);
    doReturn(versionTwo).when(store).getLargestUsedVersionNumber();

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.PUSHED);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(any());
    }

    mockVeniceHelixAdmin(controllerClientMap);
    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, versionTwo);
    coloToVersions.put(region2, versionOne);
    coloToVersions.put(region3, versionOne);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithCompletedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithCompletedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, mock(DeferredVersionSwapStats.class));

    List<LifecycleHooksRecord> lifecycleHooks = new ArrayList<>();
    Map<String, String> params = new HashMap<>();
    params.put("outcome", StoreVersionLifecycleEventOutcome.PROCEED.toString());
    lifecycleHooks.add(new LifecycleHooksRecordImpl(MockStoreLifecycleHooks.class.getName(), params));
    doReturn(lifecycleHooks).when(store).getStoreLifecycleHooks();
    Assert.assertEquals(store.getStoreLifecycleHooks().size(), 1);

    Properties properties = new Properties();
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceControllerClusterConfig veniceControllerClusterConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(veniceProperties).when(veniceControllerClusterConfig).getProps();
    doReturn(veniceControllerClusterConfig).when(veniceControllerMultiClusterConfig).getCommonConfig();

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(admin, atLeast(1)).rollForwardToFutureVersion(clusterName, storeName, region2 + "," + region3);
      verify(store, atLeast(1)).updateVersionStatus(versionTwo, VersionStatus.ONLINE);
    });
  }

  @Test
  public void testDeferredWithFailedTargetRegion() throws Exception {
    String storeName = "testStore";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.KILLED);
    Store store = mockStore(versionOne, 60, region1, versions, storeName);
    doReturn(versionTwo).when(store).getLargestUsedVersionNumber();

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.PUSHED);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(any());
    }

    mockVeniceHelixAdmin(controllerClientMap);
    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, versionTwo);
    coloToVersions.put(region2, versionOne);
    coloToVersions.put(region3, versionOne);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithCompletedPush = getOfflinePushStatusInfo(
        ExecutionStatus.ERROR.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithCompletedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, mock(DeferredVersionSwapStats.class));

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(store, atLeast(1)).updateVersionStatus(versionTwo, VersionStatus.ERROR);
    });
  }

  @Test
  public void testDeferredWithFailedNonTargetRegion() throws Exception {
    String storeName = "testStore";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.KILLED);
    Store store = mockStore(versionOne, 60, region1, versions, storeName);
    doReturn(versionTwo).when(store).getLargestUsedVersionNumber();

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.PUSHED);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);

    Version failedVersionTwoImpl = new VersionImpl(storeName, versionTwo);
    failedVersionTwoImpl.setStatus(VersionStatus.KILLED);
    List<Version> failedVersionList = new ArrayList<>();
    failedVersionList.add(versionOneImpl);
    failedVersionList.add(failedVersionTwoImpl);
    StoreResponse failedStoreResponse = getStoreResponse(failedVersionList);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);

    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      if (entry.getKey().equals(region3)) {
        doReturn(failedStoreResponse).when(controllerClient).getStore(any());
      } else {
        doReturn(storeResponse).when(controllerClient).getStore(any());
      }
    }

    mockVeniceHelixAdmin(controllerClientMap);
    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, versionTwo);
    coloToVersions.put(region2, versionOne);
    coloToVersions.put(region3, versionOne);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithCompletedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithCompletedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, mock(DeferredVersionSwapStats.class));

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(admin, atLeast(1)).rollForwardToFutureVersion(clusterName, storeName, region2);
      verify(store, atLeast(1)).updateVersionStatus(versionTwo, VersionStatus.PARTIALLY_ONLINE);
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

  @Test
  public void testDeferredVersionSwapCache() throws Exception {
    String storeName = "testStore";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.PUSHED);
    Store store = mockStore(versionOne, 60, region1, versions, storeName);
    doReturn(versionTwo).when(store).getLargestUsedVersionNumber();

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.PUSHED);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(any());
    }

    mockVeniceHelixAdmin(controllerClientMap);
    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, 2);
    coloToVersions.put(region2, 1);
    coloToVersions.put(region3, 1);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);

    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithOneOngoingPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName1 = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithOneOngoingPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName1);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, mock(DeferredVersionSwapStats.class));

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // one target region, 2 non target regions: all succeeded, wait time not elapsed -> do not swap
      verify(admin, never()).rollForwardToFutureVersion(clusterName, storeName, String.join(",\\s*", region2, region3));

      // verify that the wait time is cached
      Assert.assertEquals(
          deferredVersionSwapService.getStorePushCompletionTimes().getIfPresent(kafkaTopicName1),
          offlinePushStatusInfoWithOneOngoingPush.getExtraInfoUpdateTimestamp());
    });
  }

  @Test
  public void testDeferredVersionSwapWithStalledSwap() throws Exception {
    String storeName = "testStore";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.PUSHED);
    Store store = mockStore(versionOne, 1, region1, versions, storeName);
    doReturn(versionTwo).when(store).getLargestUsedVersionNumber();

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.PUSHED);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(any());
    }

    mockVeniceHelixAdmin(controllerClientMap);
    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, versionTwo);
    coloToVersions.put(region2, versionOne);
    coloToVersions.put(region3, versionOne);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithCompletedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithCompletedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    DeferredVersionSwapStats mockDeferredVersionSwapStats = mock(DeferredVersionSwapStats.class);
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, mockDeferredVersionSwapStats);

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(mockDeferredVersionSwapStats, atLeast(1)).recordDeferredVersionSwapStalledVersionSwapSensor(1.0);
    });
  }

  @Test
  public void testDeferredVersionSwapWithRunawayVPJ() throws Exception {
    String storeName = "testStore";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.STARTED);
    Store store = mockStore(versionOne, 1, region1, versions, storeName);
    doReturn(versionTwo).when(store).getLargestUsedVersionNumber();

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.ONLINE);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);
    storeResponse.getStore().setCurrentVersion(2);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(storeName);
    }

    mockVeniceHelixAdmin(controllerClientMap);
    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, versionTwo);
    coloToVersions.put(region2, versionOne);
    coloToVersions.put(region3, versionOne);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithCompletedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithCompletedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    DeferredVersionSwapStats deferredVersionSwapStats = mock(DeferredVersionSwapStats.class);
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, deferredVersionSwapStats);

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(deferredVersionSwapStats, atLeast(1)).recordDeferredVersionSwapParentChildStatusMismatchSensor();
    });
  }

  @Test
  public void testDeferredVersionSwapFailedRollForward() throws Exception {
    String storeName = "testStore";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.PUSHED);
    Store store = mockStore(versionOne, 60, region1, versions, storeName);
    doReturn(versionTwo).when(store).getLargestUsedVersionNumber();

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.PUSHED);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(any());
    }

    mockVeniceHelixAdmin(controllerClientMap);
    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, versionTwo);
    coloToVersions.put(region2, versionOne);
    coloToVersions.put(region3, versionOne);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithCompletedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithCompletedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, mock(DeferredVersionSwapStats.class));

    VeniceException exception = new VeniceException();
    doThrow(exception).when(admin).rollForwardToFutureVersion(any(), any(), any());
    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(admin, atLeast(1)).rollForwardToFutureVersion(clusterName, storeName, region2 + "," + region3);
      verify(store, atLeast(1)).updateVersionStatus(versionTwo, VersionStatus.PARTIALLY_ONLINE);
    });
  }

  @Test
  public void testDeferredVersionSwapWithOnlineParent() throws Exception {
    String storeName = "testStore";
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.ONLINE);
    Store store = mockStore(versionOne, 1, region1, versions, storeName);
    doReturn(versionTwo).when(store).getLargestUsedVersionNumber();

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.PUSHED);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(storeName);
    }

    mockVeniceHelixAdmin(controllerClientMap);
    Map<String, Integer> coloToVersions = new HashMap<>();
    coloToVersions.put(region1, versionTwo);
    coloToVersions.put(region2, versionOne);
    coloToVersions.put(region3, versionOne);

    doReturn(coloToVersions).when(admin).getCurrentVersionsForMultiColos(any(), any());

    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithCompletedPush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        time - TimeUnit.MINUTES.toSeconds(30),
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithCompletedPush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    DeferredVersionSwapStats deferredVersionSwapStats = mock(DeferredVersionSwapStats.class);
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, deferredVersionSwapStats);

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(deferredVersionSwapStats, atLeast(1)).recordDeferredVersionSwapParentChildStatusMismatchSensor();
    });
  }
}
