package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.stats.DeferredVersionSwapStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.ConcurrentPushDetectionStrategy;
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
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
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
  private static Map<String, String> childDatacenterToUrl = new HashMap<>();
  private ReadWriteStoreRepository repository;
  private static final int controllerTimeout = 1 * Time.MS_PER_SECOND;
  private MetricsRepository metricsRepository;

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

    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    doReturn("").when(clusterConfig).getDeferredVersionSwapRegionRollforwardOrder();
    doReturn(clusterConfig).when(veniceControllerMultiClusterConfig).getControllerConfig(clusterName);
    doReturn(1).when(clusterConfig).getDeferredVersionSwapThreadPoolSize();
    doReturn(ConcurrentPushDetectionStrategy.PARENT_VERSION_STATUS_ONLY).when(clusterConfig)
        .getConcurrentPushDetectionStrategy();

    childDatacenterToUrl.put(region1, "test");
    childDatacenterToUrl.put(region2, "test");
    childDatacenterToUrl.put(region3, "test");
    doReturn(childDatacenterToUrl).when(admin).getChildDataCenterControllerUrlMap(anyString());

    metricsRepository = mock(MetricsRepository.class);
    Sensor sensor = mock(Sensor.class);
    doReturn(sensor).when(metricsRepository).sensor(any(), any());
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
      doReturn(true).when(v).isVersionSwapDeferred();
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
    repository = mock(ReadWriteStoreRepository.class);
    doReturn(repository).when(resources).getStoreMetadataRepository();
    doReturn(resources).when(veniceHelixAdmin).getHelixVeniceClusterResources(clusterName);

    ClusterLockManager clusterLockManager = mock(ClusterLockManager.class);
    doReturn(null).when(clusterLockManager).createStoreWriteLock(clusterName);
    doReturn(clusterLockManager).when(resources).getClusterLockManager();

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
      doReturn(storeResponse).when(controllerClient).getStore(any(), anyInt());
      JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
      doReturn(false).when(response).isError();
      doReturn(ExecutionStatus.COMPLETED.toString()).when(response).getStatus();
      doReturn(response).when(controllerClient).queryJobStatus(any(), any(), anyInt(), any(), anyBoolean());
    }

    mockVeniceHelixAdmin(controllerClientMap);
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
    doReturn(store).when(repository).getStore(storeName);

    DeferredVersionSwapService deferredVersionSwapService = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        mock(DeferredVersionSwapStats.class),
        metricsRepository);

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
      verify(admin, never()).truncateKafkaTopic(anyString());
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
      doReturn(storeResponse).when(controllerClient).getStore(any(), anyInt());
    }

    mockVeniceHelixAdmin(controllerClientMap);
    doReturn(store).when(repository).getStore(storeName);

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

    DeferredVersionSwapService deferredVersionSwapService = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        mock(DeferredVersionSwapStats.class),
        metricsRepository);

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
      doReturn(storeResponse).when(controllerClient).getStore(any(), anyInt());
    }

    mockVeniceHelixAdmin(controllerClientMap);
    doReturn(store).when(repository).getStore(storeName);

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
    DeferredVersionSwapService deferredVersionSwapService = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        mockDeferredVersionSwapStats,
        metricsRepository);

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(mockDeferredVersionSwapStats, atLeast(1))
          .recordDeferredVersionSwapStalledVersionSwapMetric(eq(1.0), anyString());
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
      doReturn(storeResponse).when(controllerClient).getStore(storeName, controllerTimeout);
    }

    mockVeniceHelixAdmin(controllerClientMap);
    doReturn(store).when(repository).getStore(storeName);

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
    DeferredVersionSwapService deferredVersionSwapService = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        deferredVersionSwapStats,
        metricsRepository);

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(deferredVersionSwapStats, atLeast(1))
          .recordDeferredVersionSwapParentChildStatusMismatchMetric(anyString(), anyString());
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
      doReturn(storeResponse).when(controllerClient).getStore(any(), anyInt());
      JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
      doReturn(false).when(response).isError();
      doReturn(ExecutionStatus.COMPLETED.toString()).when(response).getStatus();
      doReturn(response).when(controllerClient).queryJobStatus(any(), any(), anyInt(), any(), anyBoolean());
    }

    mockVeniceHelixAdmin(controllerClientMap);
    doReturn(store).when(repository).getStore(storeName);

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

    DeferredVersionSwapService deferredVersionSwapService = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        mock(DeferredVersionSwapStats.class),
        metricsRepository);

    VeniceException exception = new VeniceException();
    doThrow(exception).when(admin).rollForwardToFutureVersion(any(), any(), any());
    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(admin, atLeast(1)).rollForwardToFutureVersion(clusterName, storeName, region2 + "," + region3);
      verify(store, atLeast(1)).updateVersionStatus(versionTwo, VersionStatus.PARTIALLY_ONLINE);
      verify(admin, never()).truncateKafkaTopic(anyString());
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
      doReturn(storeResponse).when(controllerClient).getStore(storeName, controllerTimeout);
    }

    mockVeniceHelixAdmin(controllerClientMap);
    doReturn(store).when(repository).getStore(storeName);

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
    DeferredVersionSwapService deferredVersionSwapService = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        deferredVersionSwapStats,
        metricsRepository);

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(deferredVersionSwapStats, atLeast(1))
          .recordDeferredVersionSwapParentChildStatusMismatchMetric(anyString(), anyString());
    });
  }

  @Test
  public void testDeferredVersionSwapWithOnlineChild() throws Exception {
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
    versionTwoImpl.setStatus(VersionStatus.ONLINE);
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setCurrentVersion(versionTwoImpl.getNumber());
    storeInfo.setVersions(versionList);
    StoreResponse storeResponse = new StoreResponse();
    storeResponse.setStore(storeInfo);

    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(storeName, controllerTimeout);
    }

    mockVeniceHelixAdmin(controllerClientMap);
    doReturn(store).when(repository).getStore(storeName);

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
    DeferredVersionSwapService deferredVersionSwapService = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        deferredVersionSwapStats,
        metricsRepository);

    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(store, atLeast(1)).updateVersionStatus(2, VersionStatus.ONLINE);
      verify(admin, never()).truncateKafkaTopic(anyString());
    });
  }

  @Test
  public void testParallelRolloutWithMultipleStores() throws Exception {
    String storeName1 = "testStore1";
    String storeName2 = "testStore2";
    String storeName3 = "testStore3";

    // Create multiple stores for parallel processing
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.PUSHED);

    Store store1 = mockStore(versionOne, 60, region1, versions, storeName1);
    Store store2 = mockStore(versionOne, 60, region1, versions, storeName2);
    Store store3 = mockStore(versionOne, 60, region1, versions, storeName3);

    doReturn(versionTwo).when(store1).getLargestUsedVersionNumber();
    doReturn(versionTwo).when(store2).getLargestUsedVersionNumber();
    doReturn(versionTwo).when(store3).getLargestUsedVersionNumber();

    List<Store> storeList = Arrays.asList(store1, store2, store3);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    // Setup thread pool configuration
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(3).when(clusterConfig).getDeferredVersionSwapThreadPoolSize(); // 3 threads for 3 stores
    doReturn("").when(clusterConfig).getDeferredVersionSwapRegionRollforwardOrder(); // Parallel rollout
    doReturn(clusterConfig).when(veniceControllerMultiClusterConfig).getControllerConfig(clusterName);

    // Mock controller clients and offline push status
    List<Version> versionList = new ArrayList<>();
    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    mockVeniceHelixAdmin(controllerClientMap);

    // Mock the actual processing methods to track concurrency
    doReturn(
        getOfflinePushStatusInfo(
            ExecutionStatus.COMPLETED.toString(),
            ExecutionStatus.COMPLETED.toString(),
            System.currentTimeMillis() / 1000 - 3600,
            System.currentTimeMillis() / 1000 - 3600)).when(admin).getOffLinePushStatus(anyString(), anyString());

    DeferredVersionSwapStats deferredVersionSwapStats = mock(DeferredVersionSwapStats.class);
    DeferredVersionSwapService service = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        deferredVersionSwapStats,
        metricsRepository);

    service.startInner();

    // Wait for processing to complete
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(admin, atLeast(3)).getOffLinePushStatus(anyString(), anyString());
    });

    service.stopInner();
  }

  @Test
  public void testSequentialRolloutWithMultipleStores() throws Exception {
    String storeName1 = "testStore1";
    String storeName2 = "testStore2";

    // Create stores for sequential processing
    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.PUSHED);

    Store store1 = mockStore(versionOne, 60, region1, versions, storeName1);
    Store store2 = mockStore(versionOne, 60, region1, versions, storeName2);

    doReturn(versionTwo).when(store1).getLargestUsedVersionNumber();
    doReturn(versionTwo).when(store2).getLargestUsedVersionNumber();

    List<Store> storeList = Arrays.asList(store1, store2);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    // Setup thread pool configuration for sequential rollout
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(2).when(clusterConfig).getDeferredVersionSwapThreadPoolSize(); // 2 threads
    doReturn(region1 + "," + region2).when(clusterConfig).getDeferredVersionSwapRegionRollforwardOrder(); // Sequential
                                                                                                          // rollout
    doReturn(clusterConfig).when(veniceControllerMultiClusterConfig).getControllerConfig(clusterName);

    // Mock controller clients
    List<Version> versionList = new ArrayList<>();
    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    mockVeniceHelixAdmin(controllerClientMap);

    // Mock version information for sequential processing
    doReturn(
        getOfflinePushStatusInfo(
            ExecutionStatus.COMPLETED.toString(),
            ExecutionStatus.COMPLETED.toString(),
            System.currentTimeMillis() / 1000 - 3600,
            System.currentTimeMillis() / 1000 - 3600)).when(admin).getOffLinePushStatus(anyString(), anyString());

    DeferredVersionSwapStats deferredVersionSwapStats = mock(DeferredVersionSwapStats.class);
    DeferredVersionSwapService service = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        deferredVersionSwapStats,
        metricsRepository);

    service.startInner();

    // Wait for processing to complete
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(admin, atLeast(2)).getOffLinePushStatus(anyString(), anyString());
    });

    service.stopInner();
  }

  @Test
  public void testConcurrentStoreProcessingWithinCluster() throws Exception {
    // Test that multiple stores within same cluster can be processed concurrently
    String storeName1 = "concurrentStore1";
    String storeName2 = "concurrentStore2";
    String storeName3 = "concurrentStore3";

    Map<Integer, VersionStatus> versions = new HashMap<>();
    versions.put(versionOne, VersionStatus.ONLINE);
    versions.put(versionTwo, VersionStatus.PUSHED);

    Store store1 = mockStore(versionOne, 60, region1, versions, storeName1);
    Store store2 = mockStore(versionOne, 60, region1, versions, storeName2);
    Store store3 = mockStore(versionOne, 60, region1, versions, storeName3);

    doReturn(versionTwo).when(store1).getLargestUsedVersionNumber();
    doReturn(versionTwo).when(store2).getLargestUsedVersionNumber();
    doReturn(versionTwo).when(store3).getLargestUsedVersionNumber();

    List<Store> storeList = Arrays.asList(store1, store2, store3);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    // Setup configuration with sufficient threads
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(3).when(clusterConfig).getDeferredVersionSwapThreadPoolSize(); // 3 threads for concurrent processing
    doReturn("").when(clusterConfig).getDeferredVersionSwapRegionRollforwardOrder(); // Parallel rollout
    doReturn(clusterConfig).when(veniceControllerMultiClusterConfig).getControllerConfig(clusterName);

    List<Version> versionList = new ArrayList<>();
    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    mockVeniceHelixAdmin(controllerClientMap);

    // Mock successful processing
    doReturn(
        getOfflinePushStatusInfo(
            ExecutionStatus.COMPLETED.toString(),
            ExecutionStatus.COMPLETED.toString(),
            System.currentTimeMillis() / 1000 - 3600,
            System.currentTimeMillis() / 1000 - 3600)).when(admin).getOffLinePushStatus(anyString(), anyString());

    DeferredVersionSwapStats deferredVersionSwapStats = mock(DeferredVersionSwapStats.class);
    DeferredVersionSwapService service = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        deferredVersionSwapStats,
        metricsRepository);

    service.startInner();

    // Wait for processing and verify thread pool was used
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(admin, atLeast(3)).getOffLinePushStatus(anyString(), anyString());
    });

    service.stopInner();
  }
}
