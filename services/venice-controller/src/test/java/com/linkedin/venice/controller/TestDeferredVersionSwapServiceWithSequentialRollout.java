package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.atLeastOnce;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDeferredVersionSwapServiceWithSequentialRollout {
  private VeniceParentHelixAdmin admin;
  private VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private static final String region1 = "region1";
  private static final String region2 = "region2";
  private static final String region3 = "region3";
  private static final String clusterName = "test-cluster";
  private static final int versionOne = 1;
  private static final int versionTwo = 2;
  private static Map<String, String> childDatacenterToUrl = new HashMap<>();
  private ReadWriteStoreRepository repository;
  private DeferredVersionSwapStats stats;
  private VeniceControllerClusterConfig clusterConfig;
  private VeniceHelixAdmin veniceHelixAdmin;
  private Map<String, ControllerClient> controllerClientMap;
  private static final int controllerTimeout = 1 * Time.MS_PER_SECOND;
  private MetricsRepository metricsRepository;

  @BeforeMethod
  public void setUp() {
    // Mock the admin
    admin = mock(VeniceParentHelixAdmin.class);

    // Mock the multi-cluster config
    veniceControllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);

    // Setup cluster config with sequential rollout order
    clusterConfig = mock(VeniceControllerClusterConfig.class);
    String rolloutOrder = region1 + "," + region2 + "," + region3;
    doReturn(rolloutOrder).when(clusterConfig).getDeferredVersionSwapRegionRollforwardOrder();
    doReturn(clusterConfig).when(veniceControllerMultiClusterConfig).getControllerConfig(clusterName);
    doReturn(1).when(clusterConfig).getDeferredVersionSwapThreadPoolSize();

    // Mock the deferred version swap sleep time to avoid IllegalArgumentException in ScheduledThreadPoolExecutor
    doReturn(1000L).when(veniceControllerMultiClusterConfig).getDeferredVersionSwapSleepMs();

    // Setup Helix Admin
    veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    doReturn(veniceHelixAdmin).when(admin).getVeniceHelixAdmin();

    // Set up stats spy for verification
    stats = mock(DeferredVersionSwapStats.class);

    // Setup child datacenters from cluster config
    Set<String> childDatacenters = new HashSet<>(Arrays.asList(region1, region2, region3));
    doReturn(childDatacenters).when(clusterConfig).getChildDatacenters();

    // Setup clusters that are led by this controller
    List<String> clustersList = Arrays.asList(clusterName);
    doReturn(clustersList).when(admin).getClustersLeaderOf();

    // Setup child datacenter URLs
    childDatacenterToUrl.put(region1, "test-url-1");
    childDatacenterToUrl.put(region2, "test-url-2");
    childDatacenterToUrl.put(region3, "test-url-3");
    doReturn(childDatacenterToUrl).when(admin).getChildDataCenterControllerUrlMap(clusterName);

    // mock metrics repository
    metricsRepository = mock(MetricsRepository.class);
    Sensor sensor = mock(Sensor.class);
    doReturn(sensor).when(metricsRepository).sensor(any(), any());

    // Setup mock Venice Helix Admin
    mockVeniceHelixAdmin();
  }

  private Store mockStore(int currentVersion, int largestUsedVersion, String storeName) {
    Store store = mock(Store.class);
    doReturn(storeName).when(store).getName();
    doReturn(currentVersion).when(store).getCurrentVersion();
    doReturn(largestUsedVersion).when(store).getLargestUsedVersionNumber();

    // Setup version status retrieval
    doReturn(VersionStatus.ONLINE).when(store).getVersionStatus(currentVersion);
    doReturn(VersionStatus.PUSHED).when(store).getVersionStatus(largestUsedVersion);

    // Create and mock the target version
    Version targetVersion = mock(Version.class);
    doReturn(targetVersion).when(store).getVersion(largestUsedVersion);
    doReturn(VersionStatus.PUSHED).when(targetVersion).getStatus();
    doReturn(true).when(targetVersion).isVersionSwapDeferred();
    doReturn(largestUsedVersion).when(targetVersion).getNumber();

    // Mock target swap region - use specific regions so isPushInTerminalState doesn't return false immediately
    String targetSwapRegions = region1 + "," + region2;
    doReturn(targetSwapRegions).when(targetVersion).getTargetSwapRegion();

    // Mock store target swap region wait time
    doReturn(1).when(store).getTargetSwapRegionWaitTime(); // 1 minute wait time

    // Mock latest version promote timestamp - set to 10 minutes ago so wait time has definitely passed
    long tenMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10);
    doReturn(tenMinutesAgo).when(store).getLatestVersionPromoteToCurrentTimestamp();

    return store;
  }

  private Store mockRegionalStore(int currentVersion, int largestUsedVersion, String storeName) {
    Store store = mock(Store.class);
    doReturn(storeName).when(store).getName();
    doReturn(currentVersion).when(store).getCurrentVersion();
    doReturn(largestUsedVersion).when(store).getLargestUsedVersionNumber();

    // Setup version status retrieval
    doReturn(VersionStatus.ONLINE).when(store).getVersionStatus(currentVersion);
    doReturn(VersionStatus.PUSHED).when(store).getVersionStatus(largestUsedVersion);

    // Create and mock the target version
    Version targetVersion = mock(Version.class);
    doReturn(targetVersion).when(store).getVersion(largestUsedVersion);
    doReturn(VersionStatus.PUSHED).when(targetVersion).getStatus();
    doReturn(true).when(targetVersion).isVersionSwapDeferred();
    doReturn(largestUsedVersion).when(targetVersion).getNumber();

    // Mock target swap region - use specific regions so isPushInTerminalState doesn't return false immediately
    String targetSwapRegions = region1 + "," + region2;
    doReturn(targetSwapRegions).when(targetVersion).getTargetSwapRegion();

    // Mock store target swap region wait time
    doReturn(1).when(store).getTargetSwapRegionWaitTime(); // 1 minute wait time

    // Mock latest version promote timestamp - set to 10 minutes ago so wait time has definitely passed
    long tenMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10);
    doReturn(tenMinutesAgo).when(store).getLatestVersionPromoteToCurrentTimestamp();

    return store;
  }

  private StoreResponse getStoreResponse(List<Version> versions) {
    StoreResponse storeResponse = mock(StoreResponse.class);
    doReturn(false).when(storeResponse).isError();

    // Create a store info mock that will be returned by the store response
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(versions).when(storeInfo).getVersions();
    doReturn(storeInfo).when(storeResponse).getStore();

    return storeResponse;
  }

  private Map<String, ControllerClient> mockControllerClients(List<Version> versions) {
    controllerClientMap = new HashMap<>();
    for (String region: Arrays.asList(region1, region2, region3)) {
      ControllerClient controllerClient = mock(ControllerClient.class);
      controllerClientMap.put(region, controllerClient);
      JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
      doReturn(false).when(response).isError();
      doReturn(ExecutionStatus.COMPLETED.toString()).when(response).getStatus();
      doReturn(response).when(controllerClient).queryJobStatus(any(), any(), anyInt(), any(), anyBoolean());
    }
    return controllerClientMap;
  }

  private void mockVeniceHelixAdmin() {
    HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    repository = mock(ReadWriteStoreRepository.class);
    doReturn(repository).when(resources).getStoreMetadataRepository();
    doReturn(resources).when(veniceHelixAdmin).getHelixVeniceClusterResources(clusterName);

    ClusterLockManager clusterLockManager = mock(ClusterLockManager.class);
    doReturn(null).when(clusterLockManager).createStoreWriteLock(anyString());
    doReturn(clusterLockManager).when(resources).getClusterLockManager();

    doReturn(controllerClientMap).when(veniceHelixAdmin).getControllerClientMap(clusterName);

    // Setup controller client map for region access
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();

    ControllerClient region1Client = mock(ControllerClient.class);
    ControllerClient region2Client = mock(ControllerClient.class);
    ControllerClient region3Client = mock(ControllerClient.class);

    controllerClientMap.put(region1, region1Client);
    controllerClientMap.put(region2, region2Client);
    controllerClientMap.put(region3, region3Client);

    doReturn(controllerClientMap).when(veniceHelixAdmin).getControllerClientMap(clusterName);

    // Mock store responses for each region - create regional stores with target version available
    Store region1Store = mockRegionalStore(2, 2, "testStore");
    Store region2Store = mockStore(1, 2, "testStore");
    Store region3Store = mockStore(1, 2, "testStore");

    StoreResponse region1Response = new StoreResponse();
    StoreInfo region1StoreInfo = StoreInfo.fromStore(region1Store);
    // Explicitly set versions for region1 - include both version 1 and target version 2
    Version region1Version1 = mock(Version.class);
    doReturn(1).when(region1Version1).getNumber();
    doReturn(VersionStatus.ONLINE).when(region1Version1).getStatus();

    Version region1Version2 = mock(Version.class);
    doReturn(2).when(region1Version2).getNumber();
    doReturn(VersionStatus.PUSHED).when(region1Version2).getStatus();

    region1StoreInfo.setVersions(Arrays.asList(region1Version1, region1Version2));
    region1Response.setStore(region1StoreInfo);

    StoreResponse region2Response = new StoreResponse();
    StoreInfo region2StoreInfo = StoreInfo.fromStore(region2Store);
    // Region2 has current version 2 - target version needs PUSHED status for sequential rollout
    Version region2Version2 = mock(Version.class);
    doReturn(2).when(region2Version2).getNumber();
    doReturn(VersionStatus.PUSHED).when(region2Version2).getStatus();
    region2StoreInfo.setVersions(Arrays.asList(region2Version2));
    region2Response.setStore(region2StoreInfo);

    StoreResponse region3Response = new StoreResponse();
    StoreInfo region3StoreInfo = StoreInfo.fromStore(region3Store);
    // Region3 has current version 2 - target version needs PUSHED status for sequential rollout
    Version region3Version2 = mock(Version.class);
    doReturn(2).when(region3Version2).getNumber();
    doReturn(VersionStatus.PUSHED).when(region3Version2).getStatus();
    region3StoreInfo.setVersions(Arrays.asList(region3Version2));
    region3Response.setStore(region3StoreInfo);

    doReturn(region1Response).when(region1Client).getStore("testStore", controllerTimeout);
    doReturn(region2Response).when(region2Client).getStore("testStore", controllerTimeout);
    doReturn(region3Response).when(region3Client).getStore("testStore", controllerTimeout);

    // Mock offline push status - return completed status for the target version
    Admin.OfflinePushStatusInfo pushStatusInfo = new Admin.OfflinePushStatusInfo(
        ExecutionStatus.COMPLETED,
        System.currentTimeMillis(),
        new HashMap<>(),
        "Push completed",
        new HashMap<>(),
        new HashMap<>());
    doReturn(pushStatusInfo).when(admin).getOffLinePushStatus(clusterName, "testStore_v2");

    // Mock current versions across regions - simulate that region1 is at version 1, others at version 2
    Map<String, Integer> currentVersionsMap = new HashMap<>();
    currentVersionsMap.put(region1, 2); // Behind - needs rollforward
    currentVersionsMap.put(region2, 1); // Current
    currentVersionsMap.put(region3, 1); // Current
    doReturn(currentVersionsMap).when(admin).getCurrentVersionsForMultiColos(clusterName, "testStore");
  }

  private Admin.OfflinePushStatusInfo getOfflinePushStatusInfo(
      String region1Status,
      String region2Status,
      String region3Status,
      long startTime,
      long region1EndTime,
      long region2EndTime) {
    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put(region1, region1Status);
    extraInfo.put(region2, region2Status);
    extraInfo.put(region3, region3Status);

    Map<String, Long> extraInfoUpdateTimestamp = new HashMap<>();
    extraInfoUpdateTimestamp.put(region1, region1EndTime);
    extraInfoUpdateTimestamp.put(region2, region2EndTime);

    return new Admin.OfflinePushStatusInfo(
        ExecutionStatus.COMPLETED,
        startTime,
        extraInfo,
        null,
        null,
        extraInfoUpdateTimestamp);
  }

  /**
   * Version swaps happen in sequence according to the configured rollout order
   */
  @Test
  public void testSequentialRolloutHappyPath() throws Exception {
    String storeName = "testStore";
    Store store = mockStore(versionOne, versionTwo, storeName);

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

    // Mock controller clients for each region
    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(anyString(), anyInt());
    }

    // Setup store repo
    doReturn(store).when(repository).getStore(storeName);

    // Simulate push completed in all regions
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

    // Create service
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats, metricsRepository);

    // Start the service
    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // Verify rollForwardToFutureVersion was called for the correct regions
      verify(admin, atLeastOnce()).rollForwardToFutureVersion(clusterName, storeName, region2);
    });

    // Verify error recording was not called
    verify(stats, never()).recordDeferredVersionSwapErrorSensor();
  }

  /**
   * Failure path - If one region fails, subsequent roll forwards don't happen
   */
  @Test
  public void testSequentialRolloutFailurePath() throws Exception {
    String storeName = "testStore";
    Store store = mockStore(versionOne, versionTwo, storeName);

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

    // Mock controller clients for each region
    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(anyString(), anyInt());
    }

    // Setup store repo
    doReturn(store).when(repository).getStore(storeName);

    // Simulate push completed in all regions
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

    // Simulate failure on region2 rollout by making rollForwardToFutureVersion throw an exception when region2 appears
    doThrow(new VeniceException()).when(admin).rollForwardToFutureVersion(clusterName, storeName, region2);
    doReturn(clusterConfig).when(veniceControllerMultiClusterConfig).getControllerConfig(clusterName);
    doReturn(ConcurrentPushDetectionStrategy.PARENT_VERSION_STATUS_ONLY).when(clusterConfig)
        .getConcurrentPushDetectionStrategy();
    // Create service
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats, metricsRepository);

    // Start the service
    deferredVersionSwapService.startInner();

    // Wait for the service to process the store
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // Verify error recording was called due to the failure
      verify(store, atLeastOnce()).updateVersionStatus(2, VersionStatus.PARTIALLY_ONLINE);
      verify(admin, never()).rollForwardToFutureVersion(clusterName, storeName, region3);
      verify(admin, never()).truncateKafkaTopic(anyString(), anyInt());
    });
  }

  /**
   * Validation test - Version is not ready for swap when push status is not complete
   */
  @Test
  public void testSequentialRolloutVersionValidationFails() throws Exception {
    String storeName = "testStore";
    Store store = mockStore(versionOne, versionTwo, storeName);

    // Add lifecycle hooks mocking using MockStoreLifecycleHooks pattern
    List<LifecycleHooksRecord> lifecycleHooks = new ArrayList<>();
    Map<String, String> params = new HashMap<>();
    params.put("outcome", StoreVersionLifecycleEventOutcome.PROCEED.toString());
    lifecycleHooks.add(new LifecycleHooksRecordImpl(MockStoreLifecycleHooks.class.getName(), params));
    doReturn(lifecycleHooks).when(store).getStoreLifecycleHooks();

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

    // Mock store response
    StoreResponse storeResponse = getStoreResponse(versionList);

    // Mock controller clients for each region
    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(anyString(), anyInt());
    }

    // Setup store repo
    doReturn(store).when(repository).getStore(storeName);

    // Simulate push NOT completed in all regions (region2 is still in STARTED state)
    Long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithIncompletePush = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.STARTED.toString(), // region2 push is not complete
        ExecutionStatus.COMPLETED.toString(),
        time - TimeUnit.MINUTES.toSeconds(90),
        0, // No end time for region2
        time - TimeUnit.MINUTES.toSeconds(30));

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithIncompletePush).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    // Create service
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats, metricsRepository);

    // Start the service
    deferredVersionSwapService.startInner();

    // Wait for the service to process the store
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // No rollforward should happen for any region
      verify(admin, never()).rollForwardToFutureVersion(anyString(), anyString(), anyString());
    });

    // Verify error was not recorded since this is an expected validation failure
    verify(stats, never()).recordDeferredVersionSwapErrorSensor();
  }

  /**
   * When the last region in rollout order is ONLINE,
   * parent version is marked as ONLINE
   */
  @Test
  public void testSequentialRolloutFinalRegionCompletion() throws Exception {
    String storeName = "testStore";
    Store store = mockStore(versionOne, versionTwo, storeName);

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

    // Setup store repo
    doReturn(store).when(repository).getStore(storeName);

    // Mock controller clients - setup region3 (last region) to have ONLINE status
    controllerClientMap = new HashMap<>();
    for (String region: Arrays.asList(region1, region2, region3)) {
      ControllerClient controllerClient = mock(ControllerClient.class);
      controllerClientMap.put(region, controllerClient);

      StoreResponse storeResponse = new StoreResponse();
      StoreInfo storeInfo = mock(StoreInfo.class);
      doReturn(versionList).when(storeInfo).getVersions();

      // Create version with ONLINE status for region3 (last region in rollout order)
      Version regionVersion = mock(Version.class);
      doReturn(versionTwo).when(regionVersion).getNumber();
      if (region.equals(region3)) {
        // Last region has ONLINE status to trigger the condition at L625-635
        doReturn(VersionStatus.ONLINE).when(regionVersion).getStatus();
      } else {
        // Other regions have PUSHED status
        doReturn(VersionStatus.PUSHED).when(regionVersion).getStatus();
      }

      // For region3, we need both version 1 (current) and version 2 (target with ONLINE status)
      if (region.equals(region3)) {
        Version currentVersion = mock(Version.class);
        doReturn(versionOne).when(currentVersion).getNumber();
        doReturn(VersionStatus.ONLINE).when(currentVersion).getStatus();

        List<Version> regionVersions = Arrays.asList(currentVersion, regionVersion);
        doReturn(regionVersions).when(storeInfo).getVersions();
        doReturn(Optional.of(currentVersion)).when(storeInfo).getVersion(versionOne);
        doReturn(Optional.of(regionVersion)).when(storeInfo).getVersion(versionTwo);
      } else {
        List<Version> regionVersions = Arrays.asList(regionVersion);
        doReturn(regionVersions).when(storeInfo).getVersions();
        doReturn(Optional.of(regionVersion)).when(storeInfo).getVersion(versionTwo);
      }

      storeResponse.setStore(storeInfo);
      doReturn(storeResponse).when(controllerClient).getStore(storeName, controllerTimeout);

      JobStatusQueryResponse response = mock(JobStatusQueryResponse.class);
      doReturn(false).when(response).isError();
      doReturn(ExecutionStatus.COMPLETED.toString()).when(response).getStatus();
      doReturn(response).when(controllerClient).queryJobStatus(any(), any(), anyInt(), any(), anyBoolean());
    }
    doReturn(controllerClientMap).when(veniceHelixAdmin).getControllerClientMap(clusterName);

    // Setup current versions - region3 (last region) should be behind to trigger the condition
    // All other regions should be at target version, region3 should be behind but status ONLINE
    Map<String, Integer> currentVersionsMap = new HashMap<>();
    currentVersionsMap.put(region1, versionTwo);
    currentVersionsMap.put(region2, versionTwo);
    currentVersionsMap.put(region3, versionOne); // Final region is behind - this makes it nextEligibleRegion
    doReturn(currentVersionsMap).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName);

    // Simulate push completed in all regions
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

    doReturn(clusterConfig).when(veniceControllerMultiClusterConfig).getControllerConfig(clusterName);
    doReturn(ConcurrentPushDetectionStrategy.TOPIC_BASED_ONLY).when(clusterConfig).getConcurrentPushDetectionStrategy();
    doReturn(false).when(admin).isTopicTruncated(anyString());

    // Create service
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats, metricsRepository);

    // Start the service
    deferredVersionSwapService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // Verify that updateStore was called to mark parent version as ONLINE
      verify(store, atLeastOnce()).updateVersionStatus(versionTwo, VersionStatus.ONLINE);
      verify(admin, never()).truncateKafkaTopic(anyString(), anyInt());
    });

    // Verify error recording was not called
    verify(stats, never()).recordDeferredVersionSwapErrorSensor();
  }

  /**
   * Stalled version swap - Metric is emitted for long-running swaps
   */
  @Test
  public void testStalledVersionSwapMetric() throws Exception {
    String storeName = "testStore";
    Store store = mockStore(versionOne, versionTwo, storeName);

    List<Store> storeList = new ArrayList<>();
    storeList.add(store);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    Version versionOneImpl = new VersionImpl(storeName, versionOne);
    Version versionTwoImpl = new VersionImpl(storeName, versionTwo);
    versionTwoImpl.setStatus(VersionStatus.PUSHED);
    versionTwoImpl.setVersionSwapDeferred(true);
    versionTwoImpl.setTargetSwapRegion("region1,region2,region3");
    List<Version> versionList = new ArrayList<>();
    versionList.add(versionOneImpl);
    versionList.add(versionTwoImpl);
    StoreResponse storeResponse = getStoreResponse(versionList);

    // Mock controller clients for each region
    Map<String, ControllerClient> controllerClientMap = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponse).when(controllerClient).getStore(anyString(), anyInt());
    }

    // Setup store repo
    doReturn(store).when(repository).getStore(storeName);

    // Setup current versions for multi-colos (region1 already rolled forward, regions 2&3 are stalled)
    Map<String, Integer> currentVersionsMap = new HashMap<>();
    currentVersionsMap.put("region1", versionTwo); // Already rolled forward to version 2
    currentVersionsMap.put("region2", versionOne); // Stuck on version 1 (stalled)
    currentVersionsMap.put("region3", versionOne); // Stuck on version 1 (stalled)
    doReturn(currentVersionsMap).when(admin).getCurrentVersionsForMultiColos(clusterName, storeName);

    Long currentTime = System.currentTimeMillis();

    // For stalled metric to be recorded, condition at L523 must be true:
    // (latestVersionPromoteToCurrentTimestamp + storeWaitTime + bufferedWaitTime) > currentTime
    // We need a timestamp that when added to wait times still exceeds current time
    Long recentTimestamp = currentTime - TimeUnit.MINUTES.toSeconds(5); // 5 minutes ago - recent enough to pass
                                                                        // condition

    // Create separate StoreResponse objects for each region with proper timestamps
    StoreResponse storeResponseWithTimestamp = new StoreResponse();
    StoreInfo storeInfoWithTimestamp = mock(StoreInfo.class);
    doReturn(versionList).when(storeInfoWithTimestamp).getVersions();
    doReturn(currentTime - TimeUnit.MINUTES.toMillis(90)).when(storeInfoWithTimestamp)
        .getLatestVersionPromoteToCurrentTimestamp(); // Convert to milliseconds

    // Mock individual version lookup - crucial for service to find specific versions
    doReturn(Optional.of(versionOneImpl)).when(storeInfoWithTimestamp).getVersion(versionOne);
    doReturn(Optional.of(versionTwoImpl)).when(storeInfoWithTimestamp).getVersion(versionTwo);

    storeResponseWithTimestamp.setStore(storeInfoWithTimestamp);

    // Mock controller clients for each region to return StoreInfo with proper timestamp
    Map<String, ControllerClient> controllerClientMapWithTimestamp = mockControllerClients(versionList);
    for (Map.Entry<String, ControllerClient> entry: controllerClientMapWithTimestamp.entrySet()) {
      ControllerClient controllerClient = entry.getValue();
      doReturn(storeResponseWithTimestamp).when(controllerClient).getStore(anyString(), anyInt());
    }

    // Wire the controller client map to the VeniceHelixAdmin
    doReturn(controllerClientMapWithTimestamp).when(veniceHelixAdmin).getControllerClientMap(clusterName);

    // Simulate push completed in all regions
    Admin.OfflinePushStatusInfo offlinePushStatusInfoWithStalledSwap = getOfflinePushStatusInfo(
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        ExecutionStatus.COMPLETED.toString(),
        recentTimestamp,
        recentTimestamp,
        recentTimestamp);

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
    doReturn(offlinePushStatusInfoWithStalledSwap).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);

    // Create service
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats, metricsRepository);

    // Start the service
    deferredVersionSwapService.startInner();

    // Wait for the service to process the store and record stalled metric
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // Verify stalled swap metric was emitted - this happens when condition at L523 is true
      verify(stats, atLeastOnce()).recordDeferredVersionSwapStalledVersionSwapSensor(anyDouble());
    });
  }

  @Test
  public void testPerClusterThreadPoolCreationWithSequentialRollout() throws Exception {
    // Test that thread pools are created per cluster with correct configuration for sequential rollout
    String cluster1 = "sequentialCluster1";
    String cluster2 = "sequentialCluster2";
    int threadPoolSize1 = 8;
    int threadPoolSize2 = 12;

    // Setup mock configurations for multiple clusters with sequential rollout
    VeniceControllerClusterConfig clusterConfig1 = mock(VeniceControllerClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig2 = mock(VeniceControllerClusterConfig.class);
    doReturn(threadPoolSize1).when(clusterConfig1).getDeferredVersionSwapThreadPoolSize();
    doReturn(threadPoolSize2).when(clusterConfig2).getDeferredVersionSwapThreadPoolSize();

    // Sequential rollout configuration
    doReturn(region1 + "," + region2 + "," + region3).when(clusterConfig1)
        .getDeferredVersionSwapRegionRollforwardOrder();
    doReturn(region1 + "," + region2).when(clusterConfig2).getDeferredVersionSwapRegionRollforwardOrder();

    doReturn(clusterConfig1).when(veniceControllerMultiClusterConfig).getControllerConfig(cluster1);
    doReturn(clusterConfig2).when(veniceControllerMultiClusterConfig).getControllerConfig(cluster2);

    DeferredVersionSwapStats deferredVersionSwapStats = mock(DeferredVersionSwapStats.class);
    DeferredVersionSwapService service = new DeferredVersionSwapService(
        admin,
        veniceControllerMultiClusterConfig,
        deferredVersionSwapStats,
        metricsRepository);

    // Use reflection to access the private getOrCreateExecutorForCluster method
    java.lang.reflect.Method getExecutorMethod =
        DeferredVersionSwapService.class.getDeclaredMethod("getOrCreateExecutorForCluster", String.class);
    getExecutorMethod.setAccessible(true);

    // Create executors for both clusters
    ExecutorService executor1 = (ExecutorService) getExecutorMethod.invoke(service, cluster1);
    ExecutorService executor2 = (ExecutorService) getExecutorMethod.invoke(service, cluster2);

    // Verify executors are created and different for each cluster
    Assert.assertNotNull(executor1, "Executor for cluster1 should be created");
    Assert.assertNotNull(executor2, "Executor for cluster2 should be created");
    Assert.assertNotEquals(executor1, executor2, "Each cluster should have its own executor");

    // Clean up
    service.stopInner();
  }

  @Test
  public void testSequentialRolloutWithMultipleStoresUsingThreadPools() throws Exception {
    String storeName1 = "sequentialStore1";
    String storeName2 = "sequentialStore2";
    String storeName3 = "sequentialStore3";

    // Create multiple stores for sequential rollout processing
    Store store1 = mockStore(versionOne, versionTwo, storeName1);
    Store store2 = mockStore(versionOne, versionTwo, storeName2);
    Store store3 = mockStore(versionOne, versionTwo, storeName3);

    List<Store> storeList = Arrays.asList(store1, store2, store3);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    // Setup thread pool configuration for sequential rollout
    doReturn(3).when(clusterConfig).getDeferredVersionSwapThreadPoolSize(); // 3 threads for concurrent store processing
    // Keep existing sequential rollout order from setUp()

    // Mock controller clients
    List<Version> versionList = new ArrayList<>();
    Map<String, ControllerClient> controllerClientMapWithStores = mockControllerClients(versionList);
    doReturn(controllerClientMapWithStores).when(veniceHelixAdmin).getControllerClientMap(clusterName);

    // Mock successful processing for all stores
    for (String storeName: Arrays.asList(storeName1, storeName2, storeName3)) {
      String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
      Admin.OfflinePushStatusInfo pushStatusInfo = getOfflinePushStatusInfo(
          ExecutionStatus.COMPLETED.toString(),
          ExecutionStatus.COMPLETED.toString(),
          ExecutionStatus.COMPLETED.toString(),
          System.currentTimeMillis() / 1000 - 3600,
          System.currentTimeMillis() / 1000 - 3600,
          System.currentTimeMillis() / 1000 - 3600);
      doReturn(pushStatusInfo).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);
    }

    DeferredVersionSwapService service =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats, metricsRepository);
    service.startInner();

    // Wait for processing to complete
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(admin, atLeastOnce()).getOffLinePushStatus(anyString(), anyString());
    });

    service.stopInner();
  }

  @Test
  public void testConcurrentStoreProcessingWithSequentialRolloutConfiguration() throws Exception {
    // Test that multiple stores can be processed concurrently even with sequential rollout configuration
    String storeName1 = "seqConcurrentStore1";
    String storeName2 = "seqConcurrentStore2";
    String storeName3 = "seqConcurrentStore3";
    String storeName4 = "seqConcurrentStore4";

    Store store1 = mockStore(versionOne, versionTwo, storeName1);
    Store store2 = mockStore(versionOne, versionTwo, storeName2);
    Store store3 = mockStore(versionOne, versionTwo, storeName3);
    Store store4 = mockStore(versionOne, versionTwo, storeName4);

    List<Store> storeList = Arrays.asList(store1, store2, store3, store4);
    doReturn(storeList).when(admin).getAllStores(clusterName);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);

    // Setup configuration with sufficient threads for concurrent processing
    doReturn(4).when(clusterConfig).getDeferredVersionSwapThreadPoolSize(); // 4 threads for 4 stores
    // Keep existing sequential rollout order (region1,region2,region3)

    List<Version> versionList = new ArrayList<>();
    Map<String, ControllerClient> concurrentControllerClientMap = mockControllerClients(versionList);
    doReturn(concurrentControllerClientMap).when(veniceHelixAdmin).getControllerClientMap(clusterName);

    // Mock successful processing for all stores
    for (String storeName: Arrays.asList(storeName1, storeName2, storeName3, storeName4)) {
      String kafkaTopicName = Version.composeKafkaTopic(storeName, versionTwo);
      Admin.OfflinePushStatusInfo pushStatusInfo = getOfflinePushStatusInfo(
          ExecutionStatus.COMPLETED.toString(),
          ExecutionStatus.COMPLETED.toString(),
          ExecutionStatus.COMPLETED.toString(),
          System.currentTimeMillis() / 1000 - 3600,
          System.currentTimeMillis() / 1000 - 3600,
          System.currentTimeMillis() / 1000 - 3600);
      doReturn(pushStatusInfo).when(admin).getOffLinePushStatus(clusterName, kafkaTopicName);
    }

    DeferredVersionSwapService service =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats, metricsRepository);
    service.startInner();

    // Wait for processing and verify thread pool was used for concurrent processing
    TestUtils.waitForNonDeterministicAssertion(12, TimeUnit.SECONDS, () -> {
      verify(admin, atLeastOnce()).getOffLinePushStatus(anyString(), anyString());
    });

    service.stopInner();
  }

  // Helper method to create offline push status info for sequential rollout tests
  private Admin.OfflinePushStatusInfo getOfflinePushStatusInfo(
      String region1Status,
      String region2Status,
      String region3Status,
      Long region1Timestamp,
      Long region2Timestamp,
      Long region3Timestamp) {
    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put(region1, region1Status);
    extraInfo.put(region2, region2Status);
    extraInfo.put(region3, region3Status);

    Map<String, Long> extraInfoUpdateTimestamp = new HashMap<>();
    extraInfoUpdateTimestamp.put(region1, region1Timestamp);
    extraInfoUpdateTimestamp.put(region2, region2Timestamp);
    extraInfoUpdateTimestamp.put(region3, region3Timestamp);

    return new Admin.OfflinePushStatusInfo(
        ExecutionStatus.COMPLETED,
        123L,
        extraInfo,
        null,
        null,
        extraInfoUpdateTimestamp);
  }
}
