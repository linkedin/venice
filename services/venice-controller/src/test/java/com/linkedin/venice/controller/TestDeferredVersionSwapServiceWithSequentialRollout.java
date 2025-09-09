package com.linkedin.venice.controller;

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
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.locks.ClusterLockManager;
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
import java.util.concurrent.TimeUnit;
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
  private static final int controllerTimeout = 5 * Time.MS_PER_SECOND;

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
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats);

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

    // Create service
    DeferredVersionSwapService deferredVersionSwapService =
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats);

    // Start the service
    deferredVersionSwapService.startInner();

    // Wait for the service to process the store
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // Verify error recording was called due to the failure
      verify(store, atLeastOnce()).updateVersionStatus(2, VersionStatus.PARTIALLY_ONLINE);
      verify(admin, never()).rollForwardToFutureVersion(clusterName, storeName, region3);
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
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats);

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
        new DeferredVersionSwapService(admin, veniceControllerMultiClusterConfig, stats);

    // Start the service
    deferredVersionSwapService.startInner();

    // Wait for the service to process the store and record stalled metric
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // Verify stalled swap metric was emitted - this happens when condition at L523 is true
      verify(stats, atLeastOnce()).recordDeferredVersionSwapStalledVersionSwapSensor(anyDouble());
    });
  }
}
