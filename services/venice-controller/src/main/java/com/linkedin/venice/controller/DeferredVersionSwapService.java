package com.linkedin.venice.controller;

import static com.linkedin.venice.meta.VersionStatus.ERROR;
import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static com.linkedin.venice.meta.VersionStatus.PARTIALLY_ONLINE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.controller.stats.DeferredVersionSwapStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service is in charge of swapping to a new version after a specified wait time in the remaining regions of a target region push if enabled.
 * The wait time is specified through a store/version level config (target_swap_region_wait_time) and the default wait time is 60m.
 * This service also updates the parent version status of a store w/ a terminal push status after performing the swap
 * or deeming it ineligible for a version swap and the statuses mean:
 * 1. ONLINE - all regions are serving the target version
 * 2. PARTIALLY_ONLINE - 1+ regions are serving the target version, but 1+ regions' pushes failed & are serving the old version
 * 3. ERROR - all regions are serving the old version. the push failed in target regions
 */
public class DeferredVersionSwapService extends AbstractVeniceService {
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private final VeniceParentHelixAdmin veniceParentHelixAdmin;
  private final ScheduledExecutorService deferredVersionSwapExecutor =
      Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory(getClass().getSimpleName()));
  private final DeferredVersionSwapStats deferredVersionSwapStats;
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
  private static final Logger LOGGER = LogManager.getLogger(DeferredVersionSwapService.class);
  private static final int MAX_FETCH_STORE_FETCH_RETRY_LIMIT = 5;
  private Cache<String, Map<String, Long>> storePushCompletionTimeCache =
      Caffeine.newBuilder().expireAfterWrite(2, TimeUnit.HOURS).build();
  private Map<String, Integer> fetchNonTargetRegionStoreRetryCountMap = new ConcurrentHashMap<>();
  private Set<String> stalledVersionSwapSet = new ConcurrentHashMap<>().newKeySet();
  private Map<String, Integer> failedRollforwardRetryCountMap = new ConcurrentHashMap<>();
  private static final int MAX_ROLL_FORWARD_RETRY_LIMIT = 5;
  private static final Set<VersionStatus> VERSION_SWAP_COMPLETION_STATUSES =
      Utils.setOf(ONLINE, PARTIALLY_ONLINE, ERROR);
  private static final Set<VersionStatus> TERMINAL_PUSH_VERSION_STATUSES = Utils.setOf(ONLINE);
  private Cache<String, Long> storeWaitTimeCacheForSequentialRollout =
      Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build();
  private static final int CONTROLLER_CLIENT_REQUEST_TIMEOUT = 1 * Time.MS_PER_SECOND;
  private static final int LOG_LATENCY_THRESHOLD = 5 * Time.MS_PER_SECOND;
  private final Map<String, ThreadPoolExecutor> clusterToExecutorMap = new ConcurrentHashMap<>();
  private final Set<String> storesBeingProcessed = ConcurrentHashMap.newKeySet();
  private final Map<String, ThreadPoolStats> clusterToThreadPoolStatsMap = new ConcurrentHashMap<>();
  private final MetricsRepository metricsRepository;
  private Map<String, StoreLifecycleHooks> storeLifecycleHooksCache = new HashMap<>();

  public DeferredVersionSwapService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig,
      DeferredVersionSwapStats deferredVersionSwapStats,
      MetricsRepository metricsRepository) {
    this.veniceParentHelixAdmin = admin;
    this.veniceControllerMultiClusterConfig = multiClusterConfig;
    this.deferredVersionSwapStats = deferredVersionSwapStats;
    this.metricsRepository = metricsRepository;
  }

  @Override
  public boolean startInner() throws Exception {
    deferredVersionSwapExecutor.scheduleAtFixedRate(
        getRunnableForDeferredVersionSwap(),
        0,
        veniceControllerMultiClusterConfig.getDeferredVersionSwapSleepMs(),
        TimeUnit.MILLISECONDS);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    deferredVersionSwapExecutor.shutdown();

    clusterToExecutorMap.entrySet().parallelStream().forEach(entry -> {
      String cluster = entry.getKey();
      ExecutorService executor = entry.getValue();

      LOGGER.info("Shutting down executor for cluster: {}", cluster);
      executor.shutdown();

      try {
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
          LOGGER.warn("Force shutting down executor for cluster: {}", cluster);
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    });
  }

  private ThreadPoolExecutor getOrCreateExecutorForCluster(String cluster) {
    return clusterToExecutorMap.computeIfAbsent(cluster, c -> {
      int threadPoolSize =
          veniceControllerMultiClusterConfig.getControllerConfig(cluster).getDeferredVersionSwapThreadPoolSize();

      DaemonThreadFactory threadFactory = new DaemonThreadFactory(cluster + "-deferred-version-swap");
      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize, threadFactory);
      String statsName = "DeferredVersionSwap-" + cluster;
      ThreadPoolStats threadPoolStats = new ThreadPoolStats(metricsRepository, executor, statsName);
      clusterToThreadPoolStatsMap.put(cluster, threadPoolStats);

      LOGGER.info("Created thread pool for cluster {} with {} threads", cluster, threadPoolSize);
      return executor;
    });
  }

  private Set<String> getRegionsForVersionSwap(Map<String, ControllerClient> candidateRegions, String targetRegion) {
    Set<String> remainingRegions = new HashSet<>(candidateRegions.keySet());
    remainingRegions.remove(targetRegion);
    return remainingRegions;
  }

  private StoreResponse getStoreForRegion(String clusterName, String targetRegion, String storeName) {
    Map<String, ControllerClient> controllerClientMap =
        veniceParentHelixAdmin.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    ControllerClient targetRegionControllerClient = controllerClientMap.get(targetRegion);
    StoreResponse storeResponse = targetRegionControllerClient.getStore(storeName, CONTROLLER_CLIENT_REQUEST_TIMEOUT);

    if (storeResponse.isError()) {
      String message =
          "Got error " + storeResponse.getError() + " when fetching targetRegionStore: " + storeResponse.getStore();
      logMessageIfNotRedundant(message);
      return null;
    }

    return storeResponse;
  }

  /**
   * Checks whether the wait time has passed since the push completion time in the list of regions
   * @param completionTimes a mapping of region to push completion time
   * @param targetRegion the list of regions to check if wait time has elapsed
   * @param store the store to check if the push wait time has elapsed
   * @return
   */
  private boolean didWaitTimeElapseInTargetRegions(
      Map<String, Long> completionTimes,
      String targetRegion,
      Store store) {
    if (!completionTimes.containsKey(targetRegion)) {
      return false;
    }

    long completionTime = completionTimes.get(targetRegion);
    long storeWaitTime = TimeUnit.MINUTES.toSeconds(store.getTargetSwapRegionWaitTime());
    long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    if ((completionTime + storeWaitTime) > currentTime) {
      return false;
    }

    return true;
  }

  /**
   * Checks whether the wait time has passed since the cached push completion time in the list of regions
   * @param targetRegion the list of regions to check if wait time has elapsed
   * @param store the store to check if the push wait time has elapsed
   * @param targetVersionNum the version to check if the push wait time has elapsed
   * @param kafkaTopicName the name of the kafka topic for this target version
   * @return
   */
  private boolean didCachedWaitTimeElapseInTargetRegions(
      String targetRegion,
      Store store,
      int targetVersionNum,
      String kafkaTopicName) {
    Map<String, Long> storePushCompletionTimes = storePushCompletionTimeCache.getIfPresent(kafkaTopicName);

    // If there is no cached completion time, we should let the service continue the checks for the store as:
    // 1. It could be a new push that we haven't checked for yet
    // 2. The existing cached wait time expired
    if (storePushCompletionTimes == null) {
      return true;
    }

    return didWaitTimeElapseInTargetRegions(storePushCompletionTimes, targetRegion, store);
  }

  private void logMessageIfNotRedundant(String message) {
    if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(message)) {
      LOGGER.info(message);
    }
  }

  /**
   * Gets the specified version for a store in a specific region
   * @param region name of the region to get the store from
   * @param storeName name of the store
   * @param targetVersionNum the version number to get
   * @return
   */
  private Version getVersionFromStoreInRegion(
      String region,
      String storeName,
      int targetVersionNum,
      StoreResponse storeResponse) {
    if (storeResponse == null) {
      return null;
    }

    StoreInfo targetRegionStore = storeResponse.getStore();
    Optional<Version> version = targetRegionStore.getVersion(targetVersionNum);
    if (!version.isPresent()) {
      String message =
          "Unable to find version " + targetVersionNum + " for store: " + storeName + " in region " + region;
      logMessageIfNotRedundant(message);
      return null;
    }

    return version.get();
  }

  /**
   * Roll forward to the specified version for a list of regions. Once the roll forward is done, traffic will be served from
   * that version and the version status will be updated to ONLINE or PARTIALLY_ONLINE
   * @param regions the list of regions to update the version status
   * @param store the store of the version to roll forward in
   * @param targetVersion the version to start serving traffic in
   * @param cluster the cluster the store is in
   */
  private void rollForwardToTargetVersion(Set<String> regions, Store store, Version targetVersion, String cluster) {
    String regionsToRollForward = RegionUtils.composeRegionList(regions);
    String storeName = store.getName();
    int targetVersionNum = targetVersion.getNumber();
    LOGGER.info(
        "Issuing roll forward message for store: {} in regions: {} for version: {}",
        storeName,
        regionsToRollForward,
        targetVersionNum);
    veniceParentHelixAdmin.rollForwardToFutureVersion(cluster, storeName, regionsToRollForward);

    if (stalledVersionSwapSet.contains(storeName)) {
      stalledVersionSwapSet.remove(storeName);
      deferredVersionSwapStats.recordDeferredVersionSwapStalledVersionSwapSensor(stalledVersionSwapSet.size());
    }

    // Update parent version status after roll forward, so we don't check this store version again
    // If push was successful (version status is PUSHED), the parent version is marked as ONLINE
    // if push was successful in some regions (version status is KILLED), the parent version is marked PARTIALLY_ONLINE
    long totalVersionSwapTimeInMinutes =
        TimeUnit.MILLISECONDS.toMinutes(LatencyUtils.getElapsedTimeFromMsToMs(targetVersion.getCreatedTime()));
    if (targetVersion.getStatus() == VersionStatus.KILLED) {
      updateStore(cluster, storeName, PARTIALLY_ONLINE, targetVersionNum);
      LOGGER.info(
          "Updated parent version status to PARTIALLY_ONLINE for version: {} in store: {},"
              + "Version swap took {} minutes from push completion to version swap",
          targetVersionNum,
          storeName,
          totalVersionSwapTimeInMinutes);
    } else {
      updateStore(cluster, storeName, ONLINE, targetVersionNum);
      LOGGER.info(
          "Updated parent version status to ONLINE for version: {} in store: {}."
              + "Version swap took {} minutes from push completion to version swap",
          targetVersionNum,
          storeName,
          totalVersionSwapTimeInMinutes);
    }
  }

  /**
   * Checks if all the regions have a version status
   * @param regions
   * @param clusterName
   * @param storeName
   * @param targetVersionNum
   * @param versionStatus
   * @return
   */
  private boolean doesRegionsHaveVersionStatus(
      Set<String> regions,
      String clusterName,
      String storeName,
      int targetVersionNum,
      Set<VersionStatus> versionStatus) {
    for (String region: regions) {
      StoreResponse storeResponse = getStoreForRegion(clusterName, region, storeName);
      Version regionVersion = getVersionFromStoreInRegion(region, storeName, targetVersionNum, storeResponse);
      if (regionVersion == null) {
        return false;
      }

      if (!versionStatus.contains(regionVersion.getStatus())) {
        return false;
      }

      // There is a version status mismatch if the version is marked as online and the future version is not the current
      // version
      if (regionVersion.getStatus() == ONLINE && storeResponse.getStore().getCurrentVersion() != targetVersionNum) {
        return false;
      }
    }

    return true;
  }

  /**
   * Checks if a store version is in a terminal state. It is in a terminal state & eligible for a version swap if targetSwapRegion is not
   * empty and the push job for the current version is completed. It is completed if the version status is either PUSHED or
   * KILLED (see VeniceParentHelixAdmin.getOfflinePushStatus)
   * @param targetVersion the version to check eligibility for
   * @return
   */
  private boolean isPushInTerminalState(
      Version targetVersion,
      String clusterName,
      String storeName,
      int targetVersionNum,
      Set<String> nonTargetRegions,
      String targetRegion) {
    // The version could've been manually rolled forward in non target regions. If that is the case, we should check if
    // we emitted any stalled metric for it and remove it if so
    if (stalledVersionSwapSet.contains(storeName)) {
      boolean didPushCompleteInNonTargetRegions = doesRegionsHaveVersionStatus(
          nonTargetRegions,
          clusterName,
          storeName,
          targetVersion.getNumber(),
          VERSION_SWAP_COMPLETION_STATUSES);
      if (didPushCompleteInNonTargetRegions) {
        stalledVersionSwapSet.remove(storeName);
        deferredVersionSwapStats.recordDeferredVersionSwapStalledVersionSwapSensor(stalledVersionSwapSet.size());
      }
    }

    switch (targetVersion.getStatus()) {
      case STARTED:
        // Because the parent status is updated when we poll for the job status, the parent status will not always be an
        // accurate representation of push status if vpj runs away
        boolean didPushCompleteInTargetRegions = doesRegionsHaveVersionStatus(
            Collections.singleton(targetRegion),
            clusterName,
            storeName,
            targetVersion.getNumber(),
            TERMINAL_PUSH_VERSION_STATUSES);
        if (didPushCompleteInTargetRegions) {
          deferredVersionSwapStats.recordDeferredVersionSwapParentChildStatusMismatchSensor();
          String message =
              "Push completed in target regions, parent status is still STARTED. Continuing with deferred swap for store: "
                  + storeName + " for version: " + targetVersionNum;
          logMessageIfNotRedundant(message);
          return true;
        }

        return false;
      case PUSHED:
        return true;
      case ONLINE:
        // This should not happen, but if it does, we should still perform a version swap and log a metric for it
        // TODO remove this case once the sequential rollout is complete and log does not show up
        boolean didVersionSwapCompleteInNonTargetRegions = doesRegionsHaveVersionStatus(
            nonTargetRegions,
            clusterName,
            storeName,
            targetVersion.getNumber(),
            VERSION_SWAP_COMPLETION_STATUSES);

        if (!didVersionSwapCompleteInNonTargetRegions) {
          deferredVersionSwapStats.recordDeferredVersionSwapParentChildStatusMismatchSensor();
          String message =
              "Parent status is already ONLINE, but version swap has not happened in the non target regions. "
                  + "Continuing with deferred swap for store: " + storeName + " for version: " + targetVersionNum;
          logMessageIfNotRedundant(message);
          return true;
        }
    }

    return false;
  }

  /**
   * Given a list of regions and push statuses and an expected status, return the number of regions in that status
   * @param regions list of regions to find the push status for
   * @param pushStatusInfo wrapper containing push status info
   * @param status expected status to search for
   * @return
   */
  private int getRegionsWithPushStatusCount(
      Set<String> regions,
      Admin.OfflinePushStatusInfo pushStatusInfo,
      ExecutionStatus status) {
    int regionsWithStatus = 0;
    for (String region: regions) {
      String executionStatus = pushStatusInfo.getExtraInfo().get(region);
      if (executionStatus.equals(status.toString())) {
        regionsWithStatus += 1;
      }
    }
    return regionsWithStatus;
  }

  /**
   * Checks if a push completed in all target regions.
   * If the push failed in a majority of target regions, mark the parent version status as ERROR.
   * If all target regions have not reached a terminal push status yet, do not proceed yet
   * @param targetRegion target region to check the push status for
   * @param pushStatusInfo wrapper containing push status information
   * @return
   */
  private boolean didPushCompleteInTargetRegions(
      String targetRegion,
      Admin.OfflinePushStatusInfo pushStatusInfo,
      Store store,
      int targetVersionNum,
      String clusterName) {
    Set<String> targetRegionSet = Collections.singleton(targetRegion);
    int numCompletedTargetRegions =
        getRegionsWithPushStatusCount(targetRegionSet, pushStatusInfo, ExecutionStatus.COMPLETED);
    int numFailedTargetRegions = getRegionsWithPushStatusCount(targetRegionSet, pushStatusInfo, ExecutionStatus.ERROR);
    if (numFailedTargetRegions > 0) {
      String message = "Skipping version swap for store: " + store.getName() + " on version: " + targetVersionNum
          + " as push failed in 1+ target regions. Completed target regions: " + numCompletedTargetRegions
          + " , failed target regions: " + numFailedTargetRegions + ", target regions: " + targetRegion;
      logMessageIfNotRedundant(message);
      updateStore(clusterName, store.getName(), ERROR, targetVersionNum);
      return false;
    } else if (numCompletedTargetRegions < 1) {
      // TODO remove after ramp as this is a temporary log to help with debugging
      String message = "Skipping version swap for store: " + store.getName() + " on version: " + targetVersionNum
          + " as push is not complete yet in target regions. num completed target regions: " + numFailedTargetRegions;
      logMessageIfNotRedundant(message);
      return false;
    }

    return true;
  }

  /**
   * Check if the version swap for a store is stalled. A version swap is considered stalled if the wait time has elapsed and
   * more than the wait time * {deferred.version.swap.buffer.time} has passed without switching to the target version.
   * Emit a metric is this happens
   * @param completionTimes the push completion time of the regions
   * @param targetRegion the list of target regions
   * @param store the store to check for
   * @param targetVersionNum the target version number to check for
   */
  private void emitMetricIfVersionSwapIsStalled(
      Map<String, Long> completionTimes,
      String targetRegion,
      Store store,
      int targetVersionNum,
      Version parentVersion) {
    if (parentVersion.getStatus().equals(ONLINE) || parentVersion.getStatus().equals(ERROR)
        || parentVersion.getStatus().equals(PARTIALLY_ONLINE)) {
      return;
    }

    // If we already emitted a metric for this store already, do not emit it again
    if (stalledVersionSwapSet.contains(store.getName())) {
      return;
    }

    if (!completionTimes.containsKey(targetRegion)) {
      return;
    }

    long completionTime = completionTimes.get(targetRegion);
    long bufferedWaitTime = TimeUnit.MINUTES.toSeconds(
        Math.round(
            store.getTargetSwapRegionWaitTime()
                * veniceControllerMultiClusterConfig.getDeferredVersionSwapBufferTime()));
    long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    if ((completionTime + bufferedWaitTime) < currentTime) {
      String message = "Store: " + store.getName() + " has not swapped to the target version: " + targetVersionNum
          + " and the wait time: " + store.getTargetSwapRegionWaitTime() + " has passed in target region "
          + targetRegion;
      logMessageIfNotRedundant(message);
      stalledVersionSwapSet.add(store.getName());
      deferredVersionSwapStats.recordDeferredVersionSwapStalledVersionSwapSensor(stalledVersionSwapSet.size());
    }
  }

  private void emitMetricIfVersionSwapIfStalledForSequentialRollout(
      String region,
      Store parentStore,
      int targetVersionNum,
      String clusterName) {
    VersionStatus status = parentStore.getVersion(targetVersionNum).getStatus();
    if (status.equals(ONLINE) || status.equals(ERROR) || status.equals(PARTIALLY_ONLINE)) {
      return;
    }

    // If we already emitted a metric for this store already, do not emit it again
    if (stalledVersionSwapSet.contains(parentStore.getName())) {
      return;
    }

    StoreResponse storeResponse = getStoreForRegion(clusterName, region, parentStore.getName());
    if (storeResponse == null) {
      return;
    }

    StoreInfo storeInfo = storeResponse.getStore();

    if (storeInfo.getCurrentVersion() == targetVersionNum) {
      return;
    }

    long latestVersionPromoteToCurrentTimestamp = storeInfo.getLatestVersionPromoteToCurrentTimestamp();
    long storeWaitTime = TimeUnit.MINUTES.toMillis(parentStore.getTargetSwapRegionWaitTime());
    long currentTime = System.currentTimeMillis();
    long bufferedWaitTime = TimeUnit.MINUTES.toMillis(
        Math.round(
            parentStore.getTargetSwapRegionWaitTime()
                * veniceControllerMultiClusterConfig.getDeferredVersionSwapBufferTime()));
    if ((latestVersionPromoteToCurrentTimestamp + storeWaitTime + bufferedWaitTime) < currentTime) {
      String message = "Store: " + parentStore.getName() + " has not swapped to the target version: " + targetVersionNum
          + " and the wait time: " + parentStore.getTargetSwapRegionWaitTime() + " has passed in region " + region;
      logMessageIfNotRedundant(message);
      stalledVersionSwapSet.add(parentStore.getName());
      deferredVersionSwapStats.recordDeferredVersionSwapStalledVersionSwapSensor(stalledVersionSwapSet.size());
    }
  }

  private boolean didMaxRetriesExceedForStoreFetchInRegion(String regionKafkaTopicName) {
    int attemptedRetries = fetchNonTargetRegionStoreRetryCountMap.merge(regionKafkaTopicName, 1, Integer::sum);

    if (attemptedRetries == MAX_FETCH_STORE_FETCH_RETRY_LIMIT) {
      fetchNonTargetRegionStoreRetryCountMap.remove(regionKafkaTopicName);
      return true;
    }

    return false;
  }

  private String getNextRegionToRollForward(
      Store parentStore,
      int targetVersionNum,
      String clusterName,
      String kafkaTopicName,
      List<String> rolloutOrder) {
    Map<String, Integer> regionToCurrentVersion =
        veniceParentHelixAdmin.getCurrentVersionsForMultiColos(clusterName, parentStore.getName());
    String nextEligibleRegion = null;
    for (String region: rolloutOrder) {
      if (!regionToCurrentVersion.containsKey(region)) {
        continue;
      }

      if (regionToCurrentVersion.get(region) < targetVersionNum) {
        nextEligibleRegion = region;
        break;
      }
    }

    if (nextEligibleRegion == null) {
      LOGGER.info(
          "Marking parent version {} as ONLINE because all regions are serving the target version.",
          kafkaTopicName);
      updateStore(clusterName, parentStore.getName(), ONLINE, targetVersionNum);
      return null;
    }

    StoreResponse storeResponse = getStoreForRegion(clusterName, nextEligibleRegion, parentStore.getName());
    Version version =
        getVersionFromStoreInRegion(nextEligibleRegion, parentStore.getName(), targetVersionNum, storeResponse);
    String regionKafkaTopicName = nextEligibleRegion + "_" + kafkaTopicName;
    if (version == null) {
      boolean maxRetriesExceeded = didMaxRetriesExceedForStoreFetchInRegion(regionKafkaTopicName);
      if (maxRetriesExceeded) {
        VersionStatus finalVersionStatus = rolloutOrder.indexOf(nextEligibleRegion) == 0 ? ERROR : PARTIALLY_ONLINE;
        String message = "Skipping version swap for store: " + parentStore.getName() + " on version: "
            + targetVersionNum + "as the version is not available in region " + nextEligibleRegion
            + " after 5 tries. Marking parent version as " + finalVersionStatus;
        logMessageIfNotRedundant(message);
        updateStore(clusterName, parentStore.getName(), finalVersionStatus, targetVersionNum);
        fetchNonTargetRegionStoreRetryCountMap.remove(regionKafkaTopicName);
      }

      LOGGER.warn(
          "Unable to fetch version {} for store {} in region {}",
          targetVersionNum,
          parentStore.getName(),
          nextEligibleRegion);
      return null;
    }

    if (fetchNonTargetRegionStoreRetryCountMap.containsKey(regionKafkaTopicName)) {
      fetchNonTargetRegionStoreRetryCountMap.remove(regionKafkaTopicName);
    }

    if (VersionStatus.PUSHED.equals(version.getStatus())) {
      String message =
          "Found next eligible region to roll forward in: " + nextEligibleRegion + " for topic: " + kafkaTopicName;
      logMessageIfNotRedundant(message);
      return nextEligibleRegion;
    } else if (VersionStatus.ERROR.equals(version.getStatus()) || VersionStatus.KILLED.equals(version.getStatus())) {
      VersionStatus finalVersionStatus = rolloutOrder.indexOf(nextEligibleRegion) == 0 ? ERROR : PARTIALLY_ONLINE;
      updateStore(clusterName, parentStore.getName(), finalVersionStatus, targetVersionNum);
      LOGGER.info(
          "Updating parent version status to {} as {} as version failed in region: {}",
          kafkaTopicName,
          finalVersionStatus,
          nextEligibleRegion);
      return null;
    } else if (VersionStatus.ONLINE.equals(version.getStatus())
        && rolloutOrder.get(rolloutOrder.size() - 1).equals(nextEligibleRegion)) {
      String message = "Continuing to roll forward" + kafkaTopicName + " because region " + nextEligibleRegion
          + " is ONLINE, " + "but the current version" + regionToCurrentVersion.get(nextEligibleRegion)
          + " is not the target version " + targetVersionNum;
      logMessageIfNotRedundant(message);
      return nextEligibleRegion;
    }

    return null;
  }

  private boolean didWaitTimePassInRegionForSequentialRollout(
      Store parentStore,
      long latestVersionPromoteToCurrentTimestamp) {
    long storeWaitTime = TimeUnit.MINUTES.toMillis(parentStore.getTargetSwapRegionWaitTime());
    long currentTime = System.currentTimeMillis();
    if (storeWaitTime + latestVersionPromoteToCurrentTimestamp > currentTime) {
      return false;
    }

    return true;
  }

  private boolean isRegionReadyForRollout(
      String previousEligibleRegion,
      Store parentStore,
      String clusterName,
      String kafkaTopicName) {
    StoreResponse storeResponse = getStoreForRegion(clusterName, previousEligibleRegion, parentStore.getName());
    if (storeResponse == null) {
      return false;
    }

    StoreInfo storeInfo = storeResponse.getStore();
    long latestVersionPromoteToCurrentTimestamp = storeInfo.getLatestVersionPromoteToCurrentTimestamp();
    if (!didWaitTimePassInRegionForSequentialRollout(parentStore, latestVersionPromoteToCurrentTimestamp)) {
      String message =
          "Wait time has not passed for store: " + parentStore.getName() + " in region: " + previousEligibleRegion
              + " latestVersionPromoteToCurrentTimestamp " + latestVersionPromoteToCurrentTimestamp;
      logMessageIfNotRedundant(message);
      storeWaitTimeCacheForSequentialRollout.put(kafkaTopicName, latestVersionPromoteToCurrentTimestamp);
      return false;
    }

    return true;
  }

  /**
   * Gets a list of eligible regions to roll forward in. A region is eligible to be rolled forward if it's push status is
   * COMPLETED. If there are no eligible regions to roll forward in or if not all regions have reached a terminal status, null is
   * returned and the version status is marked as PARTIALLY_ONLINE as only the target regions are serving traffic from the new version
   * @param nonTargetRegions list of regions to check eligibility for
   * @param parentStore store to update
   * @param targetVersionNum target version to roll forward in
   * @param clusterName cluster the store is in
   * @return
   */
  private Set<String> getRegionsToRollForward(
      Set<String> nonTargetRegions,
      Store parentStore,
      int targetVersionNum,
      String clusterName,
      String kafkaTopicName) {

    Set<String> completedNonTargetRegions = new HashSet<>();
    Set<String> failedNonTargetRegions = new HashSet<>();
    Map<String, String> nonTargetRegionToStatus = new HashMap<>();
    Set<String> onlineNonTargetRegions = new HashSet<>();
    for (String nonTargetRegion: nonTargetRegions) {
      StoreResponse storeResponse = getStoreForRegion(clusterName, nonTargetRegion, parentStore.getName());
      Version version =
          getVersionFromStoreInRegion(nonTargetRegion, parentStore.getName(), targetVersionNum, storeResponse);

      // When a push is killed or errored out, the topic may have been cleaned up or controller is temporarily
      // unreachable so we will allow upto 5 retries before marking it as failed
      String regionKafkaTopicName = nonTargetRegion + "_" + kafkaTopicName;
      if (version == null) {
        if (didMaxRetriesExceedForStoreFetchInRegion(regionKafkaTopicName)) {
          failedNonTargetRegions.add(nonTargetRegion);
          fetchNonTargetRegionStoreRetryCountMap.remove(regionKafkaTopicName);
        }

        continue;
      }

      if (fetchNonTargetRegionStoreRetryCountMap.containsKey(regionKafkaTopicName)) {
        fetchNonTargetRegionStoreRetryCountMap.remove(regionKafkaTopicName);
      }

      if (version.getStatus().equals(VersionStatus.PUSHED)) {
        completedNonTargetRegions.add(nonTargetRegion);
      } else if (version.getStatus().equals(ERROR) || version.getStatus().equals(VersionStatus.KILLED)) {
        failedNonTargetRegions.add(nonTargetRegion);
      } else if (version.getStatus().equals(ONLINE)) {
        // The in memory store map is out of sync, and we should still allow roll forward to happen
        // to make the future version current
        StoreInfo childStore = storeResponse.getStore();
        if (childStore.getCurrentVersion() < targetVersionNum) {
          completedNonTargetRegions.add(nonTargetRegion);
          String message = "Child version " + targetVersionNum + " status is ONLINE while the current version is "
              + childStore.getCurrentVersion() + " for store " + parentStore.getName() + " in region "
              + nonTargetRegion;
          logMessageIfNotRedundant(message);
          deferredVersionSwapStats.recordDeferredVersionSwapChildStatusMismatchSensor();
        } else {
          onlineNonTargetRegions.add(nonTargetRegion);
        }
      }

      nonTargetRegionToStatus.put(nonTargetRegion, version.getStatus().toString());
    }

    if (failedNonTargetRegions.equals(nonTargetRegions)) {
      String message = "Skipping version swap for store: " + parentStore.getName() + " on version: " + targetVersionNum
          + "as push failed in all non target regions. Failed non target regions: " + failedNonTargetRegions
          + " non target regions: " + nonTargetRegionToStatus;
      logMessageIfNotRedundant(message);
      updateStore(clusterName, parentStore.getName(), PARTIALLY_ONLINE, targetVersionNum);

      return Collections.emptySet();
    } else if ((failedNonTargetRegions.size() + completedNonTargetRegions.size()
        + onlineNonTargetRegions.size()) != nonTargetRegions.size()) {
      String message = "Skipping version swap for store: " + parentStore.getName() + " on version: " + targetVersionNum
          + "as push is not in terminal status in all non target regions. Completed non target regions: "
          + completedNonTargetRegions + ", failed non target regions: " + failedNonTargetRegions
          + ", non target regions: " + nonTargetRegionToStatus;
      logMessageIfNotRedundant(message);
      return Collections.emptySet();
    } else if ((onlineNonTargetRegions.equals(nonTargetRegions))) {
      String message = "Marking parent version " + kafkaTopicName + " as ONLINE because all non target "
          + "regions are serving the target version: " + nonTargetRegionToStatus;
      logMessageIfNotRedundant(message);
      updateStore(clusterName, parentStore.getName(), ONLINE, targetVersionNum);

      return Collections.emptySet();
    }

    return completedNonTargetRegions;
  }

  private boolean didPostVersionSwapValidationsPass(
      Store parentStore,
      int targetVersionNum,
      String clusterName,
      String targetRegion,
      String kafkaTopicName) {
    boolean proceed = true;
    List<LifecycleHooksRecord> storeLifecycleHooks = parentStore.getStoreLifecycleHooks();
    for (LifecycleHooksRecord lifecycleHooksRecord: storeLifecycleHooks) {
      StoreVersionLifecycleEventOutcome outcome;

      if (!storeLifecycleHooksCache.containsKey(lifecycleHooksRecord.getStoreLifecycleHooksClassName())) {
        try {
          StoreLifecycleHooks storeLifecycleHook = ReflectUtils.callConstructor(
              ReflectUtils.loadClass(lifecycleHooksRecord.getStoreLifecycleHooksClassName()),
              new Class<?>[] { VeniceProperties.class },
              new Object[] { veniceControllerMultiClusterConfig.getCommonConfig().getProps() });

          storeLifecycleHooksCache.put(lifecycleHooksRecord.getStoreLifecycleHooksClassName(), storeLifecycleHook);
        } catch (Exception e) {
          String message = "Encountered exception while executing lifecycle hook: "
              + lifecycleHooksRecord.getStoreLifecycleHooksClassName() + " for store: " + parentStore.getName()
              + " on version: " + targetVersionNum + ". Exception: " + e;
          logMessageIfNotRedundant(message);
          continue;
        }
      }

      StoreLifecycleHooks storeLifecycleHook =
          storeLifecycleHooksCache.get(lifecycleHooksRecord.getStoreLifecycleHooksClassName());
      Properties properties = new Properties();
      properties.putAll(lifecycleHooksRecord.getStoreLifecycleHooksParams());
      VeniceProperties veniceProperties = new VeniceProperties(properties);
      outcome = storeLifecycleHook.postStoreVersionSwap(
          clusterName,
          parentStore.getName(),
          targetVersionNum,
          targetRegion,
          null,
          veniceProperties);
      String outcomeMessage = "Validation outcome for store " + parentStore.getName() + " on version "
          + targetVersionNum + " in region" + targetRegion + "with hook "
          + lifecycleHooksRecord.getStoreLifecycleHooksClassName() + " is: " + proceed;
      logMessageIfNotRedundant(outcomeMessage);

      if (StoreVersionLifecycleEventOutcome.WAIT.equals(outcome)) {
        String message = "Skipping version swap for store: " + parentStore.getName() + " on version: "
            + targetVersionNum + " as post version swap validations emitted WAIT";
        logMessageIfNotRedundant(message);
      } else if (StoreVersionLifecycleEventOutcome.ROLLBACK.equals(outcome)
          || StoreVersionLifecycleEventOutcome.ABORT.equals(outcome)) {
        String message = "Skipping version swap for store: " + parentStore.getName() + " on version: "
            + targetVersionNum + "as post version swap validations emitted " + outcome;
        logMessageIfNotRedundant(message);

        veniceParentHelixAdmin.rollbackToBackupVersion(clusterName, parentStore.getName(), targetRegion);
        updateStore(clusterName, parentStore.getName(), ERROR, targetVersionNum);
        LOGGER.info(
            "Updating store status to ERROR for store: " + parentStore.getName() + " on version: " + targetVersionNum);
      }

      if (!StoreVersionLifecycleEventOutcome.PROCEED.equals(outcome)) {
        proceed = false;
      }
    }

    String message = "Validation outcome for store " + parentStore.getName() + " on version " + targetVersionNum
        + " in region" + targetRegion + " is proceed: " + proceed;
    logMessageIfNotRedundant(message);
    return proceed;
  }

  private void handleFailedRollForward(
      int targetVersionNum,
      Store parentStore,
      String kafkaTopicName,
      String region,
      String clusterName) {
    int attemptedRetries = failedRollforwardRetryCountMap.compute(kafkaTopicName, (k, v) -> {
      if (v == null) {
        return 1;
      }
      return v + 1;
    });

    if (attemptedRetries == MAX_ROLL_FORWARD_RETRY_LIMIT) {
      deferredVersionSwapStats.recordDeferredVersionSwapFailedRollForwardSensor();
      updateStore(clusterName, parentStore.getName(), PARTIALLY_ONLINE, targetVersionNum);
      failedRollforwardRetryCountMap.remove(kafkaTopicName);
      LOGGER.info(
          "Updated parent version status to PARTIALLY_ONLINE for version: {} in store: {} after failing to roll forward in non target regions: {}",
          targetVersionNum,
          parentStore.getName(),
          region);
    }
  }

  private boolean isTargetRegionPushWithDeferredSwapEnabled(Store parentStore) {
    int targetVersionNum = parentStore.getLargestUsedVersionNumber();
    if (targetVersionNum < 1) {
      return false;
    }

    Version targetVersion = parentStore.getVersion(targetVersionNum);
    if (targetVersion == null) {
      String message =
          "Parent version is null for store " + parentStore.getName() + " for target version " + targetVersionNum;
      logMessageIfNotRedundant(message);
      return false;
    }

    if (targetVersion.isVersionSwapDeferred() && StringUtils.isNotEmpty(targetVersion.getTargetSwapRegion())) {
      return true;
    }

    return false;
  }

  /**
   * Attempts to mark a store as being processed. Returns true if successful (store wasn't already being processed),
   * false if the store is already being processed by another thread.
   */
  private boolean tryStartProcessingStore(String kafkaTopicName) {
    return storesBeingProcessed.add(kafkaTopicName);
  }

  /**
   * Marks a store as no longer being processed.
   */
  private void finishProcessingStore(String kafkaTopicName) {
    storesBeingProcessed.remove(kafkaTopicName);
  }

  private Runnable getRunnableForDeferredVersionSwap() {
    return () -> {
      LogContext.setLogContext(veniceControllerMultiClusterConfig.getLogContext());
      if (stop.get()) {
        return;
      }

      try {
        for (String cluster: veniceParentHelixAdmin.getClustersLeaderOf()) {
          if (!veniceParentHelixAdmin.isLeaderControllerFor(cluster)) {
            continue;
          }

          List<Store> parentStores;
          Map<String, ControllerClient> childControllerClientMap =
              veniceParentHelixAdmin.getVeniceHelixAdmin().getControllerClientMap(cluster);
          String rolloutOrderStr = veniceControllerMultiClusterConfig.getControllerConfig(cluster)
              .getDeferredVersionSwapRegionRollforwardOrder();
          List<String> rolloutOrder = RegionUtils.parseRegionRolloutOrderList(rolloutOrderStr);
          validateRolloutRegions(cluster, rolloutOrder, childControllerClientMap.keySet());

          try {
            parentStores = veniceParentHelixAdmin.getAllStores(cluster);
          } catch (VeniceNoClusterException e) {
            LOGGER.warn("Leadership changed during getAllStores call for cluster: {}", cluster, e);
            break;
          }

          ThreadPoolExecutor clusterExecutorService = getOrCreateExecutorForCluster(cluster);
          ThreadPoolStats clusterThreadPoolStats = clusterToThreadPoolStatsMap.get(cluster);

          // Filter out stores that aren't doing a target region push w/ deferred swap
          List<Store> eligibleStoresToProcess = new ArrayList<>();
          for (Store parentStore: parentStores) {
            if (!isTargetRegionPushWithDeferredSwapEnabled(parentStore)) {
              continue;
            }
            eligibleStoresToProcess.add(parentStore);
          }

          boolean sequentialRollForward = !StringUtils.isEmpty(rolloutOrderStr);
          for (Store parentStore: eligibleStoresToProcess) {
            Version targetVersion = parentStore.getVersion(parentStore.getLargestUsedVersionNumber());

            // Check if store is already being processed
            String kafkaTopicName =
                Version.composeKafkaTopic(parentStore.getName(), parentStore.getLargestUsedVersionNumber());
            if (!tryStartProcessingStore(kafkaTopicName)) {
              String message = "Skipping store " + parentStore.getName() + " as it's already being processed";
              logMessageIfNotRedundant(message);
              continue;
            }

            clusterExecutorService.submit(() -> {
              try {
                if (sequentialRollForward) {
                  performSequentialRollForward(
                      cluster,
                      parentStore,
                      childControllerClientMap,
                      rolloutOrder,
                      targetVersion);
                } else {
                  performParallelRollForward(cluster, parentStore, childControllerClientMap, targetVersion);
                }
              } finally {
                finishProcessingStore(Version.composeKafkaTopic(parentStore.getName(), targetVersion.getNumber()));
              }
            });
          }
          clusterThreadPoolStats.recordQueuedTasksCount(clusterExecutorService.getQueue().size());
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while performing deferred version swap", e);
        deferredVersionSwapStats.recordDeferredVersionSwapErrorSensor();
      } catch (Throwable throwable) {
        LOGGER.warn("Caught a throwable while performing deferred version swap", throwable);
        deferredVersionSwapStats.recordDeferredVersionSwapThrowableSensor();
      }
    };
  }

  private void performSequentialRollForward(
      String cluster,
      Store parentStore,
      Map<String, ControllerClient> childControllerClientMap,
      List<String> rolloutOrder,
      Version targetVersion) {
    int targetVersionNum = targetVersion.getNumber();
    String storeName = parentStore.getName();
    String targetRegion = rolloutOrder.get(0);
    Set<String> remainingRegions = getRegionsForVersionSwap(childControllerClientMap, targetRegion);

    // Check if the target version is in a terminal state (push job completed or failed)
    long startTime = System.currentTimeMillis();
    if (!isPushInTerminalState(
        targetVersion,
        cluster,
        parentStore.getName(),
        targetVersionNum,
        remainingRegions,
        targetRegion)) {
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    // Check if the cached waitTime for the target version has elapsed
    String kafkaTopicName = Version.composeKafkaTopic(storeName, targetVersionNum);
    Long cachedWaitTime = storeWaitTimeCacheForSequentialRollout.getIfPresent(kafkaTopicName);
    if (cachedWaitTime != null) {
      if (!didWaitTimePassInRegionForSequentialRollout(parentStore, cachedWaitTime)) {
        String message =
            "Cached wait time has not elapsed for store: " + parentStore.getName() + " on version: " + targetVersionNum;
        logMessageIfNotRedundant(message);
        logLatency(startTime, storeName, targetVersionNum);
        return;
      }
    }

    Admin.OfflinePushStatusInfo pushStatusInfo = veniceParentHelixAdmin.getOffLinePushStatus(cluster, kafkaTopicName);
    if (!didPushCompleteInTargetRegions(targetRegion, pushStatusInfo, parentStore, targetVersionNum, cluster)) {
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    // Find next region to roll forward
    String nextRegionToRollForward =
        getNextRegionToRollForward(parentStore, targetVersionNum, cluster, kafkaTopicName, rolloutOrder);
    if (nextRegionToRollForward == null) {
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    int nextEligibleRegionIndex = rolloutOrder.indexOf(nextRegionToRollForward);
    // Check that the wait time elapsed in the prior region that rolled forward if it is not the first region
    // to roll forward
    if (nextEligibleRegionIndex != 0) {
      int priorRolledForwardRegionIndex = rolloutOrder.indexOf(nextRegionToRollForward) - 1;

      String priorRegionRolledForward = rolloutOrder.get(priorRolledForwardRegionIndex);
      String message2 = "Found prior region that rolled forward: " + priorRegionRolledForward + " for store: "
          + parentStore.getName() + " for version: " + targetVersionNum;
      logMessageIfNotRedundant(message2);

      if (!isRegionReadyForRollout(priorRegionRolledForward, parentStore, cluster, kafkaTopicName)) {
        logLatency(startTime, storeName, targetVersionNum);
        return;
      }

      emitMetricIfVersionSwapIfStalledForSequentialRollout(
          nextRegionToRollForward,
          parentStore,
          targetVersionNum,
          cluster);

      if (!didPostVersionSwapValidationsPass(
          parentStore,
          targetVersionNum,
          cluster,
          priorRegionRolledForward,
          kafkaTopicName)) {
        logLatency(startTime, storeName, targetVersionNum);
        return;
      }
    }

    try {
      LOGGER.info(
          "Issuing roll forward for store: {} in region: {} for version: {}",
          storeName,
          nextRegionToRollForward,
          targetVersionNum);
      veniceParentHelixAdmin.rollForwardToFutureVersion(cluster, parentStore.getName(), nextRegionToRollForward);
      storeWaitTimeCacheForSequentialRollout.invalidate(kafkaTopicName);

      long totalVersionSwapTimeInMinutes =
          TimeUnit.MILLISECONDS.toMinutes(LatencyUtils.getElapsedTimeFromMsToMs(targetVersion.getCreatedTime()));
      LOGGER.info(
          "Version swap took {} minutes from push completion to version swap for {} on version {} in region {}",
          totalVersionSwapTimeInMinutes,
          storeName,
          targetVersionNum,
          nextRegionToRollForward);

      if (stalledVersionSwapSet.contains(parentStore.getName())) {
        stalledVersionSwapSet.remove(parentStore.getName());
      }

      if (rolloutOrder.get(rolloutOrder.size() - 1).equals(nextRegionToRollForward)) {
        updateStore(cluster, storeName, VersionStatus.ONLINE, targetVersionNum);

        LOGGER.info(
            "Updated parent version status to ONLINE for version: {} in store: {} as all regions have been rolled forward",
            targetVersionNum,
            storeName);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to roll forward for store: {} in version: {}", storeName, targetVersionNum, e);
      handleFailedRollForward(targetVersionNum, parentStore, kafkaTopicName, nextRegionToRollForward, cluster);
    }
    logLatency(startTime, storeName, targetVersionNum);
  }

  private void performParallelRollForward(
      String cluster,
      Store parentStore,
      Map<String, ControllerClient> childControllerClientMap,
      Version targetVersion) {
    int targetVersionNum = targetVersion.getNumber();
    String storeName = parentStore.getName();
    String targetRegion = RegionUtils.parseRegionRolloutOrderList(targetVersion.getTargetSwapRegion()).get(0);
    Set<String> remainingRegions = getRegionsForVersionSwap(childControllerClientMap, targetRegion);

    // Check if the target version is in a terminal state (push job completed or failed)
    long startTime = System.currentTimeMillis();
    if (!isPushInTerminalState(
        targetVersion,
        cluster,
        parentStore.getName(),
        targetVersionNum,
        remainingRegions,
        targetRegion)) {
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    // Check if the cached waitTime for the target version has elapsed
    String kafkaTopicName = Version.composeKafkaTopic(storeName, targetVersionNum);
    if (!didCachedWaitTimeElapseInTargetRegions(targetRegion, parentStore, targetVersionNum, kafkaTopicName)) {
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    Admin.OfflinePushStatusInfo pushStatusInfo = veniceParentHelixAdmin.getOffLinePushStatus(cluster, kafkaTopicName);
    if (!didPushCompleteInTargetRegions(targetRegion, pushStatusInfo, parentStore, targetVersionNum, cluster)) {
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    // Get eligible non target regions to roll forward in
    Set<String> nonTargetRegionsCompleted =
        getRegionsToRollForward(remainingRegions, parentStore, targetVersionNum, cluster, kafkaTopicName);
    if (nonTargetRegionsCompleted.isEmpty()) {
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    // Check that waitTime has elapsed in target regions
    if (!didWaitTimeElapseInTargetRegions(pushStatusInfo.getExtraInfoUpdateTimestamp(), targetRegion, parentStore)) {
      storePushCompletionTimeCache.put(kafkaTopicName, pushStatusInfo.getExtraInfoUpdateTimestamp());
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    // Check if version swap is stalled for the store
    emitMetricIfVersionSwapIsStalled(
        pushStatusInfo.getExtraInfoUpdateTimestamp(),
        targetRegion,
        parentStore,
        targetVersionNum,
        targetVersion);

    if (!didPostVersionSwapValidationsPass(
        parentStore,
        targetVersionNum,
        cluster,
        targetVersion.getTargetSwapRegion(),
        kafkaTopicName)) {
      logLatency(startTime, storeName, targetVersionNum);
      return;
    }

    // Switch to the target version in the completed non target regions
    try {
      rollForwardToTargetVersion(nonTargetRegionsCompleted, parentStore, targetVersion, cluster);
    } catch (Exception e) {
      LOGGER.warn("Failed to roll forward for store: {} in version: {}", storeName, targetVersionNum, e);
      handleFailedRollForward(
          targetVersionNum,
          parentStore,
          kafkaTopicName,
          nonTargetRegionsCompleted.toString(),
          cluster);
    }
    logLatency(startTime, storeName, targetVersionNum);
  }

  public void updateStore(String clusterName, String storeName, VersionStatus status, int targetVersionNum) {
    HelixVeniceClusterResources resources =
        veniceParentHelixAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
      Store store = repository.getStore(storeName);
      LOGGER.info(
          "Updating store: {} version: {} from status {} to status {}",
          storeName,
          targetVersionNum,
          store.getVersionStatus(targetVersionNum),
          status);
      store.updateVersionStatus(targetVersionNum, status);
      if (status == ONLINE || status == PARTIALLY_ONLINE) {
        store.setCurrentVersion(targetVersionNum);

        // For jobs that stop polling early or for pushes that don't poll (empty push), we need to truncate the parent
        // VT here to unblock the next push
        String kafkaTopicName = Version.composeKafkaTopic(storeName, targetVersionNum);
        if (!veniceParentHelixAdmin.isTopicTruncated(kafkaTopicName)) {
          LOGGER.info("Truncating parent VT for {}", kafkaTopicName);
          veniceParentHelixAdmin.truncateKafkaTopic(Version.composeKafkaTopic(storeName, targetVersionNum));
        }
      }
      repository.updateStore(store);
    } catch (Exception e) {
      LOGGER.warn("Failed to execute updateStore for store: {} in cluster: {}", storeName, clusterName, e);
    }
  }

  // Only used for testing
  Cache<String, Map<String, Long>> getStorePushCompletionTimes() {
    return storePushCompletionTimeCache;
  }

  private void logLatency(long startTime, String storeName, int targetVersionNum) {
    long elapsedTime = LatencyUtils.getElapsedTimeFromMsToMs(startTime);
    if (elapsedTime > LOG_LATENCY_THRESHOLD) {
      String message = "Store " + storeName + " version " + targetVersionNum + " spent "
          + TimeUnit.MILLISECONDS.toSeconds(elapsedTime) + "seconds in the DeferredVersionSwapLoop";
      logMessageIfNotRedundant(message);
    }
  }

  private void validateRolloutRegions(String cluster, List<String> rolloutRegions, Set<String> validRegions) {
    if (rolloutRegions.isEmpty()) {
      return;
    }

    for (String region: rolloutRegions) {
      if (!validRegions.contains(region)) {
        throw new VeniceException(
            "Invalid region " + region + " in cluster " + cluster + " found in rollout order list");
      }
    }
  }
}
