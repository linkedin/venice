package com.linkedin.venice.controller;

import static com.linkedin.venice.meta.VersionStatus.ERROR;
import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static com.linkedin.venice.meta.VersionStatus.PARTIALLY_ONLINE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.controller.lingeringjob.DefaultLingeringStoreVersionChecker;
import com.linkedin.venice.controller.stats.DeferredVersionSwapStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.DegradedDcInfo;
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
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.SystemTime;
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
import java.util.Objects;
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
 *
 * Missing child versions and unavailable child metadata remain pending across scheduled passes. The service interval
 * provides retry backoff without holding a worker between attempts. The persisted version creation time and the store's
 * bootstrap timeout bound recovery, including after a controller restart; cancellation and supersession stop recovery.
 */
public class DeferredVersionSwapService extends AbstractVeniceService {
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private final VeniceParentHelixAdmin veniceParentHelixAdmin;
  private final ScheduledExecutorService deferredVersionSwapExecutor;
  private final DeferredVersionSwapStats deferredVersionSwapStats;
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, TimeUnit.MINUTES.toMillis(10));
  private static final Logger LOGGER = LogManager.getLogger(DeferredVersionSwapService.class);
  private Cache<String, Map<String, Long>> storePushCompletionTimeCache =
      Caffeine.newBuilder().expireAfterWrite(2, TimeUnit.HOURS).build();
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
  private final Time time;

  public DeferredVersionSwapService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig,
      DeferredVersionSwapStats deferredVersionSwapStats,
      MetricsRepository metricsRepository) {
    this(admin, multiClusterConfig, deferredVersionSwapStats, metricsRepository, SystemTime.INSTANCE);
  }

  DeferredVersionSwapService(
      VeniceParentHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig,
      DeferredVersionSwapStats deferredVersionSwapStats,
      MetricsRepository metricsRepository,
      Time time) {
    this.veniceParentHelixAdmin = admin;
    this.veniceControllerMultiClusterConfig = multiClusterConfig;
    this.deferredVersionSwapExecutor = Executors.newSingleThreadScheduledExecutor(
        new DaemonThreadFactory(getClass().getSimpleName(), multiClusterConfig.getLogContext()));
    this.deferredVersionSwapStats = deferredVersionSwapStats;
    this.metricsRepository = metricsRepository;
    this.time = time;
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

      DaemonThreadFactory threadFactory = new DaemonThreadFactory(
          cluster + "-deferred-version-swap",
          veniceControllerMultiClusterConfig.getLogContext());
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

    logMessageIfNotRedundant(
        "Got version " + targetVersionNum + " with status " + version.get().getStatus() + " for store: " + storeName
            + " in region " + region);
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
    String targetRegion = RegionUtils.parseRegionRolloutOrderList(targetVersion.getTargetSwapRegion()).get(0);
    if (!rollForwardIfEligible(cluster, store, targetVersion, regionsToRollForward, targetRegion)) {
      return;
    }

    if (stalledVersionSwapSet.contains(storeName)) {
      stalledVersionSwapSet.remove(storeName);
      deferredVersionSwapStats.recordDeferredVersionSwapStalledVersionSwapMetric(stalledVersionSwapSet.size());
    }

    long totalVersionSwapTimeInMinutes =
        TimeUnit.MILLISECONDS.toMinutes(LatencyUtils.getElapsedTimeFromMsToMs(targetVersion.getCreatedTime()));
    updateStore(cluster, storeName, ONLINE, targetVersion);
    LOGGER.info(
        "Finished deferred swap for version: {} in store: {}."
            + "Version swap took {} minutes from push completion to version swap",
        targetVersionNum,
        storeName,
        totalVersionSwapTimeInMinutes);
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
   * empty and the push job for the current version is completed. Killed versions must never be promoted.
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
        deferredVersionSwapStats.recordDeferredVersionSwapStalledVersionSwapMetric(stalledVersionSwapSet.size());
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
          deferredVersionSwapStats.recordDeferredVersionSwapParentChildStatusMismatchMetric(clusterName, storeName);
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
          deferredVersionSwapStats.recordDeferredVersionSwapParentChildStatusMismatchMetric(clusterName, storeName);
          String message =
              "Parent status is already ONLINE, but version swap has not happened in the non target regions. "
                  + "Continuing with deferred swap for store: " + storeName + " for version: " + targetVersionNum;
          logMessageIfNotRedundant(message);
          return true;
        }
    }

    logMessageIfNotRedundant(
        "Skipping store " + storeName + " as parent version " + targetVersionNum + " status is "
            + targetVersion.getStatus());
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
      updateStore(clusterName, store.getName(), ERROR, store.getVersion(targetVersionNum));
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
      Version parentVersion,
      String clusterName) {
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
      deferredVersionSwapStats.recordDeferredVersionSwapStalledVersionSwapMetric(stalledVersionSwapSet.size());
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
      deferredVersionSwapStats.recordDeferredVersionSwapStalledVersionSwapMetric(stalledVersionSwapSet.size());
    }
  }

  private String getNextRegionToRollForward(
      Store parentStore,
      int targetVersionNum,
      String clusterName,
      String kafkaTopicName,
      List<String> rolloutOrder) {
    Map<String, Integer> regionToCurrentVersion =
        veniceParentHelixAdmin.getCurrentVersionsForMultiColos(clusterName, parentStore.getName());
    if (regionToCurrentVersion.values().stream().anyMatch(version -> version > targetVersionNum)) {
      logMessageIfNotRedundant("Skipping superseded deferred swap for " + kafkaTopicName);
      return null;
    }
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
      updateStore(clusterName, parentStore.getName(), ONLINE, parentStore.getVersion(targetVersionNum));
      return null;
    }

    StoreResponse storeResponse = getStoreForRegion(clusterName, nextEligibleRegion, parentStore.getName());
    Version version =
        getVersionFromStoreInRegion(nextEligibleRegion, parentStore.getName(), targetVersionNum, storeResponse);
    if (version == null) {
      if (isDegradedRegion(clusterName, nextEligibleRegion)) {
        // A degraded region is excluded from the push, so its version will never arrive.
        VersionStatus finalVersionStatus = rolloutOrder.indexOf(nextEligibleRegion) == 0 ? ERROR : PARTIALLY_ONLINE;
        logMessageIfNotRedundant(
            "Skipping version swap for store: " + parentStore.getName() + " on version: " + targetVersionNum
                + " as region " + nextEligibleRegion
                + " is degraded and does not have the version. Marking parent version as " + finalVersionStatus);
        updateStore(clusterName, parentStore.getName(), finalVersionStatus, parentStore.getVersion(targetVersionNum));
        return null;
      }
      LOGGER.warn(
          "Unable to fetch version {} for store {} in region {}",
          targetVersionNum,
          parentStore.getName(),
          nextEligibleRegion);
      return null;
    }

    if (VersionStatus.PUSHED.equals(version.getStatus())) {
      String message =
          "Found next eligible region to roll forward in: " + nextEligibleRegion + " for topic: " + kafkaTopicName;
      logMessageIfNotRedundant(message);
      return nextEligibleRegion;
    } else if (VersionStatus.ERROR.equals(version.getStatus()) || VersionStatus.KILLED.equals(version.getStatus())
        || VersionStatus.ROLLED_BACK.equals(version.getStatus())) {
      VersionStatus finalVersionStatus = rolloutOrder.indexOf(nextEligibleRegion) == 0 ? ERROR : PARTIALLY_ONLINE;
      updateStore(clusterName, parentStore.getName(), finalVersionStatus, parentStore.getVersion(targetVersionNum));
      LOGGER.info(
          "Updating parent version status to {} as {} as version failed in region: {}",
          kafkaTopicName,
          finalVersionStatus,
          nextEligibleRegion);
      return null;
    } else if (VersionStatus.ONLINE.equals(version.getStatus())
        && regionToCurrentVersion.get(nextEligibleRegion) < targetVersionNum) {
      String message = "Continuing to roll forward" + kafkaTopicName + " because region " + nextEligibleRegion
          + " is ONLINE, " + "but the current version" + regionToCurrentVersion.get(nextEligibleRegion)
          + " is not the target version " + targetVersionNum;
      logMessageIfNotRedundant(message);
      return nextEligibleRegion;
    }

    logMessageIfNotRedundant(
        "Did not find next eligble region to roll forward in for " + kafkaTopicName + " for version " + targetVersionNum
            + " and status " + version.getStatus());
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

  private boolean isDegradedRegion(String clusterName, String region) {
    Map<String, DegradedDcInfo> degradedRegions = veniceParentHelixAdmin.getDegradedDatacenters(clusterName);
    return degradedRegions != null && degradedRegions.containsKey(region);
  }

  /**
   * Gets a list of eligible regions to roll forward in. A region is eligible to be rolled forward if it's push status is
   * COMPLETED. Missing versions remain pending until a later pass or the bootstrap deadline, except in degraded regions,
   * which never receive the version and are treated as failed. Explicit failures retain the existing partial-rollout
   * behavior.
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
      if (storeResponse != null && storeResponse.getStore().getCurrentVersion() > targetVersionNum) {
        logMessageIfNotRedundant("Skipping superseded deferred swap for " + kafkaTopicName);
        return Collections.emptySet();
      }
      Version version =
          getVersionFromStoreInRegion(nonTargetRegion, parentStore.getName(), targetVersionNum, storeResponse);
      if (version == null) {
        if (isDegradedRegion(clusterName, nonTargetRegion)) {
          failedNonTargetRegions.add(nonTargetRegion);
        }
        continue;
      }

      if (version.getStatus().equals(VersionStatus.PUSHED)) {
        completedNonTargetRegions.add(nonTargetRegion);
      } else if (version.getStatus().equals(ERROR) || version.getStatus().equals(VersionStatus.KILLED)
          || version.getStatus().equals(VersionStatus.ROLLED_BACK)) {
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
          deferredVersionSwapStats
              .recordDeferredVersionSwapChildStatusMismatchMetric(clusterName, parentStore.getName());
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
      updateStore(clusterName, parentStore.getName(), PARTIALLY_ONLINE, parentStore.getVersion(targetVersionNum));

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
      updateStore(clusterName, parentStore.getName(), ONLINE, parentStore.getVersion(targetVersionNum));

      return Collections.emptySet();
    }

    return completedNonTargetRegions;
  }

  private boolean didPostVersionSwapValidationsPass(
      Store parentStore,
      int targetVersionNum,
      String clusterName,
      String targetRegion,
      String kafkaTopicName,
      List<String> rolloutOrder) {
    boolean proceed = true;
    List<LifecycleHooksRecord> storeLifecycleHooks = parentStore.getStoreLifecycleHooks();
    for (LifecycleHooksRecord lifecycleHooksRecord: storeLifecycleHooks) {
      StoreVersionLifecycleEventOutcome outcome;

      StoreLifecycleHooks storeLifecycleHook = veniceParentHelixAdmin.getVeniceHelixAdmin()
          .getStoreLifecycleHooksCache()
          .getOrInstantiateHook(lifecycleHooksRecord.getStoreLifecycleHooksClassName());
      if (storeLifecycleHook == null) {
        continue;
      }
      Properties properties = new Properties();
      properties.putAll(lifecycleHooksRecord.getStoreLifecycleHooksParams());
      VeniceProperties veniceProperties = new VeniceProperties(properties);
      outcome = storeLifecycleHook.preStoreVersionSwap(
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
        String regionsToRollback = computeRegionsToRollback(targetRegion, rolloutOrder);
        String message = "Skipping version swap for store: " + parentStore.getName() + " on version: "
            + targetVersionNum + " as post version swap validations emitted " + outcome + "; rolling back regions: "
            + regionsToRollback;
        logMessageIfNotRedundant(message);

        veniceParentHelixAdmin.acquireAdminMessageLock(clusterName, parentStore.getName());
        try {
          Version expectedVersion = parentStore.getVersion(targetVersionNum);
          if (getEligibleStore(clusterName, parentStore.getName(), expectedVersion) == null) {
            return false;
          }
          veniceParentHelixAdmin
              .rollbackToBackupVersionForDeferredVersionSwap(clusterName, parentStore.getName(), regionsToRollback);
          updateStore(clusterName, parentStore.getName(), VersionStatus.ROLLED_BACK, expectedVersion);
        } finally {
          veniceParentHelixAdmin.releaseAdminMessageLock(clusterName, parentStore.getName());
        }
        LOGGER.info(
            "Updated store status to ROLLED_BACK for store: {} on version: {}",
            parentStore.getName(),
            targetVersionNum);
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

  /**
   * Returns the comma-separated list of regions to roll back when a post-version-swap validation
   * fails. For the sequential rollout path, this is every region in {@code rolloutOrder} from the
   * first one up to and including {@code targetRegion} — i.e., every region that has rolled
   * forward by this point. For the parallel rollout path (no rolloutOrder), the affected set is
   * just {@code targetRegion}.
   *
   * <p>Rolling back only {@code targetRegion} alone would leave earlier regions on the new version
   * when the lifecycle hook decided the store shouldn't be on the new version anywhere.
   */
  static String computeRegionsToRollback(String targetRegion, List<String> rolloutOrder) {
    if (rolloutOrder == null || rolloutOrder.isEmpty()) {
      return targetRegion;
    }
    int targetRegionIndex = rolloutOrder.indexOf(targetRegion);
    if (targetRegionIndex < 0) {
      // Defensive: targetRegion isn't in rolloutOrder. Roll back only the named region.
      return targetRegion;
    }
    return String.join(",", rolloutOrder.subList(0, targetRegionIndex + 1));
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
      deferredVersionSwapStats.recordDeferredVersionSwapFailedRollForwardMetric(clusterName, parentStore.getName());
      updateStore(clusterName, parentStore.getName(), PARTIALLY_ONLINE, parentStore.getVersion(targetVersionNum));
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
    logMessageIfNotRedundant("Finished processing store " + kafkaTopicName);
    storesBeingProcessed.remove(kafkaTopicName);
  }

  private Runnable getRunnableForDeferredVersionSwap() {
    return () -> {
      LogContext.setLogContext(veniceControllerMultiClusterConfig.getLogContext());
      if (stop.get()) {
        return;
      }

      for (String cluster: veniceParentHelixAdmin.getClustersLeaderOf()) {
        try {
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

          for (Store parentStore: eligibleStoresToProcess) {
            LOGGER.debug("Processing version swap of store: {}", parentStore);
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
                processStore(cluster, parentStore, childControllerClientMap, rolloutOrder, targetVersion);
              } catch (Exception e) {
                LOGGER.warn("Failed deferred version swap for {} in cluster {}", kafkaTopicName, cluster, e);
                deferredVersionSwapStats.recordDeferredVersionSwapExceptionMetric(cluster);
              } finally {
                finishProcessingStore(Version.composeKafkaTopic(parentStore.getName(), targetVersion.getNumber()));
              }
            });
          }
          clusterThreadPoolStats.recordQueuedTasksCount();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while performing deferred version swap for cluster: {}", cluster, e);
          deferredVersionSwapStats.recordDeferredVersionSwapExceptionMetric(cluster);
        } catch (Throwable throwable) {
          LOGGER.warn("Caught a throwable while performing deferred version swap for cluster: {}", cluster, throwable);
          deferredVersionSwapStats.recordDeferredVersionSwapThrowableMetric(cluster);
        }
      }
    };
  }

  void processStore(
      String cluster,
      Store scheduledStore,
      Map<String, ControllerClient> childControllerClientMap,
      List<String> rolloutOrder,
      Version scheduledVersion) {
    String storeName = scheduledStore.getName();
    Store parentStore = getEligibleStore(cluster, storeName, scheduledVersion);
    if (parentStore == null) {
      return;
    }
    Version targetVersion = parentStore.getVersion(scheduledVersion.getNumber());
    if (stopIfBootstrapDeadlineExceeded(cluster, parentStore, targetVersion)) {
      return;
    }
    if (rolloutOrder.isEmpty()) {
      performParallelRollForward(cluster, parentStore, childControllerClientMap, targetVersion);
    } else {
      performSequentialRollForward(cluster, parentStore, childControllerClientMap, rolloutOrder, targetVersion);
    }
  }

  private Store getEligibleStore(String cluster, String storeName, Version expectedVersion) {
    if (stop.get() || !veniceParentHelixAdmin.isLeaderControllerFor(cluster)) {
      return null;
    }
    Store store = veniceParentHelixAdmin.getVeniceHelixAdmin()
        .getHelixVeniceClusterResources(cluster)
        .getStoreMetadataRepository()
        .getStore(storeName);
    if (!isEligibleVersion(store, expectedVersion)) {
      logMessageIfNotRedundant(
          "Stopping deferred swap for " + storeName + " version " + expectedVersion.getNumber()
              + ": version is no longer eligible");
      return null;
    }
    return store;
  }

  private boolean isEligibleVersion(Store store, Version expectedVersion) {
    int versionNumber = expectedVersion.getNumber();
    if (store == null || store.getLargestUsedVersionNumber() != versionNumber
        || store.getCurrentVersion() > versionNumber) {
      return false;
    }
    Version version = store.getVersion(versionNumber);
    return version != null && version.getCreatedTime() == expectedVersion.getCreatedTime()
        && Objects.equals(version.getPushJobId(), expectedVersion.getPushJobId()) && version.isVersionSwapDeferred()
        && StringUtils.isNotEmpty(version.getTargetSwapRegion()) && (version.getStatus() == VersionStatus.STARTED
            || version.getStatus() == VersionStatus.PUSHED || version.getStatus() == ONLINE);
  }

  private boolean stopIfBootstrapDeadlineExceeded(String cluster, Store store, Version version) {
    if (!DefaultLingeringStoreVersionChecker.isBootstrapTimeoutExceeded(store, version, time)) {
      return false;
    }
    // ONLINE is also visited for legacy mismatch recovery; expiration must not downgrade a completed rollout.
    if (version.getStatus() == ONLINE) {
      return true;
    }
    Map<String, Integer> currentVersions =
        veniceParentHelixAdmin.getCurrentVersionsForMultiColos(cluster, store.getName());
    Set<String> regions = veniceParentHelixAdmin.getVeniceHelixAdmin().getControllerClientMap(cluster).keySet();
    boolean allRegionsServing = !regions.isEmpty()
        && regions.stream().allMatch(region -> Objects.equals(currentVersions.get(region), version.getNumber()));
    boolean anyRegionServing = currentVersions.containsValue(version.getNumber()) || version.isTargetRegionPromoted();
    VersionStatus status = allRegionsServing ? ONLINE : anyRegionServing ? PARTIALLY_ONLINE : ERROR;
    LOGGER.info(
        "Deferred swap bootstrap deadline exceeded for store {} version {}; finishing with status {}",
        store.getName(),
        version.getNumber(),
        status);
    updateStore(cluster, store.getName(), status, version);
    return true;
  }

  private boolean isTargetRegionEligible(String cluster, Store store, Version version, String targetRegion) {
    StoreResponse response = getStoreForRegion(cluster, targetRegion, store.getName());
    if (response == null) {
      return false;
    }
    if (response.getStore().getCurrentVersion() > version.getNumber()) {
      logMessageIfNotRedundant(
          "Skipping superseded deferred swap for " + store.getName() + " in target region " + targetRegion);
      return false;
    }
    Version childVersion = getVersionFromStoreInRegion(targetRegion, store.getName(), version.getNumber(), response);
    if (childVersion == null) {
      return false;
    }
    if (childVersion.getStatus() == ERROR || childVersion.getStatus() == VersionStatus.KILLED
        || childVersion.getStatus() == VersionStatus.ROLLED_BACK) {
      LOGGER.info(
          "Stopping deferred swap for store {} version {}: target region {} is {}",
          store.getName(),
          version.getNumber(),
          targetRegion,
          childVersion.getStatus());
      updateStore(cluster, store.getName(), ERROR, version);
      return false;
    }
    return true;
  }

  private boolean rollForwardIfEligible(
      String cluster,
      Store store,
      Version version,
      String regions,
      String targetRegion) {
    veniceParentHelixAdmin.acquireAdminMessageLock(cluster, store.getName());
    try {
      Store latestStore = getEligibleStore(cluster, store.getName(), version);
      if (latestStore == null || !isTargetRegionEligible(cluster, latestStore, version, targetRegion)
          || stopIfBootstrapDeadlineExceeded(cluster, latestStore, version)) {
        return false;
      }
      veniceParentHelixAdmin.rollForwardToFutureVersion(cluster, store.getName(), regions);
      return true;
    } finally {
      veniceParentHelixAdmin.releaseAdminMessageLock(cluster, store.getName());
    }
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
    if (!isTargetRegionEligible(cluster, parentStore, targetVersion, targetRegion)) {
      return;
    }

    if (!targetVersion.isTargetRegionPromoted()) {
      markTargetRegionPromoted(cluster, storeName, targetVersion);
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
          kafkaTopicName,
          rolloutOrder)) {
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
      if (!rollForwardIfEligible(cluster, parentStore, targetVersion, nextRegionToRollForward, targetRegion)) {
        return;
      }
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
        updateStore(cluster, storeName, VersionStatus.ONLINE, targetVersion);

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
    if (!isTargetRegionEligible(cluster, parentStore, targetVersion, targetRegion)) {
      return;
    }

    if (!targetVersion.isTargetRegionPromoted()) {
      markTargetRegionPromoted(cluster, storeName, targetVersion);
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
        targetVersion,
        cluster);

    if (!didPostVersionSwapValidationsPass(
        parentStore,
        targetVersionNum,
        cluster,
        targetVersion.getTargetSwapRegion(),
        kafkaTopicName,
        Collections.emptyList())) {
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

  void updateStore(String clusterName, String storeName, VersionStatus status, Version expectedVersion) {
    int targetVersionNum = expectedVersion.getNumber();
    HelixVeniceClusterResources resources =
        veniceParentHelixAdmin.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
      Store store = repository.getStore(storeName);
      if (stop.get() || !veniceParentHelixAdmin.isLeaderControllerFor(clusterName)
          || !isEligibleVersion(store, expectedVersion)) {
        logMessageIfNotRedundant(
            "Not updating ineligible deferred swap for " + storeName + " version " + targetVersionNum);
        return;
      }
      // A pass working from a stale PUSHED snapshot can race with completion; never downgrade a live ONLINE version.
      if (status != ONLINE && store.getVersionStatus(targetVersionNum) == ONLINE) {
        return;
      }
      LOGGER.info(
          "Updating store: {} version: {} from status {} to status {}",
          storeName,
          targetVersionNum,
          store.getVersionStatus(targetVersionNum),
          status);
      store.updateVersionStatus(targetVersionNum, status);
      if (status == ONLINE || status == PARTIALLY_ONLINE) {
        store.setCurrentVersion(targetVersionNum);
      }
      repository.updateStore(store);
    } catch (Exception e) {
      LOGGER.warn("Failed to execute updateStore for store: {} in cluster: {}", storeName, clusterName, e);
    }
  }

  private void markTargetRegionPromoted(String clusterName, String storeName, Version targetVersion) {
    int targetVersionNum = targetVersion.getNumber();
    veniceParentHelixAdmin.acquireAdminMessageLock(clusterName, storeName);
    try {
      if (getEligibleStore(clusterName, storeName, targetVersion) == null) {
        return;
      }
      LOGGER.info("Marking targetRegionPromoted=true for store: {} version: {}", storeName, targetVersionNum);
      veniceParentHelixAdmin
          .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setTargetRegionPromoted(true));
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to mark targetRegionPromoted for store: {} version: {} in cluster: {}",
          storeName,
          targetVersionNum,
          clusterName,
          e);
    } finally {
      veniceParentHelixAdmin.releaseAdminMessageLock(clusterName, storeName);
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
