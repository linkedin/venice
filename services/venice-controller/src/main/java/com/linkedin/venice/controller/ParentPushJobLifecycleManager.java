package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.VeniceHelixAdmin.VERSION_ID_UNSET;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.meta.VersionStatus.CREATED;
import static com.linkedin.venice.meta.VersionStatus.ERROR;
import static com.linkedin.venice.meta.VersionStatus.KILLED;
import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static com.linkedin.venice.meta.VersionStatus.PARTIALLY_ONLINE;
import static com.linkedin.venice.meta.VersionStatus.PUSHED;
import static com.linkedin.venice.meta.VersionStatus.STARTED;
import static com.linkedin.venice.utils.RegionUtils.parseRegionsFilterList;
import static java.lang.Thread.currentThread;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin.OfflinePushStatusInfo;
import com.linkedin.venice.controller.lingeringjob.LingeringStoreVersionChecker;
import com.linkedin.venice.controller.stats.DegradedModeStats;
import com.linkedin.venice.controller.versionlifecycle.VersionLifecyclePolicy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.ConcurrentBatchPushException;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.ConcurrentPushDetectionStrategy;
import com.linkedin.venice.meta.DegradedDcInfo;
import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Owns parent-controller push-job lifecycle orchestration that used to live directly in
 * {@link VeniceParentHelixAdmin}: detecting in-flight pushes, validating whether a new push may
 * start, aggregating child-region push status, and handling parent-side terminal push cleanup.
 *
 * <p>This first extraction intentionally keeps {@link VeniceParentHelixAdmin} as the collaborator
 * so behavior stays mechanical and API-compatible.</p>
 */
class ParentPushJobLifecycleManager {
  private static final Logger LOGGER = LogManager.getLogger(ParentPushJobLifecycleManager.class);
  private static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];
  private static final long TOPIC_DELETION_DELAY_MS = TimeUnit.MINUTES.toMillis(5);
  private static final Set<VersionStatus> TERMINAL_VERSION_SWAP_STATUSES =
      Utils.setOf(ONLINE, PARTIALLY_ONLINE, KILLED, ERROR);

  private final VeniceParentHelixAdmin parent;

  ParentPushJobLifecycleManager(VeniceParentHelixAdmin parent) {
    this.parent = parent;
  }

  Optional<String> getTopicForCurrentPushJob(
      String clusterName,
      String storeName,
      boolean isIncrementalPush,
      boolean isRepush) {
    VeniceControllerClusterConfig controllerConfig =
        parent.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig();
    ConcurrentPushDetectionStrategy pushDetectionStrategy = controllerConfig.getConcurrentPushDetectionStrategy();
    if (ConcurrentPushDetectionStrategy.TOPIC_BASED_ONLY.equals(pushDetectionStrategy)) {
      return getTopicForCurrentPushJobTopicBasedTracking(clusterName, storeName, isIncrementalPush, isRepush);
    } else if (ConcurrentPushDetectionStrategy.DUAL.equals(pushDetectionStrategy)) {
      Optional<String> topicBased =
          getTopicForCurrentPushJobTopicBasedTracking(clusterName, storeName, isIncrementalPush, isRepush);
      Optional<String> versionStatusBased =
          getTopicForCurrentPushJobParentVersionStatusBasedTracking(clusterName, storeName);
      if (!topicBased.equals(versionStatusBased)) {
        LOGGER.error(
            "getTopicForCurrentPushJob returns different value for store {} in cluster {}, topicBased: {}, versionStatusBased: {}",
            storeName,
            clusterName,
            topicBased,
            versionStatusBased);
      }
      return topicBased;
    } else {
      return getTopicForCurrentPushJobParentVersionStatusBasedTracking(clusterName, storeName);
    }
  }

  List<PubSubTopic> existingVersionTopicsForStore(String storeName) {
    List<PubSubTopic> outputList = new ArrayList<>();
    TopicManager topicManager = parent.getTopicManager();
    Set<PubSubTopic> topics = topicManager.listTopics();
    String storeNameForCurrentTopic;
    for (PubSubTopic topic: topics) {
      if (com.linkedin.venice.controller.kafka.AdminTopicUtils.isAdminTopic(topic.getName())
          || com.linkedin.venice.controller.kafka.AdminTopicUtils.isKafkaInternalTopic(topic.getName())
          || topic.isRealTime() || com.linkedin.venice.views.VeniceView.isViewTopic(topic.getName())) {
        continue;
      }
      try {
        storeNameForCurrentTopic = Version.parseStoreFromKafkaTopicName(topic.getName());
      } catch (Exception e) {
        LOGGER.debug("Failed to parse StoreName from topic: {}, and error message: {}", topic, e.getMessage());
        continue;
      }
      if (storeNameForCurrentTopic.equals(storeName)) {
        outputList.add(topic);
      }
    }
    return outputList;
  }

  List<PubSubTopic> getKafkaTopicsByAge(String storeName) {
    List<PubSubTopic> existingTopics = existingVersionTopicsForStore(storeName);
    if (!existingTopics.isEmpty()) {
      existingTopics.sort((t1, t2) -> {
        int v1 = Version.parseVersionFromKafkaTopicName(t1.getName());
        int v2 = Version.parseVersionFromKafkaTopicName(t2.getName());
        return v2 - v1;
      });
    }
    return existingTopics;
  }

  Optional<String> getTopicForCurrentPushJobTopicBasedTracking(
      String clusterName,
      String storeName,
      boolean isIncrementalPush,
      boolean isRepush) {
    List<PubSubTopic> versionTopics = getKafkaTopicsByAge(storeName);
    Optional<PubSubTopic> latestTopic = Optional.empty();
    if (!versionTopics.isEmpty()) {
      latestTopic = Optional.of(versionTopics.get(0));
    }

    if (latestTopic.isPresent()) {
      LOGGER.debug("Latest kafka topic for store: {} is {}", storeName, latestTopic.get());
      final String latestTopicName = latestTopic.get().getName();
      int versionNumber = Version.parseVersionFromKafkaTopicName(latestTopicName);
      Store store = parent.getStore(clusterName, storeName);
      Version version = store.getVersion(versionNumber);
      boolean onlyDeferredSwap = version.isVersionSwapDeferred() && StringUtils.isEmpty(version.getTargetSwapRegion());
      boolean isTargetRegionPushWithDeferredSwap =
          version != null && version.isVersionSwapDeferred() && StringUtils.isNotEmpty(version.getTargetSwapRegion());

      if (onlyDeferredSwap) {
        if (version.getStatus() == STARTED || version.getStatus() == PUSHED) {
          LOGGER.error(
              "Future version {} exists for store {}, please wait till the future version is made current.",
              versionNumber,
              storeName);
          return Optional.of(latestTopic.get().getName());
        } else if (version.getStatus() == ONLINE) {
          boolean validatedChildVersion = validateChildCurrentVersions(clusterName, storeName, versionNumber);
          if (!validatedChildVersion) {
            return Optional.of(latestTopic.get().getName());
          }
        }
      } else if (isTargetRegionPushWithDeferredSwap) {
        LOGGER.error(
            "Future version {} exists for store {}, please wait till the future version is made current.",
            versionNumber,
            storeName);
        return Optional.of(latestTopic.get().getName());
      }

      if (!parent.isTopicTruncated(latestTopicName)) {
        StoreVersionInfo storeVersionPair =
            parent.getVeniceHelixAdmin().waitVersion(clusterName, storeName, versionNumber, Duration.ofSeconds(30));
        if (storeVersionPair.getVersion() == null) {
          Long inMemoryTopicCreationTime = parent.getVeniceHelixAdmin().getInMemoryTopicCreationTime(latestTopicName);
          if (inMemoryTopicCreationTime != null && com.linkedin.venice.utils.SystemTime.INSTANCE
              .getMilliseconds() < (inMemoryTopicCreationTime + TOPIC_DELETION_DELAY_MS)) {
            throw new VeniceException(
                "Failed to get version information but the topic exists and has been created recently. Try again after some time.");
          }

          parent.killOfflinePush(clusterName, latestTopicName, true);
          LOGGER.info("Found topic: {} without the corresponding version, will kill it", latestTopicName);
          return Optional.empty();
        }

        final long SLEEP_MS_BETWEEN_RETRY = TimeUnit.SECONDS.toMillis(10);
        ExecutionStatus jobStatus = ExecutionStatus.PROGRESS;
        Map<String, String> extraInfo = new HashMap<>();

        int retryTimes = 5;
        int current = 0;
        while (current++ < retryTimes) {
          OfflinePushStatusInfo offlineJobStatus = parent.getOffLinePushStatus(clusterName, latestTopicName);
          jobStatus = offlineJobStatus.getExecutionStatus();
          extraInfo = offlineJobStatus.getExtraInfo();
          if (!extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
            break;
          }
          try {
            getTimer().sleep(SLEEP_MS_BETWEEN_RETRY);
          } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new VeniceException(
                "Received InterruptedException during sleep between 'getOffLinePushStatus' calls");
          }
        }
        if (extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
          LOGGER.error(
              "Failed to get job status for topic: {} after retrying {} times, extra info: {}",
              latestTopicName,
              retryTimes,
              extraInfo);
        }
        if (!jobStatus.isTerminal()) {
          LOGGER.info(
              "Job status: {} for Kafka topic: {} is not terminal, extra info: {}",
              jobStatus,
              latestTopicName,
              extraInfo);
          if (latestTopic.isPresent()) {
            return Optional.of(latestTopic.get().getName());
          }
          return Optional.empty();
        } else {
          if (!isIncrementalPush) {
            Map<String, Integer> currentVersionsMap = parent.getCurrentVersionsForMultiColos(clusterName, storeName);
            truncateTopicsBasedOnMaxErroredTopicNumToKeep(
                versionTopics.stream().map(vt -> vt.getName()).collect(Collectors.toList()),
                isRepush,
                currentVersionsMap);
          }
        }
      }
    }
    return Optional.empty();
  }

  Optional<String> getTopicForCurrentPushJobParentVersionStatusBasedTracking(String clusterName, String storeName) {
    Store store = parent.getStore(clusterName, storeName);
    if (store == null) {
      return Optional.empty();
    }
    int lastVersionNum = store.getLargestUsedVersionNumber();
    Version lastVersion = store.getVersion(lastVersionNum);

    if (lastVersionNum == NON_EXISTING_VERSION || lastVersion == null) {
      LOGGER.info("Store {} does not have any version", storeName);
      return Optional.empty();
    }

    switch (lastVersion.getStatus()) {
      case KILLED:
      case ERROR:
      case ROLLED_BACK:
      case PARTIALLY_ONLINE:
        LOGGER.info(
            "Store {} version {} is in terminal status {} (no ongoing push); allowing the next push to proceed",
            storeName,
            lastVersionNum,
            lastVersion.getStatus());
        return Optional.empty();
      default:
        break;
    }
    LOGGER.info(
        "Found latest version status: {} for store: {}, version: {}",
        lastVersion.getStatus(),
        storeName,
        lastVersionNum);
    Optional<String> latestTopic = Optional.of(Version.composeKafkaTopic(storeName, lastVersionNum));
    boolean onlyDeferredSwap =
        lastVersion.isVersionSwapDeferred() && StringUtils.isEmpty(lastVersion.getTargetSwapRegion());

    if ((lastVersion.getStatus() == STARTED || lastVersion.getStatus() == PUSHED
        || lastVersion.getStatus() == CREATED)) {
      LOGGER.error(
          "The push for version {} of store {} is not completed, please wait till the push is completed.",
          lastVersionNum,
          storeName);
      return latestTopic;
    } else if (onlyDeferredSwap && lastVersion.getStatus() == ONLINE) {
      boolean validateChildCurrentVersions = validateChildCurrentVersions(clusterName, storeName, lastVersionNum);
      if (!validateChildCurrentVersions) {
        return latestTopic;
      }
    }

    final long SLEEP_MS_BETWEEN_RETRY = TimeUnit.SECONDS.toMillis(10);
    ExecutionStatus jobStatus = ExecutionStatus.PROGRESS;
    Map<String, String> extraInfo = new HashMap<>();

    int retryTimes = 5;
    int current = 0;
    while (current++ < retryTimes) {
      OfflinePushStatusInfo offlineJobStatus = parent.getOffLinePushStatus(clusterName, latestTopic.get());
      jobStatus = offlineJobStatus.getExecutionStatus();
      extraInfo = offlineJobStatus.getExtraInfo();
      if (!extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
        break;
      }
      try {
        getTimer().sleep(SLEEP_MS_BETWEEN_RETRY);
      } catch (InterruptedException e) {
        currentThread().interrupt();
        throw new VeniceException("Received InterruptedException during sleep between 'getOffLinePushStatus' calls");
      }
    }
    if (!jobStatus.isTerminal()) {
      LOGGER.info("Job status: {} for {} is not terminal, extra info: {}", jobStatus, latestTopic.get(), extraInfo);
      return latestTopic;
    }
    return Optional.empty();
  }

  private boolean validateChildCurrentVersions(String clusterName, String storeName, int lastVersionNum) {
    Map<String, Integer> currentVersionsMap = parent.getCurrentVersionsForMultiColos(clusterName, storeName);
    for (Map.Entry entry: currentVersionsMap.entrySet()) {
      if (!entry.getValue().equals(lastVersionNum)) {
        LOGGER.error(
            "Future version {} exists for store {}, but in region {} current version {}, please wait till the future version is made current.",
            lastVersionNum,
            storeName,
            entry.getKey(),
            entry.getValue());
        return false;
      }
    }
    return true;
  }

  void truncateTopicsBasedOnMaxErroredTopicNumToKeep(
      List<String> topics,
      boolean isRepush,
      Map<String, Integer> currentVersionsMap) {
    List<String> sortedNonTruncatedTopics =
        topics.stream().filter(topic -> !parent.isTopicTruncated(topic)).sorted((t1, t2) -> {
          int v1 = Version.parseVersionFromKafkaTopicName(t1);
          int v2 = Version.parseVersionFromKafkaTopicName(t2);
          return v1 - v2;
        }).collect(Collectors.toList());
    Set<String> streamReprocessingTopics =
        sortedNonTruncatedTopics.stream().filter(Version::isStreamReprocessingTopic).collect(Collectors.toSet());
    List<String> sortedNonTruncatedVersionTopics = sortedNonTruncatedTopics.stream()
        .filter(topic -> !Version.isStreamReprocessingTopic(topic))
        .collect(Collectors.toList());
    if (sortedNonTruncatedVersionTopics.size() <= parent.maxErroredTopicNumToKeep) {
      LOGGER.info(
          "Non-truncated version topics size: {} isn't bigger than maxErroredTopicNumToKeep: {}, so no topic "
              + "will be truncated this time",
          sortedNonTruncatedTopics.size(),
          parent.maxErroredTopicNumToKeep);
      return;
    }
    int topicNumToTruncate = sortedNonTruncatedVersionTopics.size() - parent.maxErroredTopicNumToKeep;
    int truncatedTopicCnt = 0;
    for (String topic: sortedNonTruncatedVersionTopics) {
      if (isRepush && currentVersionsMap.containsValue(Version.parseVersionFromVersionTopicName(topic))) {
        LOGGER.info(
            "Do not delete the current version topic: {} since the incoming push is a Venice internal re-push.",
            topic);
        continue;
      }
      if (++truncatedTopicCnt > topicNumToTruncate) {
        break;
      }
      parent.truncateKafkaTopic(topic);
      LOGGER.info("Errored topic: {} got truncated", topic);
      String correspondingStreamReprocessingTopic = Version.composeStreamReprocessingTopicFromVersionTopic(topic);
      if (streamReprocessingTopics.contains(correspondingStreamReprocessingTopic)) {
        parent.truncateKafkaTopic(correspondingStreamReprocessingTopic);
        LOGGER.info(
            "Corresponding stream reprocessing topic: {} also got truncated.",
            correspondingStreamReprocessingTopic);
      }
    }
  }

  Version incrementVersionIdempotent(
      String clusterName,
      String storeName,
      String pushJobId,
      int numberOfPartitions,
      int replicationFactor,
      Version.PushType pushType,
      boolean sendStartOfPush,
      boolean sorted,
      String compressionDictionary,
      Optional<String> sourceGridFabric,
      Optional<X509Certificate> requesterCert,
      long rewindTimeInSecondsOverride,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      String targetedRegions,
      int repushSourceVersion,
      int repushTtlSeconds) {
    Store store = parent.getStore(clusterName, storeName);

    if (store != null) {
      IngestionPauseMode pauseMode = store.getIngestionPauseMode();
      if (pauseMode != null && pauseMode != IngestionPauseMode.NOT_PAUSED) {
        List<String> pausedRegions = store.getIngestionPausedRegions();
        String regionInfo =
            (pausedRegions == null || pausedRegions.isEmpty()) ? "all regions" : "regions: " + pausedRegions;
        throw new VeniceHttpException(
            HttpStatus.SC_CONFLICT,
            "Cannot create new version for store " + storeName + " because ingestion is paused (mode=" + pauseMode
                + ", " + regionInfo + "). Resume with: --update-store --store " + storeName
                + " --ingestion-pause-mode NOT_PAUSED",
            ErrorType.BAD_REQUEST);
      }

      if (VeniceSystemStoreType.getSystemStoreType(storeName) == null) {
        VersionLifecyclePolicy.checkRollbackOriginVersionCapacityForNewPush(
            clusterName,
            storeName,
            store,
            parent.getMultiClusterConfigs().getControllerConfig(clusterName).getRolledBackVersionRetentionMs(),
            System.currentTimeMillis());
      }
    }

    Optional<String> currentPushTopic = parent
        .getTopicForCurrentPushJob(clusterName, storeName, pushType.isIncremental(), Version.isPushIdRePush(pushJobId));

    if (currentPushTopic.isPresent()) {
      int currentPushVersion = Version.parseVersionFromKafkaTopicName(currentPushTopic.get());
      Version version = store.getVersion(currentPushVersion);
      if (version == null) {
        throw new VeniceException(
            "A corresponding version should exist with the ongoing push with topic " + currentPushTopic);
      }
      String existingPushJobId = version.getPushJobId();
      if (existingPushJobId.equals(pushJobId)) {
        return version;
      }

      LingeringStoreVersionChecker lingeringStoreVersionChecker = parent.getLingeringStoreVersionChecker();
      if (!version.isVersionSwapDeferred() && lingeringStoreVersionChecker
          .isStoreVersionLingering(store, version, getTimer(), parent, requesterCert, parent.getIdentityParser())) {
        if (pushType.isIncremental()) {
          throw new VeniceException(
              "Version " + version.getNumber() + " is not healthy in Venice backend; please "
                  + "consider running a full batch push for your store: " + storeName
                  + " before running incremental push, " + "or reach out to Venice team.");
        } else {
          LOGGER.info(
              "Found lingering topic: {} with push id: {}. Killing the lingering version that was created at: {}",
              currentPushTopic.get(),
              existingPushJobId,
              version.getCreatedTime());
          parent.killOfflinePush(clusterName, currentPushTopic.get(), true);
        }
      } else if (Version.canIncomingPushKillExistingPush(existingPushJobId, pushJobId, pushType)) {
        LOGGER.info(
            "Found running system push job (repush/compliance) with push id: {} and incoming push is a "
                + "user-initiated batch job or stream reprocessing job with push id: {}. Killing the system push for store: {}",
            existingPushJobId,
            pushJobId,
            storeName);
        parent.killOfflinePush(clusterName, currentPushTopic.get(), true);
      } else if (pushType.isIncremental()) {
        LOGGER.info(
            "Found a running batch push job: {} and incoming push: {} is an incremental push. "
                + "Letting the push continue for the store: {}",
            existingPushJobId,
            pushJobId,
            storeName);
      } else {
        String msg = version.isVersionSwapDeferred()
            ? ". There is already a future version " + version.getNumber() + " exists for the store " + storeName
                + " please make that version current before starting a next push."
            : ". An ongoing push with pushJobId " + existingPushJobId + " and topic " + currentPushTopic.get()
                + " is found and it must be terminated before another push can be started.";
        VeniceException e = new ConcurrentBatchPushException(
            "Unable to start the push with pushJobId " + pushJobId + " for store " + storeName + msg);
        e.setStackTrace(EMPTY_STACK_TRACE);
        throw e;
      }
    }

    if (parent.isDegradedModeEnabled(clusterName) && pushType.isIncremental()) {
      Map<String, DegradedDcInfo> degradedDcs = parent.getDegradedDatacenters(clusterName);
      if (!degradedDcs.isEmpty()) {
        DegradedModeStats degradedModeStats = parent.getDegradedModeStats();
        if (degradedModeStats != null) {
          degradedModeStats.recordPushBlockedIncremental(clusterName, storeName);
        }
        throw new VeniceException(
            "Incremental push blocked: DC(s) " + degradedDcs.keySet()
                + " are degraded. Incremental pushes are not supported during degraded mode for store " + storeName
                + ".");
      }
    }

    String effectiveTargetedRegions = targetedRegions;
    boolean effectiveVersionSwapDeferred = versionSwapDeferred;
    boolean autoConvertedForDegraded = false;
    if (parent.isDegradedModeEnabled(clusterName)) {
      Map<String, DegradedDcInfo> degradedDcs = parent.getDegradedDatacenters(clusterName);
      if (!degradedDcs.isEmpty() && !pushType.isIncremental() && !store.isHybrid()
          && VeniceSystemStoreType.getSystemStoreType(storeName) == null) {
        Map<String, String> allRegions = parent.getVeniceHelixAdmin().getChildDataCenterControllerUrlMap(clusterName);
        Set<String> baseTargetSet = StringUtils.isEmpty(targetedRegions)
            ? new TreeSet<>(allRegions.keySet())
            : new TreeSet<>(parseRegionsFilterList(targetedRegions));
        Set<String> healthyTarget = new TreeSet<>(baseTargetSet);
        healthyTarget.removeAll(degradedDcs.keySet());
        if (healthyTarget.isEmpty()) {
          throw new VeniceException(
              "Cannot push to store " + storeName + ": requested target DCs are all degraded. Requested: "
                  + baseTargetSet + ", degraded: " + degradedDcs.keySet());
        }
        if (!healthyTarget.equals(baseTargetSet)) {
          effectiveTargetedRegions = String.join(",", healthyTarget);
          effectiveVersionSwapDeferred = true;
          autoConvertedForDegraded = true;
          DegradedModeStats degradedModeStats = parent.getDegradedModeStats();
          if (degradedModeStats != null) {
            degradedModeStats.recordPushAutoConverted(clusterName, storeName);
          }
          LOGGER.info(
              "Auto-converting push for store {}: dropping degraded DCs {} from target set. Base target: {}, "
                  + "effective target: {}",
              storeName,
              degradedDcs.keySet(),
              baseTargetSet,
              effectiveTargetedRegions);
        }
      }
    }

    Version newVersion;
    if (pushType.isIncremental()) {
      newVersion = parent.getVeniceHelixAdmin().getIncrementalPushVersion(clusterName, storeName, pushJobId);
    } else {
      if (VeniceSystemStoreType.getSystemStoreType(storeName) != null
          && (effectiveVersionSwapDeferred && StringUtils.isNotEmpty(effectiveTargetedRegions))) {
        LOGGER.warn(
            "Target region push with deferred swap is not supported for system store {}. Ignoring versionSwapDeferred and targetedRegions configs.",
            storeName);
        effectiveVersionSwapDeferred = false;
        effectiveTargetedRegions = null;
      }

      validateTargetedRegions(effectiveTargetedRegions, clusterName);

      newVersion = parent.addVersionAndTopicOnly(
          clusterName,
          storeName,
          pushJobId,
          VERSION_ID_UNSET,
          numberOfPartitions,
          replicationFactor,
          pushType,
          sendStartOfPush,
          sorted,
          compressionDictionary,
          sourceGridFabric,
          rewindTimeInSecondsOverride,
          emergencySourceRegion,
          effectiveVersionSwapDeferred,
          effectiveTargetedRegions,
          repushSourceVersion,
          store.getLargestUsedRTVersionNumber(),
          repushTtlSeconds,
          autoConvertedForDegraded);
    }
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == null) {
      if (pushType.isBatch()) {
        parent.getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(clusterName)
            .getVeniceAdminStats()
            .recordSuccessfullyStartedUserBatchPushParentAdminCount();
      } else if (pushType.isIncremental()) {
        parent.getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(clusterName)
            .getVeniceAdminStats()
            .recordSuccessfullyStartedUserIncrementalPushParentAdminCount();
      }
    }

    return newVersion;
  }

  private void validateTargetedRegions(String targetedRegions, String clusterName) throws VeniceException {
    if (StringUtils.isEmpty(targetedRegions)) {
      return;
    }
    Set<String> targetedRegionSet = parseRegionsFilterList(targetedRegions);
    Map<String, ControllerClient> clientMap = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    for (String region: targetedRegionSet) {
      if (!clientMap.containsKey(region)) {
        throw new VeniceException(
            "One of the targeted region " + region + " is not a valid region in cluster " + clusterName);
      }
    }
  }

  Version getIncrementalPushVersion(String clusterName, String storeName, String pushJobId) {
    Version incrementalPushVersion =
        parent.getVeniceHelixAdmin().getIncrementalPushVersion(clusterName, storeName, pushJobId);
    String incrementalPushTopic = incrementalPushVersion.kafkaTopicName();
    ExecutionStatus status = parent.getOffLinePushStatus(clusterName, incrementalPushTopic).getExecutionStatus();

    return getIncrementalPushVersion(incrementalPushVersion, status);
  }

  Version getIncrementalPushVersion(Version incrementalPushVersion, ExecutionStatus status) {
    String storeName = incrementalPushVersion.getStoreName();
    if (!status.isTerminal()) {
      throw new VeniceException("Cannot start incremental push since batch push is on going." + " store: " + storeName);
    }

    String incrementalPushTopic = Utils.composeRealTimeTopic(storeName);
    if (status.isError() || parent.getVeniceHelixAdmin().isTopicTruncated(incrementalPushTopic)) {
      throw new VeniceException(
          "Cannot start incremental push since previous batch push has failed. Please run another bash job."
              + " store: " + storeName);
    }
    return incrementalPushVersion;
  }

  OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
    Map<String, ControllerClient> controllerClients = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients);
  }

  OfflinePushStatusInfo getOffLinePushStatus(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      String region,
      String targetedRegions,
      boolean isTargetRegionPushWithDeferredSwap) {
    Map<String, ControllerClient> controllerClients = parent.getVeniceHelixAdmin().getControllerClientMap(clusterName);
    if (region != null) {
      if (!controllerClients.containsKey(region)) {
        throw new VeniceException("Region " + region + " does not exist in " + controllerClients.keySet());
      }
      JobStatusQueryResponse response = controllerClients.get(region).queryDetailedJobStatus(kafkaTopic, region);
      if (response.isError()) {
        throw new VeniceException(
            "Couldn't query " + region + " for job " + kafkaTopic + " status: " + response.getError());
      }
      ExecutionStatus status = ExecutionStatus.valueOf(response.getStatus());
      String statusDetails = response.getOptionalStatusDetails().orElse(null);
      OfflinePushStatusInfo offlinePushStatusInfo =
          new OfflinePushStatusInfo(status, response.getStatusUpdateTimestamp(), statusDetails);
      offlinePushStatusInfo.setUncompletedPartitions(response.getUncompletedPartitions());
      return offlinePushStatusInfo;
    }
    return getOffLineJobStatus(
        clusterName,
        kafkaTopic,
        controllerClients,
        incrementalPushVersion,
        targetedRegions,
        isTargetRegionPushWithDeferredSwap);
  }

  OfflinePushStatusInfo getOffLineJobStatus(
      String clusterName,
      String kafkaTopic,
      Map<String, ControllerClient> controllerClients) {
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, Optional.empty(), null, false);
  }

  private OfflinePushStatusInfo getOffLineJobStatus(
      String clusterName,
      String kafkaTopic,
      Map<String, ControllerClient> controllerClients,
      Optional<String> incrementalPushVersion,
      String targetedRegions,
      boolean isTargetRegionPushWithDeferredSwap) {
    Set<String> childRegions = controllerClients.keySet();
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    Map<String, String> extraInfo = new HashMap<>();
    Map<String, String> extraDetails = new HashMap<>();
    Map<String, Long> extraInfoUpdateTimestamp = new HashMap<>();
    int numChildRegionsFailedToFetchStatus = 0;
    Set<String> targetedRegionSet = parseRegionsFilterList(targetedRegions);

    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      if (!targetedRegionSet.isEmpty() && !targetedRegionSet.contains(region) && !isTargetRegionPushWithDeferredSwap) {
        continue;
      }
      ControllerClient controllerClient = entry.getValue();
      String leaderControllerUrl;
      try {
        leaderControllerUrl = controllerClient.getLeaderControllerUrl();
      } catch (VeniceException exception) {
        LOGGER.warn("Couldn't query {} for job status of {}", region, kafkaTopic, exception);
        statuses.put(region, ExecutionStatus.UNKNOWN);
        extraInfo.put(region, ExecutionStatus.UNKNOWN.toString());
        extraDetails.put(region, "Failed to get leader controller url " + exception.getMessage());
        continue;
      }
      JobStatusQueryResponse response = controllerClient.queryJobStatus(kafkaTopic, incrementalPushVersion);
      if (response.isError()) {
        numChildRegionsFailedToFetchStatus += 1;
        LOGGER.warn("Couldn't query {} for job {} status: {}", region, kafkaTopic, response.getError());
        statuses.put(region, ExecutionStatus.UNKNOWN);
        extraInfo.put(region, ExecutionStatus.UNKNOWN.toString());
        extraDetails.put(region, leaderControllerUrl + " " + response.getError());
      } else {
        ExecutionStatus status = ExecutionStatus.valueOf(response.getStatus());
        statuses.put(region, status);
        extraInfo.put(region, response.getStatus());
        if (response.getStatusUpdateTimestamp() != null) {
          extraInfoUpdateTimestamp.put(region, response.getStatusUpdateTimestamp());
        }
        Optional<String> statusDetails = response.getOptionalStatusDetails();
        statusDetails.ifPresent(s -> extraDetails.put(region, leaderControllerUrl + " " + s));
      }
    }

    StringBuilder currentReturnStatusDetails = new StringBuilder();

    ExecutionStatus currentReturnStatus = VersionLifecyclePolicy
        .getFinalReturnStatus(statuses, childRegions, numChildRegionsFailedToFetchStatus, currentReturnStatusDetails);

    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int versionNum = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    HelixVeniceClusterResources resources = parent.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
    Store parentStore = repository.getStore(storeName);
    Version version = parentStore.getVersion(versionNum);

    boolean isParentVersionStatusStarted = parentStore.getVersionStatus(versionNum).equals(STARTED);
    if (isTargetRegionPushWithDeferredSwap && isParentVersionStatusStarted) {
      updateParentVersionStatusIfTerminal(targetedRegionSet, extraInfo, parentStore, repository, versionNum);
    }

    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      if (currentReturnStatus.isTerminal()) {
        LOGGER.info("Received terminal status: {} for topic: {}", currentReturnStatus, kafkaTopic);

        boolean isDeferredSwap = version != null && version.isVersionSwapDeferred();
        if (!isDeferredSwap) {
          handleTerminalJobStatus(
              clusterName,
              kafkaTopic,
              incrementalPushVersion,
              targetedRegions,
              parentStore,
              version,
              versionNum,
              currentReturnStatus,
              currentReturnStatusDetails,
              repository);
        }

        if (isTargetRegionPushWithDeferredSwap) {
          boolean isVersionTerminal = TERMINAL_VERSION_SWAP_STATUSES.contains(version.getStatus());
          if (isVersionTerminal) {
            LOGGER.info(
                "Truncating parent VT {} after push status {} and version status {}",
                kafkaTopic,
                currentReturnStatus.getRootStatus(),
                version.getStatus());
            truncateTopicsOptionally(
                clusterName,
                kafkaTopic,
                incrementalPushVersion,
                currentReturnStatus,
                currentReturnStatusDetails);
          }
        }
      } else {
        if (version.getStatus().equals(KILLED)) {
          LOGGER.info(
              "Marking execution status as ERROR for store {} because parent version status is KILLED",
              storeName);
          return new OfflinePushStatusInfo(
              ExecutionStatus.ERROR,
              null,
              extraInfo,
              currentReturnStatusDetails.toString(),
              extraDetails,
              extraInfoUpdateTimestamp);
        }
      }
    }

    return new OfflinePushStatusInfo(
        currentReturnStatus,
        null,
        extraInfo,
        currentReturnStatusDetails.toString(),
        extraDetails,
        extraInfoUpdateTimestamp);
  }

  private void updateParentVersionStatusIfTerminal(
      Set<String> targetRegions,
      Map<String, String> regionToPushStatusInfo,
      Store parentStore,
      ReadWriteStoreRepository repository,
      int versionNum) {
    Set<String> completedRegions = new HashSet<>();
    Set<String> failedRegions = new HashSet<>();
    for (Map.Entry<String, String> entry: regionToPushStatusInfo.entrySet()) {
      String region = entry.getKey();
      String pushStatus = regionToPushStatusInfo.get(region);
      if (targetRegions.contains(region)) {
        if (pushStatus.equals(ExecutionStatus.COMPLETED.toString())) {
          completedRegions.add(region);
        } else if (pushStatus.equals(ExecutionStatus.ERROR.toString())) {
          failedRegions.add(region);
        }
      }
    }

    if (completedRegions.size() == targetRegions.size()) {
      parentStore.updateVersionStatus(versionNum, PUSHED);
      repository.updateStore(parentStore);
      LOGGER.info(
          "Updating parent store {} version {} status to {} for target region push w/ deferred swap",
          parentStore.getName(),
          versionNum,
          PUSHED);
    } else if (failedRegions.size() > 0) {
      parentStore.updateVersionStatus(versionNum, ERROR);
      repository.updateStore(parentStore);
      LOGGER.info(
          "Updating parent store {} version {} status to {} for target region push w/ deferred swap",
          parentStore.getName(),
          versionNum,
          ERROR);
    }
  }

  private void handleTerminalJobStatus(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      String targetedRegions,
      Store parentStore,
      Version version,
      int versionNum,
      ExecutionStatus currentReturnStatus,
      StringBuilder currentReturnStatusDetails,
      ReadWriteStoreRepository repository) {
    Version storeVersion = parentStore.getVersion(versionNum);
    boolean isPushCompleteInAllRegionsForTargetRegionPush =
        storeVersion != null && storeVersion.getStatus().equals(PUSHED);
    boolean isHybridStore = storeVersion != null && storeVersion.getHybridStoreConfig() != null;

    boolean isTargetRegionPush = !StringUtils.isEmpty(targetedRegions);

    if (!isTargetRegionPush || isPushCompleteInAllRegionsForTargetRegionPush || isHybridStore) {
      LOGGER.info("Truncating parent VT {} after push status {}", kafkaTopic, currentReturnStatus.getRootStatus());
      truncateTopicsOptionally(
          clusterName,
          kafkaTopic,
          incrementalPushVersion,
          currentReturnStatus,
          currentReturnStatusDetails);
    }

    if (currentReturnStatus.equals(ExecutionStatus.COMPLETED)) {
      if (storeVersion != null && storeVersion.getStatus().equals(ONLINE)) {
        LOGGER.info("Parent store version {} status is already ONLINE, no need to update it.", kafkaTopic);
        return;
      }
      if (isTargetRegionPush && !isPushCompleteInAllRegionsForTargetRegionPush) {
        parentStore.updateVersionStatus(versionNum, PUSHED);
        repository.updateStore(parentStore);
        LOGGER.info("Updating parent store version {} status to {}", kafkaTopic, PUSHED);
      } else {
        parentStore.updateVersionStatus(versionNum, ONLINE);
        parentStore.setCurrentVersion(versionNum);
        repository.updateStore(parentStore);
        LOGGER.info("Updating parent store version {} status to {}", kafkaTopic, ONLINE);
      }
    }
  }

  private void truncateTopicsOptionally(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      ExecutionStatus currentReturnStatus,
      StringBuilder currentReturnStatusDetails) {
    if (parent.maxErroredTopicNumToKeep > 0 && currentReturnStatus.isError()) {
      currentReturnStatusDetails.append("Parent Kafka topic won't be truncated");
      LOGGER.info(
          "The errored kafka topic {} won't be truncated since it will be used to investigate some Kafka related issue",
          kafkaTopic);
    } else {
      Store store =
          parent.getVeniceHelixAdmin().getStore(clusterName, Version.parseStoreFromKafkaTopicName(kafkaTopic));
      boolean failedBatchPush = !incrementalPushVersion.isPresent() && currentReturnStatus.isError();
      Version version = store.getVersion(Version.parseVersionFromKafkaTopicName(kafkaTopic));

      boolean incPushEnabledBatchPushSuccess = !incrementalPushVersion.isPresent() && store.isIncrementalPushEnabled();
      boolean nonIncPushBatchSuccess = !store.isIncrementalPushEnabled() && !currentReturnStatus.isError();
      boolean isDeferredVersionSwap = version != null && version.isVersionSwapDeferred();
      boolean isTargetRegionPushWithDeferredSwap =
          isDeferredVersionSwap && !StringUtils.isEmpty(version.getTargetSwapRegion());

      ConcurrentPushDetectionStrategy concurrentPushDetectionStrategy =
          parent.getMultiClusterConfigs().getControllerConfig(clusterName).getConcurrentPushDetectionStrategy();
      if ((failedBatchPush || nonIncPushBatchSuccess && !isDeferredVersionSwap || incPushEnabledBatchPushSuccess
          || isTargetRegionPushWithDeferredSwap)
          && !parent.getMultiClusterConfigs().getCommonConfig().disableParentTopicTruncationUponCompletion()) {
        if (concurrentPushDetectionStrategy.isTopicWriteNeeded()) {
          LOGGER.info("Truncating kafka topic: {} with job status: {}", kafkaTopic, currentReturnStatus);
          parent.truncateKafkaTopic(kafkaTopic);
        }
        if (version != null && version.getPushType().isStreamReprocessing()) {
          String streamReprocessingTopic = Version.composeStreamReprocessingTopic(store.getName(), version.getNumber());
          LOGGER.info("Truncating kafka topic: {} with job status: {}", streamReprocessingTopic, currentReturnStatus);
          parent.truncateKafkaTopic(streamReprocessingTopic);
        }
        currentReturnStatusDetails.append("Parent Kafka topic truncated");
      }
    }
  }

  private com.linkedin.venice.utils.Time getTimer() {
    return parent.timer == null ? SystemTime.INSTANCE : parent.timer;
  }
}
