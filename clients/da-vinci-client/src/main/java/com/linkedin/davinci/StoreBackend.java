package com.linkedin.davinci;

import com.linkedin.davinci.client.DaVinciSeekCheckpointInfo;
import com.linkedin.davinci.config.StoreBackendConfig;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.ConcurrentRef;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.RegionUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreBackend {
  private static final Logger LOGGER = LogManager.getLogger(StoreBackend.class);

  private final DaVinciBackend backend;
  private final String storeName;
  private final StoreBackendStats stats;
  private final StoreBackendConfig config;
  private final Set<Integer> faultyVersionSet = new HashSet<>();
  private final ComplementSet<Integer> subscription = ComplementSet.emptySet();
  private final ConcurrentRef<VersionBackend> daVinciCurrentVersionRef = new ConcurrentRef<>(this::deleteVersion);
  private final AvroStoreDeserializerCache storeDeserializerCache;
  private VersionBackend daVinciCurrentVersion;
  private VersionBackend daVinciFutureVersion;

  StoreBackend(DaVinciBackend backend, String storeName) {
    LOGGER.info("Opening local store {}", storeName);
    this.backend = backend;
    this.storeName = storeName;
    this.config =
        new StoreBackendConfig(backend.getConfigLoader().getVeniceServerConfig().getDataBasePath(), storeName);
    this.stats = new StoreBackendStats(backend.getMetricsRepository(), storeName);
    this.storeDeserializerCache = new AvroStoreDeserializerCache(backend.getSchemaRepository(), storeName, true);
    try {
      backend.getStoreRepository().subscribe(storeName);
    } catch (InterruptedException e) {
      LOGGER.warn("StoreRepository::subscribe was interrupted", e);
      Thread.currentThread().interrupt();
      return;
    }
    this.config.store();
  }

  synchronized void close() {
    if (subscription.isEmpty()) {
      LOGGER.info("Closing empty local store {}", storeName);
      delete();
      return;
    }

    LOGGER.info("Closing local store {}", storeName);
    subscription.clear();
    daVinciCurrentVersionRef.clear();

    if (daVinciFutureVersion != null) {
      VersionBackend version = daVinciFutureVersion;
      setDaVinciFutureVersion(null);
      version.close();
    }

    if (daVinciCurrentVersion != null) {
      VersionBackend version = daVinciCurrentVersion;
      setDaVinciCurrentVersion(null);
      version.close();
    }
  }

  synchronized void delete() {
    LOGGER.info("Deleting local store {}", storeName);
    config.delete();
    subscription.clear();
    daVinciCurrentVersionRef.clear();

    if (daVinciFutureVersion != null) {
      deleteFutureVersion();
    }

    if (daVinciCurrentVersion != null) {
      VersionBackend version = daVinciCurrentVersion;
      setDaVinciCurrentVersion(null);
      version.delete();
    }

    backend.getStoreRepository().unsubscribe(storeName);
  }

  public boolean isManaged() {
    return config.isManaged();
  }

  public void setManaged(boolean isManaged) {
    config.setManaged(isManaged);
    config.store();
  }

  public StoreBackendStats getStats() {
    return stats;
  }

  public ComplementSet<Integer> getSubscription() {
    return subscription;
  }

  public ReferenceCounted<VersionBackend> getDaVinciCurrentVersion() {
    return daVinciCurrentVersionRef.get();
  }

  private synchronized void setDaVinciCurrentVersion(VersionBackend version) {
    LOGGER.info("Switching to new version {}, currentVersion {}", version, daVinciCurrentVersion);
    daVinciCurrentVersion = version;
    daVinciCurrentVersionRef.set(version);
    stats.recordCurrentVersion(version);
  }

  private void setDaVinciFutureVersion(VersionBackend version) {
    daVinciFutureVersion = version;
    stats.recordFutureVersion(version);
  }

  public CompletableFuture<Void> subscribe(ComplementSet<Integer> partitions) {
    return subscribe(partitions, Optional.empty(), null);
  }

  // public CompletableFuture<Void> seekToTimestamps(Long allPartitionTimestamp, Optional<Version> storeVersion) {
  // return subscribe(
  // ComplementSet.universalSet(),
  // storeVersion, null);
  // }

  public CompletableFuture<Void> seekToCheckpoint(
      DaVinciSeekCheckpointInfo checkpointInfo,
      Optional<Version> storeVersion) {
    return subscribe(checkpointInfo.getPartitions(), storeVersion, checkpointInfo);
  }

  // public CompletableFuture<Void> seekToTail(Optional<Version> storeVersion) {
  // return subscribe(ComplementSet.universalSet(), storeVersion, null);
  // }

  // public CompletableFuture<Void> seekToCheckPoints(
  // Map<Integer, PubSubPosition> checkpoints,
  // Optional<Version> storeVersion) {
  // return subscribe(
  // ComplementSet.wrap(checkpoints.keySet()),
  // storeVersion, null);
  // }

  // public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps, Optional<Version> storeVersion) {
  // return subscribe(
  // ComplementSet.wrap(timestamps.keySet()),
  // storeVersion, null);
  // }

  private Version getCurrentVersion() {
    return backend.getVeniceCurrentVersion(storeName);
  }

  private Version getLatestNonFaultyVersion() {
    return backend.getVeniceLatestNonFaultyVersion(storeName, faultyVersionSet);
  }

  public synchronized CompletableFuture<Void> subscribe(
      ComplementSet<Integer> partitions,
      Optional<Version> bootstrapVersion,
      DaVinciSeekCheckpointInfo checkpointInfo) {
    if (daVinciCurrentVersion == null) {
      setDaVinciCurrentVersion(new VersionBackend(backend, bootstrapVersion.orElseGet(() -> {
        Version version = getCurrentVersion();
        if (version == null) {
          version = getLatestNonFaultyVersion();
        }
        if (version == null) {
          throw new VeniceException("Cannot subscribe to an empty store, storeName=" + storeName);
        }
        return version;
      }), stats));

    } else if (bootstrapVersion.isPresent()
        && !bootstrapVersion.get().kafkaTopicName().equals(daVinciCurrentVersion.getVersion().kafkaTopicName())) {
      throw new VeniceException(
          "Bootstrap version is already selected, storeName=" + storeName + ", currentVersion=" + daVinciCurrentVersion
              + ", desiredVersion=" + bootstrapVersion.get().kafkaTopicName());
    }

    LOGGER.info("Subscribing to partitions {} of store {}", partitions, storeName);
    if (subscription.isEmpty() && !partitions.isEmpty()) {
      // Recreate store config that was potentially deleted by unsubscribe.
      config.store();
    }
    subscription.addAll(partitions);

    if (DaVinciBackend.ClientType.VERSION_SPECIFIC.equals(backend.getStoreClientType(getStoreName()))
        && bootstrapVersion.isPresent()) {
      LOGGER.info(
          "Ignoring version swaps for store: {} since its a version-specific client. Staying on target version: {}.",
          storeName,
          bootstrapVersion.get().getNumber());
    } else {
      if (daVinciFutureVersion == null) {
        trySubscribeDaVinciFutureVersion();
      } else {
        daVinciFutureVersion.subscribe(partitions, null).whenComplete((v, e) -> trySwapDaVinciCurrentVersion(e));
      }
    }

    VersionBackend savedVersion = daVinciCurrentVersion;
    List<Integer> partitionList = daVinciCurrentVersion.getPartitions(partitions);
    if (checkpointInfo.getAllPartitionsTimestamp() != null) {
      Map<Integer, Long> timestamps = new HashMap<>();
      for (int partition: partitionList) {
        timestamps.put(partition, checkpointInfo.getAllPartitionsTimestamp());
      }
      checkpointInfo.setTimestampsMap(timestamps);
    } else if (checkpointInfo.isSeekToTail()) {
      Map<Integer, PubSubPosition> positionMap = new HashMap<>();
      for (int partition: partitionList) {
        positionMap.put(partition, PubSubSymbolicPosition.LATEST);
      }
      checkpointInfo.setPositionMap(positionMap);
    }
    return daVinciCurrentVersion.subscribe(partitions, checkpointInfo).exceptionally(e -> {
      synchronized (this) {
        addFaultyVersion(savedVersion, e);
        // Don't propagate failure to subscribe() caller, if future version has become current and is ready to
        // serve.
        if (daVinciCurrentVersion != null && daVinciCurrentVersion.isReadyToServe(subscription)) {
          return null;
        }
      }
      throw (e instanceof CompletionException) ? (CompletionException) e : new CompletionException(e);
    }).whenComplete((v, e) -> {
      synchronized (this) {
        if (e == null) {
          LOGGER.info("Ready to serve partitions {} of {}", subscription, daVinciCurrentVersion);
        } else {
          LOGGER.warn("Failed to subscribe to partitions {} of {}", subscription, savedVersion, e);
        }
      }
    });
  }

  public synchronized void unsubscribe(ComplementSet<Integer> partitions) {
    LOGGER.info("Unsubscribing from partitions {} of {}", partitions, storeName);
    subscription.removeAll(partitions);

    if (daVinciCurrentVersion != null) {
      daVinciCurrentVersion.unsubscribe(partitions);
    }

    if (daVinciFutureVersion != null) {
      daVinciFutureVersion.unsubscribe(partitions);
    }

    if (subscription.isEmpty()) {
      config.delete();

      if (daVinciFutureVersion != null) {
        deleteFutureVersion();
      }

      if (daVinciCurrentVersion != null) {
        VersionBackend version = daVinciCurrentVersion;
        daVinciCurrentVersionRef.clear();
        setDaVinciCurrentVersion(null);
        version.delete();
      }
    }
    LOGGER.info("Finished the unsubscription from partitions {} of {}", partitions, storeName);

  }

  synchronized void trySubscribeDaVinciFutureVersion() {
    if (daVinciCurrentVersion == null || daVinciFutureVersion != null) {
      return;
    }

    Version veniceCurrentVersion = getCurrentVersion();
    // Latest non-faulty store version in Venice store.
    Version veniceLatestVersion = getLatestNonFaultyVersion();
    Version targetVersion;
    // Make sure current version in the store config has highest priority.
    if (veniceCurrentVersion != null
        && veniceCurrentVersion.getNumber() != daVinciCurrentVersion.getVersion().getNumber()) {
      targetVersion = veniceCurrentVersion;
    } else if (veniceLatestVersion != null
        && veniceLatestVersion.getNumber() > daVinciCurrentVersion.getVersion().getNumber()) {
      targetVersion = veniceLatestVersion;
    } else {
      return;
    }

    Set<String> targetRegions = RegionUtils.parseRegionsFilterList(targetVersion.getTargetSwapRegion());
    VeniceServerConfig veniceServerConfig = backend.getConfigLoader().getVeniceServerConfig();
    String currentRegion = veniceServerConfig.getRegionName();
    boolean isTargetRegionEnabled = !StringUtils.isEmpty(targetVersion.getTargetSwapRegion());
    boolean startIngestionInNonTargetRegion = isTargetRegionEnabled && !targetRegions.contains(currentRegion)
        && targetVersion.getStatus() == VersionStatus.ONLINE;

    // Subscribe to the future version if:
    // 1. Target region push with delayed ingestion is not enabled
    // 2. Target region push with delayed ingestion is enabled and the current region is a target region
    // 3. Target region push with delayed ingestion is enabled and the current region is a non target region
    // and the wait time has elapsed. The wait time has elapsed when the version status is marked ONLINE
    if (targetRegions.contains(currentRegion) || startIngestionInNonTargetRegion || !isTargetRegionEnabled) {
      LOGGER.info("Subscribing to future version {}", targetVersion.kafkaTopicName());
      setDaVinciFutureVersion(new VersionBackend(backend, targetVersion, stats));
      // For future version subscription, we don't need to pass any timestamps or position map
      daVinciFutureVersion.subscribe(subscription, null).whenComplete((v, e) -> trySwapDaVinciCurrentVersion(e));
    } else {
      LOGGER.info(
          "Skipping subscribe to future version: {} in region: {} because the target version status is: {} and the target regions are: {}",
          targetVersion.kafkaTopicName(),
          currentRegion,
          targetVersion.getStatus(),
          targetVersion.getTargetSwapRegion());
    }
  }

  /**
   * This method intends to check Venice store's current version and compare with Da Vinci current version when a store
   * change is detected.
   * If current version is smaller than Da Vinci current version, it is considered rollback of store's current version.
   * If current version is greater than Da Vinci current version, it indicates that we have a new current version and
   * we might need to remove it from faulty version set, which might be added because of previous rollback or local ingestion
   * failure.
   */
  synchronized void validateDaVinciAndVeniceCurrentVersion() {
    Version veniceCurrentVersion = getCurrentVersion();
    if (veniceCurrentVersion != null && daVinciCurrentVersion != null) {
      if (veniceCurrentVersion.getNumber() > daVinciCurrentVersion.getVersion().getNumber()
          && faultyVersionSet.contains(veniceCurrentVersion.getNumber())) {
        LOGGER.info(
            "Venice is rolling forward to version: " + veniceCurrentVersion.getNumber()
                + ", removing it from faulty version set.");
        removeFaultyVersion(veniceCurrentVersion);
        return;
      }
      if (veniceCurrentVersion.getNumber() < daVinciCurrentVersion.getVersion().getNumber()) {
        LOGGER.info(
            "Detected a version rollback from Da Vinci current version: " + daVinciCurrentVersion.getVersion()
                + " to Venice current version: " + veniceCurrentVersion);
        removeFaultyVersion(veniceCurrentVersion);
        addFaultyVersion(daVinciCurrentVersion, null);
      }
    }
  }

  /**
   * This method intends to remove faulty/obsolete Da Vinci future version when a store change is detected.
   */
  synchronized void tryDeleteInvalidDaVinciFutureVersion() {
    if (daVinciFutureVersion != null) {
      Store store = backend.getStoreRepository().getStoreOrThrow(storeName);
      int versionNumber = daVinciFutureVersion.getVersion().getNumber();
      if (store.getVersion(versionNumber) == null) {
        LOGGER.info(
            "Deleting obsolete future version " + daVinciFutureVersion + ", currentVersion=" + daVinciCurrentVersion);
        deleteFutureVersion();
      }
      if (faultyVersionSet.contains(versionNumber)) {
        LOGGER.info(
            "Deleting faulty future version " + daVinciFutureVersion + ", currentVersion=" + daVinciCurrentVersion);
        deleteFutureVersion();
      }
    }
  }

  /**
   * This method intends to swap Da Vinci future version to current version. It might be triggered in the following cases:
   * (1) A store change is detected;
   * (2) Da Vinci future version ingestion is completed;
   */
  synchronized void trySwapDaVinciCurrentVersion(Throwable failure) {
    if (daVinciFutureVersion != null) {
      // Fetch current version from store config.
      Version veniceCurrentVersion = getCurrentVersion();
      if (veniceCurrentVersion == null) {
        LOGGER.warn("Failed to retrieve current version of store: " + storeName);
        return;
      }
      int veniceCurrentVersionNumber = veniceCurrentVersion.getNumber();
      int daVinciFutureVersionNumber = daVinciFutureVersion.getVersion().getNumber();
      boolean isDaVinciFutureVersionInvalid =
          faultyVersionSet.contains(daVinciFutureVersionNumber) || backend.getStoreRepository()
              .getStoreOrThrow(storeName)
              .getVersions()
              .stream()
              .noneMatch(v -> (v.getNumber() == daVinciFutureVersionNumber));
      /**
       * We will only swap it to current version slot when it is fully pushed and the version number is (or was) the
       * current version in store config.
       */
      if (daVinciFutureVersion.isReadyToServe(subscription) && !isDaVinciFutureVersionInvalid
          && daVinciFutureVersionNumber <= veniceCurrentVersionNumber) {
        LOGGER.info("Ready to serve partitions " + subscription + " of " + daVinciFutureVersion);
        swapCurrentVersion();
        trySubscribeDaVinciFutureVersion();
      } else if (failure != null) {
        addFaultyVersion(daVinciFutureVersion, failure);
        LOGGER.info(
            "Deleting faulty Da Vinci future version " + daVinciFutureVersion + ", Da Vinci current version="
                + daVinciCurrentVersion);
        deleteFutureVersion();
        trySubscribeDaVinciFutureVersion();
      } else {
        LOGGER.info(
            "Da Vinci future version " + daVinciFutureVersion
                + " is not ready to serve traffic, will try again later.");
      }
    }
  }

  private synchronized void addFaultyVersion(VersionBackend version, Throwable failure) {
    addFaultyVersion(version.getVersion(), failure);
  }

  private synchronized void addFaultyVersion(Version version, Throwable failure) {
    LOGGER.warn("Adding faulty version " + version + " to faulty version set: " + faultyVersionSet, failure);
    faultyVersionSet.add(version.getNumber());
  }

  private synchronized void removeFaultyVersion(Version version) {
    LOGGER.warn("Removing version " + version + " from faulty version set: " + faultyVersionSet);
    faultyVersionSet.remove(version.getNumber());
  }

  // May be called indirectly by readers via ReferenceCounted::release(), so cannot be blocking.
  private void deleteVersion(VersionBackend version) {
    backend.getExecutor().execute(version::delete);
  }

  private void deleteFutureVersion() {
    VersionBackend version = daVinciFutureVersion;
    setDaVinciFutureVersion(null);
    version.delete();
  }

  private void swapCurrentVersion() {
    VersionBackend version = daVinciFutureVersion;
    setDaVinciFutureVersion(null);
    setDaVinciCurrentVersion(version);
  }

  public StoreDeserializerCache getStoreDeserializerCache() {
    return storeDeserializerCache;
  }

  public String getStoreName() {
    return storeName;
  }
}
