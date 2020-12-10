package com.linkedin.davinci;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.ConcurrentRef;
import com.linkedin.venice.utils.ReferenceCounted;

import com.linkedin.davinci.client.ClientStats;
import com.linkedin.davinci.config.StoreBackendConfig;

import org.apache.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;


public class StoreBackend {
  private static final Logger logger = Logger.getLogger(StoreBackend.class);

  private final DaVinciBackend backend;
  private final String storeName;
  private final StoreBackendConfig config;
  private final ClientStats stats;
  private final ComplementSet<Integer> subscription = ComplementSet.emptySet();
  private final ConcurrentRef<VersionBackend> currentVersionRef = new ConcurrentRef<>(this::deleteVersion);
  private VersionBackend currentVersion;
  private VersionBackend futureVersion;

  StoreBackend(DaVinciBackend backend, String storeName) {
    this.backend = backend;
    this.storeName = storeName;
    this.config = new StoreBackendConfig(backend.getConfigLoader().getVeniceServerConfig().getDataBasePath(), storeName);
    this.stats = new ClientStats(backend.getMetricsRepository(), storeName);
    try {
      backend.getStoreRepository().subscribe(storeName);
    } catch (InterruptedException e) {
      logger.info("StoreRepository::subscribe was interrupted", e);
      Thread.currentThread().interrupt();
    }
    this.config.store();
  }

  synchronized void close() {
    currentVersionRef.clear();
    subscription.clear();

    if (futureVersion != null) {
      futureVersion.close();
      futureVersion = null;
    }

    if (currentVersion != null) {
      currentVersion.close();
      currentVersion = null;
    }
    backend.getStoreRepository().unsubscribe(storeName);
  }

  synchronized void delete() {
    logger.info("Deleting local store " + storeName);
    currentVersionRef.clear();
    subscription.clear();

    if (futureVersion != null) {
      futureVersion.delete();
      futureVersion = null;
    }

    if (currentVersion != null) {
      currentVersion.delete();
      currentVersion = null;
    }

    config.delete();
    backend.getStoreRepository().unsubscribe(storeName);
  }

  public boolean isManaged() {
    return config.isManaged();
  }

  public void setManaged(boolean isManaged) {
    config.setManaged(isManaged);
    config.store();
  }

  public void setMemoryLimit(long memoryLimit) {
    PersistenceType engineType = backend.getConfigLoader().getVeniceServerConfig().getPersistenceType();
    if (engineType != PersistenceType.ROCKS_DB) {
      logger.warn("Memory limit is only supported for RocksDB engines, storeName=" + storeName + ", engineType=" + engineType);
      return;
    }
    backend.registerRocksDBMemoryLimit(storeName, memoryLimit);
  }

  public ClientStats getStats() {
    return stats;
  }

  public ReferenceCounted<VersionBackend> getCurrentVersion() {
    return currentVersionRef.get();
  }

  private void setCurrentVersion(VersionBackend version) {
    logger.info("Switching to a new version " + version);
    currentVersion = version;
    currentVersionRef.set(version);
  }

  public CompletableFuture<Void> subscribe(ComplementSet<Integer> partitions) {
    return subscribe(partitions, Optional.empty());
  }

  synchronized CompletableFuture<Void> subscribe(ComplementSet<Integer> partitions, Optional<Version> bootstrapVersion) {
    if (currentVersion == null) {
      setCurrentVersion(new VersionBackend(
          backend,
          bootstrapVersion.orElseGet(
              () -> backend.getLatestVersion(storeName).orElseThrow(
                  () -> new VeniceException("Cannot subscribe to an empty store, storeName=" + storeName)))));

    } else if (bootstrapVersion.isPresent()) {
      throw new VeniceException("Bootstrap version is already selected, storeName=" + storeName +
                                    ", currentVersion=" + currentVersion +
                                    ", desiredVersion=" + bootstrapVersion.get().kafkaTopicName());
    }

    logger.info("Subscribing to partitions, storeName=" + storeName + ", partitions=" + partitions);
    if (subscription.isEmpty() && !partitions.isEmpty()) {
      // re-create store config that was potentially deleted by unsubscribe
      config.store();
    }
    subscription.addAll(partitions);

    if (futureVersion != null) {
      futureVersion.subscribe(partitions).whenComplete((v, t) -> trySwapCurrentVersion());
    } else if (bootstrapVersion.isPresent()) {
      trySubscribeFutureVersion();
    }

    ReferenceCounted<VersionBackend> ref = getCurrentVersion();
    return currentVersion.subscribe(partitions).whenComplete((v, t) -> ref.release());
  }

  public synchronized void unsubscribe(ComplementSet<Integer> partitions) {
    logger.info("Unsubscribing from partitions, storeName=" + storeName + ", partitions=" + subscription);
    subscription.removeAll(partitions);

    if (currentVersion != null) {
      currentVersion.unsubscribe(partitions);
    }

    if (futureVersion != null) {
      futureVersion.unsubscribe(partitions);
    }

    if (subscription.isEmpty()) {
      config.delete();
    }
  }

  synchronized void trySubscribeFutureVersion() {
    if (currentVersion == null || futureVersion != null) {
      return;
    }

    Version version = backend.getLatestVersion(storeName).orElse(null);
    if (version == null || version.getNumber() <= currentVersion.getVersion().getNumber()) {
      return;
    }

    logger.info("Subscribing to a future version " + version.kafkaTopicName());
    futureVersion = new VersionBackend(backend, version);
    futureVersion.subscribe(subscription).whenComplete((v, t) -> trySwapCurrentVersion());
  }

  // May be called indirectly by readers, so cannot be blocking
  private void deleteVersion(VersionBackend version) {
    backend.getExecutor().submit(version::delete);
  }

  synchronized void deleteOldVersions() {
    if (futureVersion != null) {
      Store store = backend.getStoreRepository().getStoreOrThrow(storeName);
      int versionNumber = futureVersion.getVersion().getNumber();
      if (!store.getVersion(versionNumber).isPresent()) {
        logger.info("Deleting obsolete future version " + futureVersion);
        futureVersion.delete();
        futureVersion = null;
      }
    }
  }

  // May be called several times even after version was swapped
  private synchronized void trySwapCurrentVersion() {
    if (futureVersion != null && futureVersion.isReadyToServe(subscription)) {
      logger.info("Ready to serve " + subscription + " partitions of " + futureVersion.getVersion().kafkaTopicName());
      setCurrentVersion(futureVersion);
      futureVersion = null;
      trySubscribeFutureVersion();
    }
  }
}
