package com.linkedin.davinci;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.ConcurrentRef;
import com.linkedin.venice.utils.ReferenceCounted;

import com.linkedin.davinci.config.StoreBackendConfig;

import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


public class StoreBackend {
  private static final Logger logger = Logger.getLogger(StoreBackend.class);

  private final DaVinciBackend backend;
  private final String storeName;
  private final StoreBackendStats stats;
  private final StoreBackendConfig config;
  private final Set<Integer> faultyVersions = new HashSet<>();
  private final ComplementSet<Integer> subscription = ComplementSet.emptySet();
  private final ConcurrentRef<VersionBackend> currentVersionRef = new ConcurrentRef<>(this::deleteVersion);
  private VersionBackend currentVersion;
  private VersionBackend futureVersion;

  StoreBackend(DaVinciBackend backend, String storeName) {
    logger.info("Opening local store " + storeName);
    this.backend = backend;
    this.storeName = storeName;
    this.config = new StoreBackendConfig(backend.getConfigLoader().getVeniceServerConfig().getDataBasePath(), storeName);
    this.stats = new StoreBackendStats(backend.getMetricsRepository(), storeName);
    try {
      backend.getStoreRepository().subscribe(storeName);
    } catch (InterruptedException e) {
      logger.warn("StoreRepository::subscribe was interrupted", e);
      Thread.currentThread().interrupt();
      return;
    }
    this.config.store();
  }

  synchronized void close() {
    if (subscription.isEmpty()) {
      logger.info("Closing empty local store " + storeName);
      delete();
      return;
    }

    logger.info("Closing local store " + storeName);
    subscription.clear();
    currentVersionRef.clear();

    if (futureVersion != null) {
      VersionBackend version = futureVersion;
      setFutureVersion(null);
      version.close();
    }

    if (currentVersion != null) {
      VersionBackend version = currentVersion;
      setCurrentVersion(null);
      version.close();
    }

    backend.getStoreRepository().unsubscribe(storeName);
  }

  synchronized void delete() {
    logger.info("Deleting local store " + storeName);
    config.delete();
    subscription.clear();
    currentVersionRef.clear();

    if (futureVersion != null) {
      deleteFutureVersion();
    }

    if (currentVersion != null) {
      VersionBackend version = currentVersion;
      setCurrentVersion(null);
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

  public void setMemoryLimit(long memoryLimit) {
    backend.setMemoryLimit(storeName, memoryLimit);
  }

  public StoreBackendStats getStats() {
    return stats;
  }

  public ReferenceCounted<VersionBackend> getCurrentVersion() {
    return currentVersionRef.get();
  }

  private synchronized void setCurrentVersion(VersionBackend version) {
    logger.info("Switching to new version " + version + ", currentVersion=" + currentVersion);
    currentVersion = version;
    currentVersionRef.set(version);
    stats.recordCurrentVersion(version);
  }

  private void setFutureVersion(VersionBackend version) {
    futureVersion = version;
    stats.recordFutureVersion(version);
  }

  public CompletableFuture subscribe(ComplementSet<Integer> partitions) {
    return subscribe(partitions, Optional.empty());
  }

  synchronized CompletableFuture subscribe(ComplementSet<Integer> partitions, Optional<Version> bootstrapVersion) {
    if (currentVersion == null) {
      setCurrentVersion(new VersionBackend(
          backend,
          bootstrapVersion.orElseGet(
              () -> backend.getCurrentVersion(storeName, faultyVersions).orElseGet(
                  () -> backend.getLatestVersion(storeName, faultyVersions).orElseThrow(
                      () -> new VeniceException("Cannot subscribe to an empty store, storeName=" + storeName))))));

    } else if (bootstrapVersion.isPresent()) {
      throw new VeniceException("Bootstrap version is already selected, storeName=" + storeName +
                                    ", currentVersion=" + currentVersion +
                                    ", desiredVersion=" + bootstrapVersion.get().kafkaTopicName());
    }

    logger.info("Subscribing to partitions " + partitions + " of " + storeName);
    if (subscription.isEmpty() && !partitions.isEmpty()) {
      // Recreate store config that was potentially deleted by unsubscribe.
      config.store();
    }
    subscription.addAll(partitions);

    if (futureVersion == null) {
      trySubscribeFutureVersion();
    } else {
      futureVersion.subscribe(partitions).whenComplete((v, e) -> trySwapCurrentVersion(e));
    }

    VersionBackend savedVersion = currentVersion;
    return currentVersion.subscribe(partitions).exceptionally(e -> {
      synchronized (this) {
        addFaultyVersion(savedVersion, e);
        // Don't propagate failure to subscribe() caller, if future version has become current and is ready to serve.
        if (currentVersion != null && currentVersion.isReadyToServe(subscription)) {
          return null;
        }
      }
      throw (e instanceof CompletionException) ? (CompletionException) e : new CompletionException(e);
    }).whenComplete((v, e) -> {
      synchronized (this) {
        if (e == null) {
          logger.info("Ready to serve partitions " + subscription + " of " + currentVersion);
        } else {
          logger.warn("Failed to subscribe to partitions " + subscription + " of " + savedVersion, e);
        }
      }
    });
  }

  public synchronized void unsubscribe(ComplementSet<Integer> partitions) {
    logger.info("Unsubscribing from partitions " + partitions + " of " + storeName);
    subscription.removeAll(partitions);

    if (currentVersion != null) {
      currentVersion.unsubscribe(partitions);
    }

    if (futureVersion != null) {
      futureVersion.unsubscribe(partitions);
    }

    if (subscription.isEmpty()) {
      config.delete();

      if (futureVersion != null) {
        deleteFutureVersion();
      }

      if (currentVersion != null) {
        VersionBackend version = currentVersion;
        currentVersionRef.clear();
        setCurrentVersion(null);
        version.delete();
      }
    }
  }

  synchronized void trySubscribeFutureVersion() {
    if (currentVersion == null || futureVersion != null) {
      return;
    }

    Version version = backend.getLatestVersion(storeName, faultyVersions).orElse(null);
    if (version == null || version.getNumber() <= currentVersion.getVersion().getNumber()) {
      return;
    }

    logger.info("Subscribing to future version " + version.kafkaTopicName());
    setFutureVersion(new VersionBackend(backend, version));
    futureVersion.subscribe(subscription).whenComplete((v, e) -> trySwapCurrentVersion(e));
  }

  private synchronized void addFaultyVersion(VersionBackend version, Throwable failure) {
    logger.warn("Failed to subscribe to version " + version +
                    ", currentVersion=" + currentVersion +
                    ", faultyVersions=" + faultyVersions, failure);
    faultyVersions.add(version.getVersion().getNumber());
  }

  // May be called indirectly by readers via ReferenceCounted::release(), so cannot be blocking.
  private void deleteVersion(VersionBackend version) {
    backend.getExecutor().execute(version::delete);
  }

  private void deleteFutureVersion() {
    VersionBackend version = futureVersion;
    setFutureVersion(null);
    version.delete();
  }

  synchronized void deleteOldVersions() {
    if (futureVersion != null) {
      Store store = backend.getStoreRepository().getStoreOrThrow(storeName);
      int versionNumber = futureVersion.getVersion().getNumber();
      if (!store.getVersion(versionNumber).isPresent()) {
        logger.info("Deleting obsolete future version " + futureVersion + ", currentVersion=" + currentVersion);
        deleteFutureVersion();
      }
    }
  }

  // May be called several times even after version was swapped.
  private synchronized void trySwapCurrentVersion(Throwable failure) {
    if (futureVersion == null) {
      // Nothing to do here because future version was deleted.

    } else if (futureVersion.isReadyToServe(subscription)) {
      logger.info("Ready to serve partitions " + subscription + " of " + futureVersion);
      VersionBackend version = futureVersion;
      setFutureVersion(null);
      setCurrentVersion(version);
      trySubscribeFutureVersion();

    } else if (failure != null) {
      addFaultyVersion(futureVersion, failure);
      logger.info("Deleting faulty future version " + futureVersion + ", currentVersion=" + currentVersion);
      deleteFutureVersion();
      trySubscribeFutureVersion();
    }
  }
}
