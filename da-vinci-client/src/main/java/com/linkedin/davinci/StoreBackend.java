package com.linkedin.davinci;

import com.linkedin.davinci.config.StoreBackendConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.ConcurrentRef;
import com.linkedin.venice.utils.ReferenceCounted;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.log4j.Logger;


public class StoreBackend {
  private static final Logger logger = Logger.getLogger(StoreBackend.class);

  private final DaVinciBackend backend;
  private final String storeName;
  private final StoreBackendStats stats;
  private final StoreBackendConfig config;
  private final Set<Integer> faultyVersions = new HashSet<>();
  private final ComplementSet<Integer> subscription = ComplementSet.emptySet();
  private final ConcurrentRef<VersionBackend> daVinciCurrentVersionRef = new ConcurrentRef<>(this::deleteVersion);
  private VersionBackend daVinciCurrentVersion;
  private VersionBackend daVinciFutureVersion;

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

    backend.getStoreRepository().unsubscribe(storeName);
  }

  synchronized void delete() {
    logger.info("Deleting local store " + storeName);
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

  public void setMemoryLimit(long memoryLimit) {
    backend.setMemoryLimit(storeName, memoryLimit);
  }

  public StoreBackendStats getStats() {
    return stats;
  }

  public ReferenceCounted<VersionBackend> getDaVinciCurrentVersion() {
    return daVinciCurrentVersionRef.get();
  }

  private synchronized void setDaVinciCurrentVersion(VersionBackend version) {
    logger.info("Switching to new version " + version + ", currentVersion=" + daVinciCurrentVersion);
    daVinciCurrentVersion = version;
    daVinciCurrentVersionRef.set(version);
    stats.recordCurrentVersion(version);
  }

  private void setDaVinciFutureVersion(VersionBackend version) {
    daVinciFutureVersion = version;
    stats.recordFutureVersion(version);
  }

  public CompletableFuture subscribe(ComplementSet<Integer> partitions) {
    return subscribe(partitions, Optional.empty());
  }

  synchronized CompletableFuture subscribe(ComplementSet<Integer> partitions, Optional<Version> bootstrapVersion) {
    if (daVinciCurrentVersion == null) {
      setDaVinciCurrentVersion(new VersionBackend(
          backend,
          bootstrapVersion.orElseGet(
              () -> backend.getVeniceCurrentVersion(storeName, faultyVersions).orElseGet(
                  () -> backend.getVeniceLatestVersion(storeName, faultyVersions).orElseThrow(
                      () -> new VeniceException("Cannot subscribe to an empty store, storeName=" + storeName)))),
          stats));

    } else if (bootstrapVersion.isPresent()) {
      throw new VeniceException("Bootstrap version is already selected, storeName=" + storeName +
                                    ", currentVersion=" + daVinciCurrentVersion +
                                    ", desiredVersion=" + bootstrapVersion.get().kafkaTopicName());
    }

    logger.info("Subscribing to partitions " + partitions + " of " + storeName);
    if (subscription.isEmpty() && !partitions.isEmpty()) {
      // Recreate store config that was potentially deleted by unsubscribe.
      config.store();
    }
    subscription.addAll(partitions);

    if (daVinciFutureVersion == null) {
      trySubscribeDaVinciFutureVersion();
    } else {
      daVinciFutureVersion.subscribe(partitions).whenComplete((v, e) -> trySwapDaVinciCurrentVersion(e));
    }

    VersionBackend savedVersion = daVinciCurrentVersion;
    return daVinciCurrentVersion.subscribe(partitions).exceptionally(e -> {
      synchronized (this) {
        addFaultyVersion(savedVersion, e);
        // Don't propagate failure to subscribe() caller, if future version has become current and is ready to serve.
        if (daVinciCurrentVersion != null && daVinciCurrentVersion.isReadyToServe(subscription)) {
          return null;
        }
      }
      throw (e instanceof CompletionException) ? (CompletionException) e : new CompletionException(e);
    }).whenComplete((v, e) -> {
      synchronized (this) {
        if (e == null) {
          logger.info("Ready to serve partitions " + subscription + " of " + daVinciCurrentVersion);
        } else {
          logger.warn("Failed to subscribe to partitions " + subscription + " of " + savedVersion, e);
        }
      }
    });
  }

  public synchronized void unsubscribe(ComplementSet<Integer> partitions) {
    logger.info("Unsubscribing from partitions " + partitions + " of " + storeName);
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
  }

  synchronized void trySubscribeDaVinciFutureVersion() {
    if (daVinciCurrentVersion == null || daVinciFutureVersion != null) {
      return;
    }

    Version storeCurrentVersion = backend.getVeniceCurrentVersion(storeName, faultyVersions).orElse(null);
    Version storeLatestVersion = backend.getVeniceLatestVersion(storeName, faultyVersions).orElse(null);
    Version targetVersion;
    // Make sure current version in the store config has highest priority.
    if (storeCurrentVersion != null && storeCurrentVersion.getNumber() != daVinciCurrentVersion.getVersion().getNumber()) {
      targetVersion = storeCurrentVersion;
    } else {
      /**
       * Before we implement rollback, this condition checking is valid as we should always moving forward. Once we have
       * rollback, we might need to review it again to make sure it won't block rollback.
       */
      if (storeLatestVersion == null || storeLatestVersion.getNumber() <= daVinciCurrentVersion.getVersion().getNumber()) {
        return;
      }
      targetVersion = storeLatestVersion;
    }
    logger.info("Subscribing to future version " + targetVersion.kafkaTopicName());
    setDaVinciFutureVersion(new VersionBackend(backend, targetVersion, stats));
    daVinciFutureVersion.subscribe(subscription).whenComplete((v, e) -> trySwapDaVinciCurrentVersion(e));
  }

  synchronized void tryDeleteObsoleteDaVinciFutureVersion() {
    if (daVinciFutureVersion != null) {
      Store store = backend.getStoreRepository().getStoreOrThrow(storeName);
      int versionNumber = daVinciFutureVersion.getVersion().getNumber();
      if (!store.getVersion(versionNumber).isPresent()) {
        logger.info("Deleting obsolete future version " + daVinciFutureVersion + ", currentVersion=" + daVinciCurrentVersion);
        deleteFutureVersion();
      }
    }
  }

  private synchronized void addFaultyVersion(VersionBackend version, Throwable failure) {
    logger.warn("Failed to subscribe to version " + version +
                    ", currentVersion=" + daVinciCurrentVersion +
                    ", faultyVersions=" + faultyVersions, failure);
    faultyVersions.add(version.getVersion().getNumber());
  }

  // May be called several times even after version was swapped.
  synchronized void trySwapDaVinciCurrentVersion(Throwable failure) {
    if (daVinciFutureVersion != null) {
      // Fetch current version from store config.
      Version veniceCurrentVersion = backend.getVeniceCurrentVersion(storeName, faultyVersions).orElse(null);
      if (veniceCurrentVersion == null) {
        logger.warn("Failed to retrieve current version of store: " + storeName);
        return;
      }
      int veniceCurrentVersionNumber = veniceCurrentVersion.getNumber();
      int daVinciFutureVersionNumber = daVinciFutureVersion.getVersion().getNumber();
      boolean isDaVinciFutureVersionObsolete = backend.getStoreRepository().getStoreOrThrow(storeName).getVersions().stream().noneMatch(v -> (v.getNumber() == daVinciFutureVersionNumber));
      /**
       * We will only swap it to current version slot when it is fully pushed and the version number is (or was) the
       * current version in store config.
       */
      if (daVinciFutureVersion.isReadyToServe(subscription) && !isDaVinciFutureVersionObsolete && daVinciFutureVersionNumber <= veniceCurrentVersionNumber) {
        logger.info("Ready to serve partitions " + subscription + " of " + daVinciFutureVersion);
        swapCurrentVersion();
        trySubscribeDaVinciFutureVersion();
      } else if (failure != null) {
        addFaultyVersion(daVinciFutureVersion, failure);
        logger.info("Deleting faulty Da Vinci future version " + daVinciFutureVersion + ", Da Vinci current version=" + daVinciCurrentVersion);
        deleteFutureVersion();
        trySubscribeDaVinciFutureVersion();
      } else {
        logger.info("Da Vinci future version " + daVinciFutureVersion + " is not ready to serve traffic, will try again later.");
      }
    }
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
}
