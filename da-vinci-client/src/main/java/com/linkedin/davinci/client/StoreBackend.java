package com.linkedin.davinci.client;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ConcurrentRef;
import com.linkedin.venice.utils.ReferenceCounted;

import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StoreBackend {
  private static final Logger logger = Logger.getLogger(StoreBackend.class);

  private final DaVinciBackend backend;
  private final String storeName;
  private final Set<Integer> subscription = new HashSet<>();
  private final ConcurrentRef<VersionBackend> currentVersionRef = new ConcurrentRef<>(this::deleteVersion);
  private VersionBackend currentVersion;
  private VersionBackend futureVersion;

  StoreBackend(DaVinciBackend backend, String storeName) {
    this.backend = backend;
    this.storeName = storeName;
    backend.getStoreRepository().subscribe(storeName);
    backend.getStoreRepository().getStoreOrThrow(storeName);
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
  }

  synchronized void delete() {
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
  }

  public ReferenceCounted<VersionBackend> getCurrentVersion() {
    return currentVersionRef.get();
  }

  void setCurrentVersion(VersionBackend version) {
    logger.info("Switching to a new version, version=" + version);
    currentVersion = version;
    currentVersionRef.set(version);
  }

  public synchronized CompletableFuture subscribeAll() {
    // TODO: Add non-static partitioning support
    if (currentVersion == null) {
      setCurrentVersion(new VersionBackend(
          backend,
          backend.getLatestVersion(storeName).orElseThrow(
              () -> new VeniceException("Cannot subscribe to an empty store, storeName=" + storeName))));
    }
    Version version = currentVersion.getVersion();
    Set<Integer> partitions = IntStream.range(0, version.getPartitionCount()).boxed().collect(Collectors.toSet());
    return subscribe(partitions, Optional.empty());
  }

  public synchronized CompletableFuture subscribe(Set<Integer> partitions, Optional<Version> bootstrapVersion) {
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
    subscription.addAll(partitions);

    if (futureVersion != null) {
      futureVersion.subscribe(partitions).whenComplete((v, t) -> trySwapCurrentVersion());
    } else if (bootstrapVersion.isPresent()) {
      trySubscribeFutureVersion();
    }

    ReferenceCounted<VersionBackend> ref = getCurrentVersion();
    return currentVersion.subscribe(partitions).whenComplete((v, t) -> ref.release());
  }

  public synchronized void unsubscribe(Set<Integer> partitions) {
    logger.info("Unsubscribing from partitions, storeName=" + storeName + ", partitions=" + subscription);

    List<Integer> notSubscribedPartitions = partitions.stream().filter(x -> !subscription.contains(x)).collect(Collectors.toList());
    if (!notSubscribedPartitions.isEmpty()) {
      throw new VeniceException("Cannot unsubscribe from not-subscribed partitions, storeName=" + storeName + ", partitions=" + notSubscribedPartitions);
    }

    subscription.removeAll(partitions);
    if (currentVersion != null) {
      currentVersion.unsubscribe(partitions);
    }
    if (futureVersion != null) {
      futureVersion.unsubscribe(partitions);
    }
  }

  public synchronized void unsubscribeAll() {
    if (subscription.isEmpty()) {
      logger.warn("Not subscribed to any partition, storeName=" + storeName);
      return;
    }
    unsubscribe(new HashSet<>(subscription));
  }

  synchronized void trySubscribeFutureVersion() {
    if (currentVersion == null || futureVersion != null) {
      return;
    }

    Version version = backend.getLatestVersion(storeName).orElse(null);
    if (version == null || version.getNumber() <= currentVersion.getVersion().getNumber()) {
      return;
    }

    logger.info("Subscribing to a future version, version=" + version.kafkaTopicName());
    futureVersion = new VersionBackend(backend, version);
    futureVersion.subscribe(subscription).whenComplete((v, t) -> trySwapCurrentVersion());
  }

  // May be called indirectly by readers, so cannot be blocking
  void deleteVersion(VersionBackend version) {
    backend.getExecutor().submit(() -> version.delete());
  }

  synchronized void deleteOldVersions() {
    if (futureVersion != null) {
      Store store = backend.getStoreRepository().getStoreOrThrow(storeName);
      int versionNumber = futureVersion.getVersion().getNumber();
      if (!store.getVersion(versionNumber).isPresent()) {
        futureVersion.delete();
        futureVersion = null;
      }
    }
  }

  // May be called several times even after version was swapped
  synchronized void trySwapCurrentVersion() {
    if (futureVersion != null && futureVersion.isReadyToServe(subscription)) {
      setCurrentVersion(futureVersion);
      futureVersion = null;
      trySubscribeFutureVersion();
    }
  }
}
