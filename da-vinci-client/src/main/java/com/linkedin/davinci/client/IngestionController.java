package com.linkedin.davinci.client;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ConcurrentRef;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


public class IngestionController implements Closeable {

  public class VersionBackend {
    private final Version version;
    private final VeniceStoreConfig config;
    private final Map<Integer, CompletableFuture> partitionFutures = new HashMap<>();

    private VersionBackend(Version version) {
      this.version = version;
      this.config = configLoader.getStoreConfig(version.kafkaTopicName());
      versionByTopicMap.put(version.kafkaTopicName(), this);
    }

    public Version getVersion() {
      return version;
    }

    private synchronized void close() {
      versionByTopicMap.remove(version.kafkaTopicName());
      for (Map.Entry<Integer, CompletableFuture> entry : partitionFutures.entrySet()) {
        ingestionService.stopConsumption(config, entry.getKey());
        entry.getValue().cancel(true);
      }
      // TODO: Make sure partitions aren't consuming
    }

    private synchronized void delete() {
      close();
      for (Map.Entry<Integer, CompletableFuture> entry : partitionFutures.entrySet()) {
        storageService.dropStorePartition(config, entry.getKey());
      }
    }

    private synchronized boolean isReadyToServe(Set<Integer> partitions) {
      for (Integer id : partitions) {
        CompletableFuture future = partitionFutures.get(id);
        if (future == null || !future.isDone()) {
          return false;
        }
      }
      return true;
    }

    private synchronized CompletableFuture subscribe(Set<Integer> partitions) {
      for (Integer id : partitions) {
        if (id < 0 || id >= version.getPartitionCount()) {
          String msg = "Cannot subscribe to out of bounds partition" +
                           ", kafkaTopic=" + version.kafkaTopicName() +
                           ", partition=" + id +
                           ", partitionCount=" + version.getPartitionCount();
          throw new VeniceException(msg);
        }
      }

      Set<Integer> subPartitions = PartitionUtils.getSubPartitions(partitions, version.getPartitionerConfig().getAmplificationFactor());
      List<CompletableFuture> futures = new ArrayList<>(subPartitions.size());
      for (Integer id : subPartitions) {
        futures.add(subscribeSubPartition(id));
      }

      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private synchronized CompletableFuture subscribeSubPartition(int subPartitionId) {
      storageService.openStoreForNewPartition(config, subPartitionId);
      ingestionService.startConsumption(config, subPartitionId, false);
      return partitionFutures.computeIfAbsent(subPartitionId, k -> new CompletableFuture());
    }

    private synchronized void completeSubPartition(int subPartitionId) {
      partitionFutures.computeIfAbsent(subPartitionId, k -> new CompletableFuture()).complete(null);
    }

  }


  public class StoreBackend {
    private final String storeName;
    private final Set<Integer> subscription = new HashSet<>();
    private final ConcurrentRef<VersionBackend> currentVersionRef = new ConcurrentRef<>(this::deleteVersion);
    private VersionBackend currentVersion;
    private VersionBackend futureVersion;

    private StoreBackend(String storeName) {
      storeRepository.getStoreOrThrow(storeName);
      this.storeName = storeName;
      storeRepository.registerStoreDataChangedListener(storeChangeListener);
    }

    private synchronized void close() {
      storeRepository.unregisterStoreDataChangedListener(storeChangeListener);
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

    private synchronized void delete() {
      storeByNameMap.remove(storeName);

      storeRepository.unregisterStoreDataChangedListener(storeChangeListener);
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

    // may be called indirectly by readers, so has to be fast
    private void deleteVersion(VersionBackend version) {
      executor.submit(() -> version.delete());
    }

    private synchronized void deleteOldVersions() {
      if (futureVersion != null) {
        Store store = storeRepository.getStoreOrThrow(storeName);
        int versionNumber = futureVersion.getVersion().getNumber();
        if (!store.getVersion(versionNumber).isPresent()) {
          futureVersion.delete();
          futureVersion = null;
        }
      }
    }

    public ReferenceCounted<VersionBackend> getCurrentVersion() {
      return currentVersionRef.get();
    }

    private void setCurrentVersion(VersionBackend version) {
      currentVersion = version;
      currentVersionRef.set(version);
    }

    public synchronized CompletableFuture subscribe(Optional<Version> version, Set<Integer> partitions) {
      if (currentVersion == null) {
        setCurrentVersion(new VersionBackend(
            version.orElseGet(
                () -> getLatestVersion(storeName).orElseThrow(
                    () -> new VeniceException("Cannot subscribe to an empty store, storeName=" + storeName)))));

      } else if (version.isPresent()) {
        throw new VeniceException("Bootstrap version is already selected, storeName=" + storeName +
                                     ", currentVersion=" + currentVersion.getVersion().kafkaTopicName() +
                                     ", desiredVersion=" + version.get().kafkaTopicName());
      }

      subscription.addAll(partitions);

      if (futureVersion != null) {
        futureVersion.subscribe(partitions).whenComplete((v, t) -> trySwapCurrentVersion());
      } else if (version.isPresent()) {
        trySubscribeFutureVersion();
      }

      ReferenceCounted<VersionBackend> ref = getCurrentVersion();
      return currentVersion.subscribe(partitions).whenComplete((v, t) -> ref.release());
    }

    private synchronized void trySubscribeFutureVersion() {
      if (currentVersion == null || futureVersion != null) {
        return;
      }

      Version version = getLatestVersion(storeName).orElse(null);
      if (version == null || version.getNumber() <= currentVersion.getVersion().getNumber()) {
        return;
      }

      futureVersion = new VersionBackend(version);
      futureVersion.subscribe(subscription).whenComplete((v, t) -> trySwapCurrentVersion());
    }

    // may be called several times even after version was swapped
    private synchronized void trySwapCurrentVersion() {
      if (futureVersion != null && futureVersion.isReadyToServe(subscription)) {
        setCurrentVersion(futureVersion);
        futureVersion = null;
        trySubscribeFutureVersion();
      }
    }

    private final StoreDataChangedListener storeChangeListener = new StoreDataChangedListener() {
      @Override
      public void handleStoreChanged(Store store) {
        if (store.getName().equals(storeName)) {
          deleteOldVersions();
          trySubscribeFutureVersion();
        }
      }

      @Override
      public void handleStoreDeleted(Store store) {
        if (store.getName().equals(storeName)) {
          delete();
        }
      }
    };
  }


  private static final Logger logger = Logger.getLogger(IngestionController.class);

  private final VeniceConfigLoader configLoader;
  private final ReadOnlyStoreRepository storeRepository;
  private final StorageService storageService;
  private final StoreIngestionService ingestionService;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final Map<String, StoreBackend> storeByNameMap = new VeniceConcurrentHashMap<>();
  private final Map<String, VersionBackend> versionByTopicMap = new VeniceConcurrentHashMap<>();

  public IngestionController(
      VeniceConfigLoader configLoader,
      ReadOnlyStoreRepository storeRepository,
      StorageService storageService,
      StoreIngestionService ingestionService) {
    this.configLoader = configLoader;
    this.storeRepository = storeRepository;
    this.storageService = storageService;
    this.ingestionService = ingestionService;
    ingestionService.addNotifier(ingestionListener);
  }

  public synchronized void start() {
    for (AbstractStorageEngine storageEngine : storageService.getStorageEngineRepository().getAllLocalStorageEngines()) {
      String kafkaTopicName = storageEngine.getName();
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);

      if (storeByNameMap.containsKey(storeName)) {
        // We have discovered the current version for the store, so we will delete all other local versions.
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      }

      Version version = getLatestVersion(storeName).orElseGet(null);
      if (version == null || version.kafkaTopicName().equals(kafkaTopicName) == false) {
        // If the version is not the latest, it should be removed.
        storageService.removeStorageEngine(kafkaTopicName);
        continue;
      }

      // subscribe() make sures a current version for the store is determined and stored inside storeByNameMap.
      subscribe(storeName, version, storageEngine.getPartitionIds());
    }
  }

  @Override
  public synchronized void close() {
    for (StoreBackend store : storeByNameMap.values()) {
      store.close();
    }
    storeByNameMap.clear();
    versionByTopicMap.clear();

    executor.shutdown();
    try {
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public StoreBackend getStoreOrThrow(String storeName) {
    StoreBackend store = storeByNameMap.get(storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, Optional.of("Store does not locally exist, storeName=" + storeName));
    }
    return store;
  }

  public synchronized CompletableFuture<Void> subscribe(String storeName, Set<Integer> partitions) {
    StoreBackend store = storeByNameMap.computeIfAbsent(storeName, StoreBackend::new);
    return store.subscribe(Optional.empty(), partitions);
  }

  private synchronized CompletableFuture<Void> subscribe(String storeName, Version version, Set<Integer> partitions) {
    StoreBackend store = storeByNameMap.computeIfAbsent(storeName, StoreBackend::new);
    return store.subscribe(Optional.of(version), partitions);
  }

  public synchronized CompletableFuture<Void> unsubscribe(String storeName, Set<Integer> partitions) {
    StoreBackend store = getStoreOrThrow(storeName);
    // TODO: implement StoreBackend::unsubscribe
    return CompletableFuture.completedFuture(null);
  }

  private Optional<Version> getLatestVersion(String storeName) {
    try {
      return getLatestVersion(storeRepository.getStoreOrThrow(storeName));
    } catch (VeniceNoStoreException e) {
      return Optional.empty();
    }
  }

  public static Optional<Version> getLatestVersion(Store store) {
    return store.getVersions().stream().max(Comparator.comparing(Version::getNumber));
  }

  private final VeniceNotifier ingestionListener = new VeniceNotifier() {
    @Override
    public void completed(String kafkaTopic, int partitionId, long offset) {
      VersionBackend version = versionByTopicMap.get(kafkaTopic);
      if (version != null) {
        version.completeSubPartition(partitionId);
      }
    }
  };
}
