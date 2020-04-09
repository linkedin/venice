package com.linkedin.davinci.client;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ConcurrentRef;
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

    private synchronized CompletableFuture subscribe(Set<Integer> partitions) {
      for (int id : partitions) {
        if (id < 0 || id >= version.getPartitionCount()) {
          String msg = "Cannot subscribe to out of bounds partition" +
                           ", kafkaTopic=" + version.kafkaTopicName() +
                           ", partition=" + id +
                           ", partitionCount=" + version.getPartitionCount();
          throw new VeniceException(msg);
        }
      }

      List<CompletableFuture> futures = new ArrayList<>(partitions.size());
      for (int id : partitions) {
        futures.add(subscribePartition(id));
      }

      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private synchronized CompletableFuture subscribePartition(int partitionId) {
      storageService.openStoreForNewPartition(config, partitionId);
      ingestionService.startConsumption(config, partitionId, false);
      return partitionFutures.computeIfAbsent(partitionId, k -> new CompletableFuture());
    }

    private synchronized void completePartition(int partitionId) {
      partitionFutures.computeIfAbsent(partitionId, k -> new CompletableFuture()).complete(null);
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
    }

    private synchronized void close() {
      if (currentVersion != null) {
        currentVersion.close();
        currentVersion = null;
      }
      if (futureVersion != null) {
        futureVersion.close();
        futureVersion = null;
      }
      subscription.clear();
      currentVersionRef.clear();
    }

    public ConcurrentRef getCurrentVersion() {
      return currentVersionRef.acquire();
    }

    private synchronized CompletableFuture subscribe(Version version, Set<Integer> partitions) {
      if (currentVersion == null) {
        currentVersion = new VersionBackend(version);
        currentVersionRef.reset(currentVersion);
      }

      subscription.addAll(partitions);
      ConcurrentRef ref = currentVersionRef.acquire();
      return currentVersion.subscribe(partitions).whenComplete((v, t) -> ref.release());
    }


    // may be called indirectly by readers, so has to be fast
    private void deleteVersion(VersionBackend version) {
      executor.submit(() -> version.delete());
    }
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
    this.storageService =  storageService;
    this.ingestionService = ingestionService;
    ingestionService.addNotifier(ingestionListener);
  }

  public synchronized void start() {
    for (AbstractStorageEngine<?> storageEngine : storageService.getStorageEngineRepository().getAllLocalStorageEngines()) {
      String kafkaTopicName = storageEngine.getName();
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);
      if (storeByNameMap.containsKey(storeName)) {
        // We have discovered the current version for the store, so we will delete all other local versions.
        storageService.removeStorageEngine(kafkaTopicName);
      } else {
        Version version;
        try {
          version = findLatestVersion(storeName);
        } catch (VeniceException e) {
          // If store does not exists in store repository or store is empty, all local versions should be removed.
          storageService.removeStorageEngine(kafkaTopicName);
          continue;
        }
        if (version.kafkaTopicName().equals(storageEngine.getName())) {
          // subscribe() makes sures a current version for the store is determined and stored inside storeByNameMap.
          subscribe(storeName, version, storageEngine.getPartitionIds());
        } else {
          // If the version is not the latest, it should be removed.
          storageService.removeStorageEngine(kafkaTopicName);
        }
      }
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
      executor.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public StoreBackend getStoreOrThrow(String storeName) {
    StoreBackend store = storeByNameMap.get(storeName);
    if (store == null) {
      throw new VeniceException("Store is not subscribed, storeName=" + storeName);
    }
    return store;
  }

  public synchronized CompletableFuture<Void> subscribe(String storeName, Set<Integer> partitions) {
    StoreBackend store = storeByNameMap.computeIfAbsent(storeName, StoreBackend::new);
    return store.subscribe(findLatestVersion(storeName), partitions);
  }

  public synchronized CompletableFuture<Void> subscribe(String storeName, Version version, Set<Integer> partitions) {
    StoreBackend store = storeByNameMap.computeIfAbsent(storeName, StoreBackend::new);
    return store.subscribe(version, partitions);
  }

  public synchronized CompletableFuture<Void> unsubscribe(String storeName, Set<Integer> partitions) {
    StoreBackend store = getStoreOrThrow(storeName);
    // TODO: implement StoreBackend::unsubscribe
    return CompletableFuture.completedFuture(null);
  }

  private Version findLatestVersion(String storeName) {
    Store store = storeRepository.getStoreOrThrow(storeName);
    Optional<Version> version = store.getVersions().stream().max(Comparator.comparing(Version::getNumber));
    version.orElseThrow(() -> new VeniceException("Cannot subscribe to an empty store, storeName=" + storeName));
    return version.get();
  }

  private final VeniceNotifier ingestionListener = new VeniceNotifier() {
    @Override
    public synchronized void completed(String kafkaTopic, int partitionId, long offset) {
      VersionBackend version = versionByTopicMap.get(kafkaTopic);
      if (version != null) {
        version.completePartition(partitionId);
      }
    }
  };
}
