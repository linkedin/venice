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
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public class IngestionController implements Closeable {
  private final VeniceConfigLoader configLoader;
  private final ReadOnlyStoreRepository storeRepository;
  private final StorageService storageService;
  private final StoreIngestionService ingestionService;

  private final Map<String, Set<Integer>> topicToReadyPartitions = new VeniceConcurrentHashMap<>();
  private final Map<String, Map<Integer, CompletableFuture>> topicToPartitionFutures = new VeniceConcurrentHashMap<>();

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

  public void start() {
  }

  @Override
  public synchronized void close() {
    // stop consumption of all locally present partitions
    for (AbstractStorageEngine<? extends AbstractStoragePartition> engine : storageService.getStorageEngineRepository().getAllLocalStorageEngines()) {
      String kafkaTopic = engine.getName();
      for (int partitionId : engine.getPartitionIds()) {
        ingestionService.stopConsumption(configLoader.getStoreConfig(kafkaTopic), partitionId);
      }
    }

    // TODO: remove notifier from ingestionService
    topicToReadyPartitions.clear();

    for (Map<Integer, CompletableFuture> partitionFutures : topicToPartitionFutures.values()) {
      for (CompletableFuture future : partitionFutures.values()) {
        future.cancel(true);
      }
    }
    topicToPartitionFutures.clear();
  }

  public synchronized CompletableFuture<Void> subscribe(String storeName, Set<Integer> partitions) {
    Store store = storeRepository.getStoreOrThrow(storeName);
    Optional<Version> version = store.getVersions().stream().max(Comparator.comparing(Version::getNumber));
    if (!version.isPresent()) {
      String msg = "Cannot subscribe to empty store " + storeName;
      throw new VeniceException(msg);
    }
    return consumeVersion(version.get(), partitions);
  }

  protected synchronized CompletableFuture<Void> consumeVersion(Version version, Set<Integer> partitions) {
    for (int partitionId : partitions) {
      if (partitionId < 0 || partitionId >= version.getPartitionCount()) {
        String msg = "Cannot subscribe to out of bounds partition" +
                         ", kafkaTopic=" + version.kafkaTopicName() +
                         ", partition=" + partitionId +
                         ", partitionCount=" + version.getPartitionCount();
        throw new VeniceException(msg);
      }
    }

    List<CompletableFuture<Void>> partitionFutures = new ArrayList<>();
    for (int partitionId : partitions) {
      partitionFutures.add(consumePartition(version.kafkaTopicName(), partitionId));
    }
    return CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[partitionFutures.size()]));
  }

  protected synchronized CompletableFuture<Void> consumePartition(String kafkaTopic, int partitionId) {
    VeniceStoreConfig config = configLoader.getStoreConfig(kafkaTopic);
    storageService.openStoreForNewPartition(config, partitionId);
    ingestionService.startConsumption(config, partitionId, false);
    Map<Integer, CompletableFuture> partitionFutures = topicToPartitionFutures.computeIfAbsent(kafkaTopic, k -> new VeniceConcurrentHashMap<>());
    return partitionFutures.computeIfAbsent(partitionId, k -> new CompletableFuture());
  }

  protected synchronized void completePartition(String kafkaTopic, int partitionId) {
    topicToReadyPartitions.computeIfAbsent(kafkaTopic, k -> VeniceConcurrentHashMap.newKeySet()).add(partitionId);
    Map<Integer, CompletableFuture> partitionFutures = topicToPartitionFutures.computeIfAbsent(kafkaTopic, k -> new VeniceConcurrentHashMap<>());
    partitionFutures.computeIfAbsent(partitionId, k -> new CompletableFuture()).complete(null);
  }

  public Set<Integer> getReadyPartitions(String storeName, int versionId) {
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionId);
    return topicToReadyPartitions.getOrDefault(kafkaTopic, Collections.emptySet());
  }

  public boolean isPartitionReady(String storeName, int versionId, int partitionId) {
    return getReadyPartitions(storeName, versionId).contains(partitionId);
  }

  private final VeniceNotifier ingestionListener = new VeniceNotifier() {
    @Override
    public void completed(String kafkaTopic, int partitionId, long offset) {
      completePartition(kafkaTopic, partitionId);
    }
  };
}
