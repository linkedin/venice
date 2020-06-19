package com.linkedin.davinci.client;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Time;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.*;

public class VersionBackend {
  private static final Logger logger = Logger.getLogger(VersionBackend.class);

  private final DaVinciBackend backend;
  private final Version version;
  private final VeniceStoreConfig config;
  private final AtomicReference<AbstractStorageEngine> storageEngine = new AtomicReference<>();
  private final Map<Integer, CompletableFuture> partitionFutures = new ConcurrentHashMap<>();

  VersionBackend(DaVinciBackend backend, Version version) {
    this.backend = backend;
    this.version = version;
    this.config = backend.getConfigLoader().getStoreConfig(version.kafkaTopicName());
    storageEngine.set(backend.getStorageService().getStorageEngineRepository().getLocalStorageEngine(version.kafkaTopicName()));
    backend.getVersionByTopicMap().put(version.kafkaTopicName(), this);
  }

  synchronized void close() {
    backend.getVersionByTopicMap().remove(version.kafkaTopicName());
    for (Map.Entry<Integer, CompletableFuture> entry : partitionFutures.entrySet()) {
      backend.getIngestionService().stopConsumption(config, entry.getKey());
      entry.getValue().cancel(true);
    }

    for (Map.Entry<Integer, CompletableFuture> entry : partitionFutures.entrySet()) {
      try {
        makeSurePartitionIsNotConsuming(entry.getKey());
      } catch (InterruptedException e) {
        logger.warn("Waiting for partition is not consuming was interrupted", e);
        currentThread().interrupt();
        break;
      }
    }
  }

  synchronized void delete() {
    close();
    for (Map.Entry<Integer, CompletableFuture> entry : partitionFutures.entrySet()) {
      backend.getStorageService().dropStorePartition(config, entry.getKey());
    }
  }

  public Version getVersion() {
    return version;
  }

  public AbstractStorageEngine getStorageEngine() {
    AbstractStorageEngine engine = storageEngine.get();
    if (engine == null) {
      throw new VeniceException("Storage engine is not ready, version=" + version.kafkaTopicName());
    }
    return engine;
  }

  public boolean isReadyToServe(Integer subPartitionId) {
    CompletableFuture future = partitionFutures.get(subPartitionId);
    return future != null && future.isDone();
  }

  synchronized boolean isReadyToServe(Set<Integer> subPartitions) {
    return subPartitions.stream().allMatch(this::isReadyToServe);
  }

  synchronized CompletableFuture subscribe(Set<Integer> partitions) {
    for (Integer id : partitions) {
      if (id < 0 || id >= version.getPartitionCount()) {
        String msg = "Cannot subscribe to out of bounds partition" +
                         ", kafkaTopic=" + version.kafkaTopicName() +
                         ", partition=" + id +
                         ", partitionCount=" + version.getPartitionCount();
        throw new VeniceException(msg);
      }
    }

    int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
    logger.info("Amplification factor: " + amplificationFactor);
    logger.info("Subscribing to partitions: " + partitions.toString());
    Set<Integer> subPartitions = PartitionUtils.getSubPartitions(partitions, amplificationFactor);
    logger.info("Subscribing to sub-partitions: " + subPartitions.toString());
    List<CompletableFuture> futures = new ArrayList<>(subPartitions.size());
    for (Integer id : subPartitions) {
      futures.add(subscribeSubPartition(id));
    }

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
  }

  synchronized void unsubscribe(Set<Integer> partitions) {
    for (Integer id : partitions) {
      if (id < 0 || id >= version.getPartitionCount()) {
        String msg = "Cannot unsubscribe from out of bounds partition" +
            ", kafkaTopic=" + version.kafkaTopicName() +
            ", partition=" + id +
            ", partitionCount=" + version.getPartitionCount();
        throw new VeniceException(msg);
      }
    }

    int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
    logger.info("Amplification factor: " + amplificationFactor);
    logger.info("Unsubscribing from partitions: " + partitions.toString());
    Set<Integer> subPartitions = PartitionUtils.getSubPartitions(partitions, amplificationFactor);
    logger.info("Unsubscribing from sub-partitions: " + subPartitions.toString());
    for (Integer id : subPartitions) {
      unsubscribeSubPartition(id);
    }

    for (Integer id : subPartitions) {
      try {
        makeSurePartitionIsNotConsuming(id);
      } catch (InterruptedException e) {
        logger.warn("Waiting for partition is not consuming was interrupted", e);
        currentThread().interrupt();
        return;
      }
      backend.getStorageService().dropStorePartition(config, id);
    }
  }

  private synchronized CompletableFuture subscribeSubPartition(int subPartitionId) {
    storageEngine.set(backend.getStorageService().openStoreForNewPartition(config, subPartitionId));
    backend.getIngestionService().startConsumption(config, subPartitionId, false);
    return partitionFutures.computeIfAbsent(subPartitionId, k -> new CompletableFuture());
  }

  private synchronized void unsubscribeSubPartition(int partitionId) {
    backend.getIngestionService().stopConsumption(config, partitionId);
    partitionFutures.get(partitionId).cancel(true);
    partitionFutures.remove(partitionId);
  }

  synchronized void completeSubPartition(int subPartitionId) {
    partitionFutures.computeIfAbsent(subPartitionId, k -> new CompletableFuture()).complete(null);
  }

  private void makeSurePartitionIsNotConsuming(int partitionId) throws InterruptedException {
    final int SLEEP_SECONDS = 3;
    final int RETRY_NUM = 100; // 5 min
    for (int i = 0; i < RETRY_NUM; i++) {
      if (!backend.getIngestionService().isPartitionConsuming(config, partitionId)) {
        return;
      }
      sleep(SLEEP_SECONDS * Time.MS_PER_SECOND);
    }
    throw new VeniceException("Partition: " + partitionId + " of store: " + config.getStoreName()
                                  + " is still consuming after waiting for it to stop for " + RETRY_NUM * SLEEP_SECONDS + " seconds.");
  }
}
