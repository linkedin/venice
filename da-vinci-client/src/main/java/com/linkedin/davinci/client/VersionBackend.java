package com.linkedin.davinci.client;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import org.apache.avro.io.BinaryDecoder;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.*;

public class VersionBackend {
  private static final Logger logger = Logger.getLogger(VersionBackend.class);

  private final DaVinciBackend backend;
  private final Version version;
  private final VeniceStoreConfig config;
  private final VenicePartitioner partitioner;
  private final int subPartitionCount;
  private final AtomicReference<AbstractStorageEngine> storageEngine = new AtomicReference<>();
  private final Map<Integer, CompletableFuture> subPartitionFutures = new VeniceConcurrentHashMap<>();

  VersionBackend(DaVinciBackend backend, Version version) {
    this.backend = backend;
    this.version = version;
    this.config = backend.getConfigLoader().getStoreConfig(version.kafkaTopicName());
    this.partitioner = PartitionUtils.getVenicePartitioner(version.getPartitionerConfig());
    this.subPartitionCount = version.getPartitionCount() * version.getPartitionerConfig().getAmplificationFactor();
    storageEngine.set(backend.getStorageService().getStorageEngineRepository().getLocalStorageEngine(version.kafkaTopicName()));
    backend.getVersionByTopicMap().put(version.kafkaTopicName(), this);
  }

  synchronized void close() {
    backend.getVersionByTopicMap().remove(version.kafkaTopicName());
    for (Map.Entry<Integer, CompletableFuture> entry : subPartitionFutures.entrySet()) {
      backend.getIngestionService().stopConsumption(config, entry.getKey());
      entry.getValue().cancel(true);
    }

    for (Map.Entry<Integer, CompletableFuture> entry : subPartitionFutures.entrySet()) {
      try {
        makeSureSubPartitionIsNotConsuming(entry.getKey());
      } catch (InterruptedException e) {
        logger.warn("Waiting for partition to stop consumption was interrupted", e);
        currentThread().interrupt();
        break;
      }
    }
  }

  synchronized void delete() {
    logger.info("Deleting local version " + this);
    close();
    for (Map.Entry<Integer, CompletableFuture> entry : subPartitionFutures.entrySet()) {
      backend.getStorageService().dropStorePartition(config, entry.getKey());
    }
  }

  @Override
  public String toString() {
    return version.kafkaTopicName();
  }

  Version getVersion() {
    return version;
  }

  public AbstractStorageEngine getStorageEngine() {
    AbstractStorageEngine engine = storageEngine.get();
    if (engine == null) {
      throw new VeniceException("Storage engine is not ready, version=" + version.kafkaTopicName());
    }
    return engine;
  }

  public <V> V read(
      int subPartition,
      byte[] keyBytes,
      AbstractAvroChunkingAdapter<V> chunkingAdaptor,
      BinaryDecoder binaryDecoder,
      ByteBuffer reusedRawValue,
      V reusedValue) {
    return chunkingAdaptor.get(
        version.getStoreName(),
        getStorageEngine(),
        subPartition,
        keyBytes,
        reusedRawValue,
        reusedValue,
        binaryDecoder,
        version.isChunkingEnabled(),
        version.getCompressionStrategy(),
        true,
        backend.getSchemaRepository(),
        null);
  }

  synchronized boolean isReadyToServe(ComplementSet<Integer> partitions) {
    return getSubPartitions(partitions).stream().allMatch(this::isSubPartitionReadyToServe);
  }

  synchronized CompletableFuture subscribe(ComplementSet<Integer> partitions) {
    Set<Integer> subPartitions = getSubPartitions(partitions);
    logger.info("Subscribing to sub-partitions, storeName=" + this + ", subPartitions=" + subPartitions);

    List<CompletableFuture> futures = new ArrayList<>(subPartitions.size());
    for (Integer id : subPartitions) {
      futures.add(subscribeSubPartition(id));
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
  }

  synchronized void unsubscribe(ComplementSet<Integer> partitions) {
    Set<Integer> subPartitions = getSubPartitions(partitions);
    logger.info("Subscribing to sub-partitions, storeName=" + this + ", subPartitions=" + subPartitions);

    for (Integer id : subPartitions) {
      unsubscribeSubPartition(id);
    }

    for (Integer id : subPartitions) {
      try {
        makeSureSubPartitionIsNotConsuming(id);
      } catch (InterruptedException e) {
        logger.warn("Waiting for partition to stop consumption was interrupted", e);
        currentThread().interrupt();
        return;
      }
      backend.getStorageService().dropStorePartition(config, id);
    }
  }

  public int getSubPartition(byte[] keyBytes) {
    return partitioner.getPartitionId(keyBytes, subPartitionCount);
  }

  private Set<Integer> getSubPartitions(ComplementSet<Integer> partitions) {
    int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
    return PartitionUtils.getSubPartitions(
        IntStream.range(0, version.getPartitionCount())
            .filter(partitions::contains)
            .boxed()
            .collect(Collectors.toSet()),
        amplificationFactor);
  }

  public boolean isSubPartitionReadyToServe(Integer subPartition) {
    CompletableFuture future = subPartitionFutures.get(subPartition);
    return future != null && future.isDone();
  }

  private synchronized CompletableFuture subscribeSubPartition(int subPartition) {
    storageEngine.set(backend.getStorageService().openStoreForNewPartition(config, subPartition));
    backend.getIngestionService().startConsumption(config, subPartition, false);
    return subPartitionFutures.computeIfAbsent(subPartition, k -> new CompletableFuture());
  }

  private synchronized void unsubscribeSubPartition(int subPartition) {
    backend.getIngestionService().stopConsumption(config, subPartition);
    CompletableFuture future = subPartitionFutures.remove(subPartition);
    if (future != null) {
      future.cancel(true);
    }
  }

  synchronized void completeSubPartition(int subPartition) {
    subPartitionFutures.computeIfAbsent(subPartition, k -> new CompletableFuture()).complete(null);
  }

  private void makeSureSubPartitionIsNotConsuming(int subPartition) throws InterruptedException {
    final int SLEEP_SECONDS = 3;
    final int RETRY_NUM = 100; // 5 min
    for (int i = 0; i < RETRY_NUM; i++) {
      if (!backend.getIngestionService().isPartitionConsuming(config, subPartition)) {
        return;
      }
      sleep(SLEEP_SECONDS * Time.MS_PER_SECOND);
    }
    throw new VeniceException("Partition: " + subPartition + " of store: " + config.getStoreName()
                                  + " is still consuming after waiting for it to stop for " + RETRY_NUM * SLEEP_SECONDS + " seconds.");
  }
}
