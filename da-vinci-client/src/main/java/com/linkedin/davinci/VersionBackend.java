package com.linkedin.davinci;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.io.BinaryDecoder;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.venice.ConfigKeys.*;


public class VersionBackend {
  private static final Logger logger = Logger.getLogger(VersionBackend.class);

  private static final int DEFAULT_PUSH_STATUS_HEARTBEAT_INTERVAL_IN_SECONDS = 10;

  private final DaVinciBackend backend;
  private final Version version;
  private final VeniceStoreConfig config;
  private final VenicePartitioner partitioner;
  private final int subPartitionCount;
  private final boolean reportPushStatus;
  private final boolean suppressLiveUpdates;
  private final AtomicReference<AbstractStorageEngine> storageEngine = new AtomicReference<>();
  private final Map<Integer, CompletableFuture> subPartitionFutures = new VeniceConcurrentHashMap<>();
  private final int stopConsumptionWaitRetriesNum;

  /*
   * if daVinciPushStatusStoreEnabled, VersionBackend will schedule a periodic job sending heartbeats
   * to PushStatusStore. The heartbeat job will be cancelled once the push completes or VersionBackend is closed.
   */
  private Future heartbeat;
  private final int heartbeatInterval;

  VersionBackend(DaVinciBackend backend, Version version) {
    this.backend = backend;
    this.version = version;
    this.config = backend.getConfigLoader().getStoreConfig(version.kafkaTopicName());
    if (this.config.getIngestionMode().equals(IngestionMode.ISOLATED)) {
      /*
       * Explicitly disable the store restore since we don't want to open other partitions that should be controlled by
       * child process. All the finished partitions will be closed by child process and reopened in parent process.
       */
      this.config.setRestoreDataPartitions(false);
      this.config.setRestoreMetadataPartition(false);
    }
    this.partitioner = PartitionUtils.getVenicePartitioner(version.getPartitionerConfig());
    this.subPartitionCount = version.getPartitionCount() * version.getPartitionerConfig().getAmplificationFactor();
    this.suppressLiveUpdates = this.config.freezeIngestionIfReadyToServeOrLocalDataExists();
    this.storageEngine.set(backend.getStorageService().getStorageEngine(version.kafkaTopicName()));
    this.backend.getIngestionBackend().setStorageEngineReference(version.kafkaTopicName(), storageEngine);
    Store store = backend.getStoreRepository().getStoreOrThrow(version.getStoreName());
    // push status store must be enabled both in Da Vinci and the store
    this.reportPushStatus = store.isDaVinciPushStatusStoreEnabled() &&
      this.config.getClusterProperties().getBoolean(PUSH_STATUS_STORE_ENABLED, false);
    this.heartbeatInterval = this.config.getClusterProperties().getInt(
        PUSH_STATUS_STORE_HEARTBEAT_INTERVAL_IN_SECONDS,
        DEFAULT_PUSH_STATUS_HEARTBEAT_INTERVAL_IN_SECONDS);
    this.stopConsumptionWaitRetriesNum = backend.getConfigLoader().getCombinedProperties().getInt(
        SERVER_STOP_CONSUMPTION_WAIT_RETRIES_NUM, 60);
    backend.getVersionByTopicMap().put(version.kafkaTopicName(), this);
  }

  synchronized void close() {
    logger.info("Closing local version " + this);
    backend.getVersionByTopicMap().remove(version.kafkaTopicName(), this);
    if (heartbeat != null) {
      heartbeat.cancel(true);
    }
    for (Map.Entry<Integer, CompletableFuture> entry : subPartitionFutures.entrySet()) {
      entry.getValue().cancel(true);
    }
    backend.getIngestionBackend().killConsumptionTask(version.kafkaTopicName());
  }

  synchronized void delete() {
    logger.info("Deleting local version " + this);
    close();
    backend.getIngestionBackend().removeStorageEngine(version.kafkaTopicName());
  }

  @Override
  public String toString() {
    return version.kafkaTopicName();
  }

  Version getVersion() {
    return version;
  }

  private AbstractStorageEngine getStorageEngineOrThrow() {
    AbstractStorageEngine engine = storageEngine.get();
    if (engine == null) {
      throw new VeniceException("Storage engine is not ready, version=" + this);
    }
    return engine;
  }

  boolean isReportingPushStatus() {
    return reportPushStatus;
  }

  synchronized void tryStartHeartbeat() {
    if (isReportingPushStatus() && heartbeat == null) {
      heartbeat = backend.getExecutor().scheduleAtFixedRate(
          () -> {
            try {
              backend.getPushStatusStoreWriter().writeHeartbeat(version.getStoreName());
            } catch (Throwable t) {
              logger.error("Unable to send heartbeat for " + this);
            }
          },
          0,
          heartbeatInterval,
          TimeUnit.SECONDS);
    }
  }

  synchronized void tryStopHeartbeat() {
    if (heartbeat != null && subPartitionFutures.values().stream().allMatch(CompletableFuture::isDone)) {
      heartbeat.cancel(true);
      heartbeat = null;
    }
  }

  public <V> V read(
      int subPartition,
      byte[] keyBytes,
      AbstractAvroChunkingAdapter<V> chunkingAdaptor,
      BinaryDecoder binaryDecoder,
      ByteBuffer reusableRawValue,
      V reusableValue) {
    return chunkingAdaptor.get(
        version.getStoreName(),
        getStorageEngineOrThrow(),
        subPartition,
        keyBytes,
        reusableRawValue,
        reusableValue,
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

  synchronized CompletableFuture<Void> subscribe(ComplementSet<Integer> partitions) {
    Instant startTime = Instant.now();
    List<Integer> subPartitions = getSubPartitions(partitions);
    logger.info("Subscribing to sub-partitions " + subPartitions + " of " + this);

    List<CompletableFuture> futures = new ArrayList<>(subPartitions.size());
    for (Integer id : subPartitions) {
      futures.add(subscribeSubPartition(id));
    }

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .whenComplete((v, e) -> {
          StoreBackend storeBackend = backend.getStoreOrThrow(version.getStoreName());
          storeBackend.getStats().recordSubscribeDuration(Duration.between(startTime, Instant.now()));
        });
  }

  synchronized void unsubscribe(ComplementSet<Integer> partitions) {
    List<Integer> subPartitions = getSubPartitions(partitions);
    logger.info("Unsubscribing from sub-partitions " + subPartitions + " of " + this);
    for (Integer id : subPartitions) {
      unsubscribeSubPartition(id);
    }
  }

  public int getUserPartition(int subPartition) {
    int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
    return PartitionUtils.getUserPartition(subPartition, amplificationFactor);
  }

  public int getSubPartition(byte[] keyBytes) {
    return partitioner.getPartitionId(keyBytes, subPartitionCount);
  }

  private List<Integer> getSubPartitions(ComplementSet<Integer> partitions) {
    int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
    return PartitionUtils.getSubPartitions(
        IntStream.range(0, version.getPartitionCount())
            .filter(partitions::contains)
            .boxed()
            .collect(Collectors.toList()),
        amplificationFactor);
  }

  public int getAmplificationFactor() {
    PartitionerConfig partitionerConfig = version.getPartitionerConfig();
    return partitionerConfig == null ? 1 : partitionerConfig.getAmplificationFactor();
  }

  public boolean isSubPartitionSubscribed(Integer subPartition) {
    return subPartitionFutures.containsKey(subPartition);
  }

  public boolean isSubPartitionReadyToServe(Integer subPartition) {
    CompletableFuture future = subPartitionFutures.get(subPartition);
    return future != null && future.isDone() && !future.isCompletedExceptionally();
  }

  private synchronized CompletableFuture subscribeSubPartition(int subPartition) {
    CompletableFuture future = subPartitionFutures.get(subPartition);
    if (future != null) {
      logger.info("Sub-partition " + subPartition + " of " + this + " is subscribed, ignoring subscribe request.");
      return future;
    }

    // If live update suppression is enabled and local data exists, don't start ingestion and report ready to serve.
    AbstractStorageEngine engine = storageEngine.get();
    if (suppressLiveUpdates && engine != null && engine.containsPartition(subPartition)) {
      return subPartitionFutures.computeIfAbsent(subPartition, k -> CompletableFuture.completedFuture(null));
    }
    // AtomicReference of storage engine will be updated internally.
    backend.getIngestionBackend().startConsumption(config, subPartition);
    tryStartHeartbeat();
    return subPartitionFutures.computeIfAbsent(subPartition, k -> new CompletableFuture());
  }

  private synchronized void unsubscribeSubPartition(int subPartition) {
    if (!subPartitionFutures.containsKey(subPartition)) {
      logger.warn("Sub-partition " + subPartition + " of " + this + " is not subscribed, ignoring unsubscribe request.");
      return;
    }
    completeSubPartition(subPartition);


    backend.getIngestionBackend().dropStoragePartitionGracefully(config, subPartition, stopConsumptionWaitRetriesNum);
    subPartitionFutures.remove(subPartition);
    tryStopHeartbeat();
  }

  void completeSubPartition(int subPartition) {
    logger.info("Sub-partition " + subPartition + " of " + this + " is ready to serve.");
    subPartitionFutures.computeIfAbsent(subPartition, k -> new CompletableFuture()).complete(null);
    tryStopHeartbeat();
  }

  void completeSubPartitionExceptionally(int subPartition, Throwable failure) {
    logger.warn("Failed to subscribe to sub-partition " + subPartition + " of " + this, failure);
    subPartitionFutures.computeIfAbsent(subPartition, k -> new CompletableFuture()).completeExceptionally(failure);
    tryStopHeartbeat();
  }
}
