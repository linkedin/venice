package com.linkedin.davinci;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.io.BinaryDecoder;
import org.apache.log4j.Logger;

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
  private final Map<Integer, CompletableFuture> partitionFutures = new VeniceConcurrentHashMap<>();
  private final int stopConsumptionWaitRetriesNum;
  private final StoreBackendStats storeBackendStats;

  /*
   * if daVinciPushStatusStoreEnabled, VersionBackend will schedule a periodic job sending heartbeats
   * to PushStatusStore. The heartbeat job will be cancelled once the push completes or VersionBackend is closed.
   */
  private Future heartbeat;
  private final int heartbeatInterval;

  VersionBackend(DaVinciBackend backend, Version version, StoreBackendStats storeBackendStats) {
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
    this.storeBackendStats = storeBackendStats;
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
    for (Map.Entry<Integer, CompletableFuture> entry : partitionFutures.entrySet()) {
      entry.getValue().cancel(true);
    }
    try {
      backend.getIngestionBackend().killConsumptionTask(version.kafkaTopicName());
    } catch (VeniceException e) {
      logger.error("Encounter exception when killing consumption task: " + e);
    }
  }

  synchronized void delete() {
    logger.info("Deleting local version " + this);
    close();
    final String topicName = version.kafkaTopicName();
    try {
      backend.getIngestionBackend().removeStorageEngine(topicName);
      backend.getCompressorFactory().removeVersionSpecificCompressor(topicName);
    } catch (VeniceException e) {
      logger.error("Encounter exception when removing version storage of topic: " + topicName, e);
    }
  }

  @Override
  public String toString() {
    return version.kafkaTopicName();
  }

  public Version getVersion() {
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
    if (heartbeat != null && partitionFutures.values().stream().allMatch(CompletableFuture::isDone)) {
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
        null,
        backend.getCompressorFactory());
  }

  public int getUserPartition(int subPartition) {
    int amplificationFactor = version.getPartitionerConfig().getAmplificationFactor();
    return PartitionUtils.getUserPartition(subPartition, amplificationFactor);
  }

  public int getSubPartition(byte[] keyBytes) {
    return partitioner.getPartitionId(keyBytes, subPartitionCount);
  }

  public int getAmplificationFactor() {
    PartitionerConfig partitionerConfig = version.getPartitionerConfig();
    return partitionerConfig == null ? 1 : partitionerConfig.getAmplificationFactor();
  }

  public boolean isPartitionSubscribed(int partition) {
    return partitionFutures.containsKey(partition);
  }

  public boolean isPartitionReadyToServe(int partition) {
    CompletableFuture future = partitionFutures.get(partition);
    return future != null && future.isDone() && !future.isCompletedExceptionally();
  }

  synchronized boolean isReadyToServe(ComplementSet<Integer> partitions) {
    return getPartitions(partitions).stream().allMatch(this::isPartitionReadyToServe);
  }

  synchronized CompletableFuture<Void> subscribe(ComplementSet<Integer> partitions) {
    Instant startTime = Instant.now();
    List<Integer> partitionList = getPartitions(partitions);
    logger.info("Subscribing to partitions " + partitionList + " of " + this);
    List<CompletableFuture> futures = new ArrayList<>(partitionList.size());
    for (int partition : partitionList) {
      AbstractStorageEngine engine = storageEngine.get();
      if (partitionFutures.containsKey(partition)) {
        logger.info("Partition " + partition + " of " + this + " is subscribed, ignoring subscribe request.");
      } else if (suppressLiveUpdates && engine != null && engine.containsPartition(partition * getAmplificationFactor())) {
        /**
         * If live update suppression is enabled and local data exists, don't start ingestion and report ready to serve.
         * Note: For now we could not completely hide the amplification concept here, since storage engine is not aware
         * of amplification factor. We only need to check one sub-partition as all sub-partitions of a user partition
         * are always subscribed/unsubscribed together.
         */
        partitionFutures.computeIfAbsent(partition, k -> CompletableFuture.completedFuture(null));
      } else {
        partitionFutures.computeIfAbsent(partition, k -> new CompletableFuture());
        // AtomicReference of storage engine will be updated internally.
        backend.getIngestionBackend().startConsumption(config, partition);
        tryStartHeartbeat();
      }
      futures.add(partitionFutures.get(partition));
    }

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .whenComplete((v, e) -> {
          storeBackendStats.recordSubscribeDuration(Duration.between(startTime, Instant.now()));
        });
  }

  synchronized void unsubscribe(ComplementSet<Integer> partitions) {
    List<Integer> partitionList = getPartitions(partitions);
    logger.info("Unsubscribing from partitions " + partitions + " of " + this);

    for (int partition : partitionList) {
      if (!partitionFutures.containsKey(partition)) {
        logger.warn("Partition " + partition + " of " + this + " is not subscribed, ignoring unsubscribe request.");
        return;
      }
      completePartition(partition);
      backend.getIngestionBackend().dropStoragePartitionGracefully(config, partition, stopConsumptionWaitRetriesNum);
      partitionFutures.remove(partition);
    }
    tryStopHeartbeat();
  }

  void completePartition(int partition) {
    logger.info("Partition " + partition + " of " + this + " is ready to serve.");
    partitionFutures.computeIfAbsent(partition, k -> new CompletableFuture()).complete(null);
  }

  void completePartitionExceptionally(int partition, Throwable failure) {
    logger.warn("Failed to subscribe to partition " + partition + " of " + this, failure);
    partitionFutures.computeIfAbsent(partition, k -> new CompletableFuture()).completeExceptionally(failure);
  }

  private List<Integer> getPartitions(ComplementSet<Integer> partitions) {
    return IntStream.range(0, version.getPartitionCount())
        .filter(partitions::contains)
        .boxed()
        .collect(Collectors.toList());
  }
}
