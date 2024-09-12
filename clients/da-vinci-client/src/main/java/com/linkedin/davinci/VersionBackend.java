package com.linkedin.davinci;

import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_HEARTBEAT_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_STOP_CONSUMPTION_TIMEOUT_IN_SECONDS;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.listener.response.NoOpReadResponseStats;
import com.linkedin.davinci.notifier.DaVinciPushStatusUpdateTask;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VersionBackend {
  private static final Logger LOGGER = LogManager.getLogger(VersionBackend.class);

  private static final int DEFAULT_PUSH_STATUS_HEARTBEAT_INTERVAL_IN_SECONDS = 10;

  private final DaVinciBackend backend;
  private final Version version;
  private final VeniceStoreVersionConfig config;
  private final VenicePartitioner partitioner;
  private final boolean reportPushStatus;
  private final boolean suppressLiveUpdates;
  private final AtomicReference<AbstractStorageEngine> storageEngine = new AtomicReference<>();
  private final Map<Integer, CompletableFuture<Void>> partitionFutures = new VeniceConcurrentHashMap<>();
  private final int stopConsumptionTimeoutInSeconds;
  private final StoreBackendStats storeBackendStats;
  private final StoreDeserializerCache storeDeserializerCache;
  private final Lazy<VeniceCompressor> compressor;

  /*
   * if daVinciPushStatusStoreEnabled, VersionBackend will schedule a periodic job sending heartbeats
   * to PushStatusStore. The heartbeat job will be cancelled once the push completes or VersionBackend is closed.
   */
  private Future heartbeat;
  private final int heartbeatInterval;
  private final DaVinciPushStatusUpdateTask daVinciPushStatusUpdateTask;

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
    this.partitioner = PartitionUtils.getUserPartitionLevelVenicePartitioner(version.getPartitionerConfig());
    this.suppressLiveUpdates = this.config.freezeIngestionIfReadyToServeOrLocalDataExists();
    this.storageEngine.set(backend.getStorageService().getStorageEngine(version.kafkaTopicName()));
    this.backend.getIngestionBackend().setStorageEngineReference(version.kafkaTopicName(), storageEngine);
    Store store = backend.getStoreRepository().getStoreOrThrow(version.getStoreName());
    this.storeBackendStats = storeBackendStats;
    // push status store must be enabled both in Da Vinci and the store
    this.reportPushStatus = store.isDaVinciPushStatusStoreEnabled()
        && this.config.getClusterProperties().getBoolean(PUSH_STATUS_STORE_ENABLED, false);
    this.heartbeatInterval = this.config.getClusterProperties()
        .getInt(PUSH_STATUS_STORE_HEARTBEAT_INTERVAL_IN_SECONDS, DEFAULT_PUSH_STATUS_HEARTBEAT_INTERVAL_IN_SECONDS);
    this.stopConsumptionTimeoutInSeconds =
        backend.getConfigLoader().getCombinedProperties().getInt(SERVER_STOP_CONSUMPTION_TIMEOUT_IN_SECONDS, 60);
    this.storeDeserializerCache = backend.getStoreOrThrow(store.getName()).getStoreDeserializerCache();
    this.compressor = Lazy.of(
        () -> backend.getCompressorFactory().getCompressor(version.getCompressionStrategy(), version.kafkaTopicName()));
    backend.getVersionByTopicMap().put(version.kafkaTopicName(), this);
    long daVinciPushStatusCheckIntervalInMs = this.config.getDaVinciPushStatusCheckIntervalInMs();
    if (daVinciPushStatusCheckIntervalInMs >= 0) {
      this.daVinciPushStatusUpdateTask = new DaVinciPushStatusUpdateTask(
          version,
          daVinciPushStatusCheckIntervalInMs,
          backend.getPushStatusStoreWriter(),
          this::areAllPartitionFuturesCompletedSuccessfully);
      this.daVinciPushStatusUpdateTask.start();
    } else {
      this.daVinciPushStatusUpdateTask = null;
    }
  }

  synchronized void close() {
    LOGGER.info("Closing local version {}", this);
    // TODO: Consider if all of the below calls to the backend could be merged into a single function.
    backend.getVersionByTopicMap().remove(version.kafkaTopicName(), this);
    backend.getIngestionBackend().setStorageEngineReference(version.kafkaTopicName(), null);
    if (heartbeat != null) {
      heartbeat.cancel(true);
    }
    for (Map.Entry<Integer, CompletableFuture<Void>> entry: partitionFutures.entrySet()) {
      entry.getValue().cancel(true);
    }
    try {
      backend.getIngestionBackend().shutdownIngestionTask(version.kafkaTopicName());
    } catch (VeniceException e) {
      LOGGER.error("Encounter exception when killing consumption task: {}", version.kafkaTopicName(), e);
    }
    if (daVinciPushStatusUpdateTask != null) {
      daVinciPushStatusUpdateTask.shutdown();
    }
  }

  synchronized void delete() {
    LOGGER.info("Deleting local version {}", this);
    close();
    final String topicName = version.kafkaTopicName();
    try {
      try {
        backend.getIngestionBackend().removeStorageEngine(topicName);
      } catch (Exception e) {
        // defensive coding
        LOGGER.error("Encountered exception while removing storage engine: {}", topicName, e);
      }
      /**
       * The following function is used to forcibly clean up any leaking data partitions, which are not
       * visibile to the corresponding {@link AbstractStorageEngine} since some data partitions can fail
       * to open because of DaVinci memory limiter.
       */
      backend.getStorageService().forceStorageEngineCleanup(topicName);
      backend.getCompressorFactory().removeVersionSpecificCompressor(topicName);
    } catch (VeniceException e) {
      LOGGER.error("Encounter exception when removing version storage of topic {}", topicName, e);
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
      heartbeat = backend.getExecutor().scheduleAtFixedRate(() -> {
        try {
          backend.getPushStatusStoreWriter().writeHeartbeat(version.getStoreName());
        } catch (Throwable t) {
          LOGGER.error("Unable to send heartbeat for {}", this);
        }
      }, 0, heartbeatInterval, TimeUnit.SECONDS);
    }
  }

  synchronized void tryStopHeartbeat() {
    if (heartbeat != null && partitionFutures.values().stream().allMatch(CompletableFuture::isDone)) {
      heartbeat.cancel(true);
      heartbeat = null;
    }
  }

  public <V> V read(
      int userPartition,
      byte[] keyBytes,
      AbstractAvroChunkingAdapter<V> chunkingAdaptor,
      StoreDeserializerCache<V> storeDeserializerCache,
      int readerSchemaId,
      BinaryDecoder binaryDecoder,
      ByteBuffer reusableRawValue,
      V reusableValue) {
    return chunkingAdaptor.get(
        getStorageEngineOrThrow(),
        userPartition,
        keyBytes,
        reusableRawValue,
        reusableValue,
        binaryDecoder,
        version.isChunkingEnabled(),
        NoOpReadResponseStats.SINGLETON,
        readerSchemaId,
        storeDeserializerCache,
        compressor.get());
  }

  public GenericRecord compute(
      int userPartition,
      byte[] keyBytes,
      AbstractAvroChunkingAdapter<GenericRecord> chunkingAdaptor,
      AvroStoreDeserializerCache<GenericRecord> storeDeserializerCache,
      int readerSchemaId,
      BinaryDecoder binaryDecoder,
      ByteBuffer reusableRawValue,
      GenericRecord reusableValueRecord,
      Map<String, Object> sharedContext,
      ComputeRequestWrapper computeRequestWrapper,
      Schema computeResultSchema) {

    reusableValueRecord = chunkingAdaptor.get(
        getStorageEngineOrThrow(),
        userPartition,
        keyBytes,
        reusableRawValue,
        reusableValueRecord,
        binaryDecoder,
        version.isChunkingEnabled(),
        NoOpReadResponseStats.SINGLETON,
        readerSchemaId,
        storeDeserializerCache,
        compressor.get());

    return ComputeUtils.computeResult(
        computeRequestWrapper.getOperations(),
        computeRequestWrapper.getOperationResultFields(),
        sharedContext,
        reusableValueRecord,
        computeResultSchema);
  }

  public void computeWithKeyPrefixFilter(
      byte[] keyPrefix,
      int partition,
      StreamingCallback<GenericRecord, GenericRecord> callback,
      ComputeRequestWrapper computeRequestWrapper,
      AbstractAvroChunkingAdapter<GenericRecord> chunkingAdaptor,
      RecordDeserializer<GenericRecord> keyRecordDeserializer,
      GenericRecord reusableValueRecord,
      BinaryDecoder reusableBinaryDecoder,
      Map<String, Object> sharedContext,
      Schema computeResultSchema) {

    StreamingCallback<GenericRecord, GenericRecord> computingCallback =
        new StreamingCallback<GenericRecord, GenericRecord>() {
          @Override
          public void onRecordReceived(GenericRecord key, GenericRecord value) {
            GenericRecord computeResult = ComputeUtils.computeResult(
                computeRequestWrapper.getOperations(),
                computeRequestWrapper.getOperationResultFields(),
                sharedContext,
                value,
                computeResultSchema);
            callback.onRecordReceived(key, computeResult);
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            if (exception.isPresent()) {
              throw new VeniceException(ExceptionUtils.compactExceptionDescription(exception.get()));
            }
          }
        };

    chunkingAdaptor.getByPartialKey(
        getStorageEngineOrThrow(),
        partition,
        keyPrefix,
        reusableValueRecord,
        reusableBinaryDecoder,
        keyRecordDeserializer,
        this.version.isChunkingEnabled(),
        getSupersetOrLatestValueSchemaId(),
        this.storeDeserializerCache,
        this.compressor.get(),
        computingCallback);
  }

  public int getPartitionCount() {
    return version.getPartitionCount();
  }

  public int getPartition(byte[] keyBytes) {
    return partitioner.getPartitionId(keyBytes, version.getPartitionCount());
  }

  public boolean isPartitionSubscribed(int partition) {
    return partitionFutures.containsKey(partition);
  }

  public boolean isPartitionReadyToServe(int partition) {
    CompletableFuture<Void> future = partitionFutures.get(partition);
    return future != null && future.isDone() && !future.isCompletedExceptionally();
  }

  public int getSupersetOrLatestValueSchemaId() {
    return backend.getSchemaRepository().getSupersetOrLatestValueSchema(version.getStoreName()).getId();
  }

  synchronized boolean isReadyToServe(ComplementSet<Integer> partitions) {
    return getPartitions(partitions).stream().allMatch(this::isPartitionReadyToServe);
  }

  synchronized CompletableFuture<Void> subscribe(ComplementSet<Integer> partitions) {
    Instant startTime = Instant.now();
    List<Integer> partitionList = getPartitions(partitions);
    LOGGER.info("Subscribing to partitions {} of {}", partitionList, this);
    List<CompletableFuture<Void>> futures = new ArrayList<>(partitionList.size());
    for (int partition: partitionList) {
      AbstractStorageEngine engine = storageEngine.get();
      if (partitionFutures.containsKey(partition)) {
        LOGGER.info("Partition {} of {}  is subscribed, ignoring subscribe request.", partition, this);
      } else if (suppressLiveUpdates && engine != null && engine.containsPartition(partition)) {
        // If live update suppression is enabled and local data exists, don't start ingestion and report ready to serve.
        partitionFutures.computeIfAbsent(partition, k -> CompletableFuture.completedFuture(null));
      } else {
        partitionFutures.computeIfAbsent(partition, k -> new CompletableFuture<>());
        // AtomicReference of storage engine will be updated internally.
        backend.getIngestionBackend().startConsumption(config, partition);
        tryStartHeartbeat();
      }
      futures.add(partitionFutures.get(partition));
    }

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((v, e) -> {
      storeBackendStats.recordSubscribeDuration(Duration.between(startTime, Instant.now()));
    });
  }

  synchronized void unsubscribe(ComplementSet<Integer> partitions) {
    List<Integer> partitionList = getPartitions(partitions);
    LOGGER.info("Unsubscribing from partitions {} of {}", partitions, this);

    for (int partition: partitionList) {
      if (!partitionFutures.containsKey(partition)) {
        LOGGER.warn("Partition {} of {} is not subscribed, ignoring unsubscribe request.", partition, this);
        return;
      }
      completePartition(partition);
      backend.getIngestionBackend().dropStoragePartitionGracefully(config, partition, stopConsumptionTimeoutInSeconds);
      partitionFutures.remove(partition);
    }
    tryStopHeartbeat();
  }

  void completePartition(int partition) {
    LOGGER.info("Partition {} of {} is ready to serve.", partition, this);
    partitionFutures.computeIfAbsent(partition, k -> new CompletableFuture<>()).complete(null);
  }

  void completePartitionExceptionally(int partition, Throwable failure) {
    LOGGER.warn("Failed to subscribe to partition {} of {}", partition, this, failure);
    partitionFutures.computeIfAbsent(partition, k -> new CompletableFuture<>()).completeExceptionally(failure);
  }

  boolean areAllPartitionFuturesCompletedSuccessfully() {
    if (partitionFutures == null || partitionFutures.isEmpty()) {
      return false;
    }
    return partitionFutures.values().stream().allMatch(f -> (f.isDone() && !f.isCompletedExceptionally()));
  }

  private List<Integer> getPartitions(ComplementSet<Integer> partitions) {
    return IntStream.range(0, version.getPartitionCount())
        .filter(partitions::contains)
        .boxed()
        .collect(Collectors.toList());
  }

  public void updatePartitionStatus(int partition, ExecutionStatus status) {
    if (daVinciPushStatusUpdateTask != null) {
      daVinciPushStatusUpdateTask.updatePartitionStatus(partition, status);
    }
  }
}
