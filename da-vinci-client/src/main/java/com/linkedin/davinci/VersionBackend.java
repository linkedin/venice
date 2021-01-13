package com.linkedin.davinci;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.ingestion.IngestionRequestClient;
import com.linkedin.davinci.ingestion.IngestionUtils;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;

import org.apache.avro.io.BinaryDecoder;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.davinci.ingestion.IngestionUtils.*;
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

  /**
   * if daVinciPushStatusStoreEnabled, VersionBackend will schedule a periodic job sending heartbeats
   * to PushStatusStore. The heartbeat job will be cancelled once the push completes or VersionBackend is closed.
   */
  private Optional<Future> heartbeat = Optional.empty();
  private int heartbeatInterval;

  VersionBackend(DaVinciBackend backend, Version version) {
    this.backend = backend;
    this.version = version;
    this.config = backend.getConfigLoader().getStoreConfig(version.kafkaTopicName());
    if (this.config.getIngestionMode().equals(IngestionMode.ISOLATED)) {
      /**
       * Explicitly disable the store restore since we don't want to open other partitions that should be controlled by
       * child process. All the finished partitions will be closed by child process and reopened in parent process.
       */
      this.config.setRestoreDataPartitions(false);
      this.config.setRestoreMetadataPartition(false);
    }
    this.partitioner = PartitionUtils.getVenicePartitioner(version.getPartitionerConfig());
    this.subPartitionCount = version.getPartitionCount() * version.getPartitionerConfig().getAmplificationFactor();
    this.suppressLiveUpdates = this.config.freezeIngestionIfReadyToServeOrLocalDataExists();
    this.storageEngine.set(backend.getStorageService().getStorageEngineRepository().getLocalStorageEngine(version.kafkaTopicName()));
    backend.getVersionByTopicMap().put(version.kafkaTopicName(), this);
    String storeName = version.getStoreName();
    Store store = backend.getStoreRepository().getStoreOrThrow(storeName);
    this.reportPushStatus = store.isDaVinciPushStatusStoreEnabled();
    this.heartbeatInterval = this.config.getClusterProperties().getInt(
        PUSH_STATUS_STORE_HEARTBEAT_INTERVAL_IN_SECONDS,
        DEFAULT_PUSH_STATUS_HEARTBEAT_INTERVAL_IN_SECONDS);
  }

  synchronized void close() {
    backend.getVersionByTopicMap().remove(version.kafkaTopicName());
    for (Map.Entry<Integer, CompletableFuture> entry : subPartitionFutures.entrySet()) {
      backend.getIngestionService().stopConsumptionAndWait(config, entry.getKey(), 1, 30);
      entry.getValue().cancel(true);
    }
    heartbeat.ifPresent(f -> f.cancel(true));
  }

  synchronized void delete() {
    logger.info("Deleting local version " + this);
    close();
    for (Map.Entry<Integer, CompletableFuture> entry : subPartitionFutures.entrySet()) {
      backend.getStorageService().dropStorePartition(config, entry.getKey());
    }

    // Send remote request to isolated ingestion service to stop ingestion.
    if (config.getIngestionMode().equals(IngestionMode.ISOLATED)) {
      logger.info("Sending DROP_STORE request to child process to drop metadata partition for "  + this);
      IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
      ingestionTaskCommand.commandType = IngestionCommandType.DROP_STORE.getValue();
      ingestionTaskCommand.topicName = version.kafkaTopicName();
      byte[] content = serializeIngestionTaskCommand(ingestionTaskCommand);
      try {
        IngestionRequestClient client = backend.getIngestionRequestClient();
        HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.COMMAND, content);
        FullHttpResponse response = client.sendRequest(httpRequest);
        logger.info("Received ingestion task report response.");
        byte[] responseContent = new byte[response.content().readableBytes()];
        response.content().readBytes(responseContent);
        IngestionTaskReport ingestionTaskReport = deserializeIngestionTaskReport(responseContent);
        logger.info("Received ingestion task report response: " + ingestionTaskReport);
        // FullHttpResponse is a reference-counted object that requires explicit de-allocation.
        response.release();
      } catch (Exception e) {
        throw new VeniceException("Encounter exception when dropping storage engine opened in child process", e);
      }
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
      throw new VeniceException("Storage engine is not ready, version=" + this);
    }
    return engine;
  }

  public boolean isReportingPushStatus() {
    return this.reportPushStatus;
  }

  synchronized void tryStartHeartbeat() {
    if (!isReportingPushStatus()) {
      logger.info("Push status reporting is not enabled for " + this);
      return;
    }
    if (!heartbeat.isPresent()) {
      heartbeat = Optional.of(backend.getExecutor().scheduleAtFixedRate(
          () -> {
            try {
              backend.getPushStatusStoreWriter().writeHeartbeat(version.getStoreName());
            } catch (Throwable t) {
              logger.error("Unable to send heartbeat for " + this);
            }
          },
          0,
          heartbeatInterval,
          TimeUnit.SECONDS));
    }
  }

  synchronized void tryStopHeartbeat() {
    if (!isReportingPushStatus()) {
      return;
    }
    if (subPartitionFutures.values().stream().allMatch(CompletableFuture::isDone)) {
      heartbeat.ifPresent(f -> f.cancel(true));
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
        getStorageEngine(),
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
    logger.info("Unsubscribing to sub-partitions, storeName=" + this + ", subPartitions=" + subPartitions);
    for (Integer id : subPartitions) {
      unsubscribeSubPartition(id);
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
    // If the partition has been subscribed already, return the future without subscribing again.
    CompletableFuture partitionFuture = subPartitionFutures.get(subPartition);
    if (null != partitionFuture) {
      logger.info("Partition " + subPartition + " of " + this + " has been subscribed already, "
          + "ignore the duplicate subscribe request");
      return partitionFuture;
    }

    /**
     * If live update suppression is enabled and local data exists, don't start ingestion and report ready to serve.
     */
    if (suppressLiveUpdates && storageEngine.get() != null) {
      AbstractStorageEngine engine = storageEngine.get();
      if (engine.containsPartition(subPartition)) {
        return subPartitionFutures.computeIfAbsent(subPartition, k -> CompletableFuture.completedFuture(null));
      }
    }

    if (config.getIngestionMode().equals(IngestionMode.ISOLATED)) {
      backend.getIngestionReportListener().addVersionPartitionToIngestionMap(version.kafkaTopicName(), subPartition);
      IngestionUtils.subscribeTopicPartition(backend.getIngestionRequestClient(), version.kafkaTopicName(), subPartition);
    } else {
      // Create partition in storage engine for ingestion.
      storageEngine.set(backend.getStorageService().openStoreForNewPartition(config, subPartition));
      backend.getIngestionService().startConsumption(config, subPartition);
    }
    tryStartHeartbeat();
    return subPartitionFutures.computeIfAbsent(subPartition, k -> new CompletableFuture());
  }

  synchronized void completeSubPartitionSubscription(int subPartition) {
    logger.info("Partition " + subPartition + " of " + this + " has been completed by ingestion isolation service.");
    // Re-open the storage engine partition in backend.
    storageEngine.set(backend.getStorageService().openStoreForNewPartition(config, subPartition));
    // The consumption task should be re-started on DaVinci side to receive future updates for hybrid stores and consumer
    // action messages for all stores. The partition and its corresponding future will be completed by the main ingestion task.
    backend.getIngestionService().startConsumption(config, subPartition);
  }

  private synchronized void unsubscribeSubPartition(int subPartition) {
    if (!subPartitionFutures.containsKey(subPartition)) {
      logger.info("Partition " + subPartition + " of " + this + " has been already unsubscribed, "
          + "ignoring the duplicate unsubscribe request");
      return;
    }
    backend.getIngestionService().stopConsumptionAndWait(config, subPartition, 1, 30);
    CompletableFuture future = subPartitionFutures.remove(subPartition);
    if (future != null) {
      future.cancel(true);
    }
    tryStopHeartbeat();
  }

  synchronized void completeSubPartition(int subPartition) {
    subPartitionFutures.computeIfAbsent(subPartition, k -> new CompletableFuture()).complete(null);
    tryStopHeartbeat();
  }

  synchronized void completeSubPartitionExceptionally(int subPartition, Exception e) {
    subPartitionFutures.computeIfAbsent(subPartition, k -> new CompletableFuture()).completeExceptionally(e);
    tryStopHeartbeat();
  }
}
