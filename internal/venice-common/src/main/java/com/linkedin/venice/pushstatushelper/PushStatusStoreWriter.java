package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * PushStatusStoreWriter is a helper class for Da Vinci to write PushStatus and heartbeat message into PushStatus store
 * real-time topic.
 * Heartbeat update is a normal Venice write.
 * PushStatus update is via a map-merge of Write-Compute.
 */
public class PushStatusStoreWriter implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusStoreWriter.class);
  private final String instanceName;
  private final PushStatusStoreVeniceWriterCache veniceWriterCache;
  private final int valueSchemaId;
  private final int derivedSchemaId;
  private final Schema updateSchema;
  private final Function<String, String> clusterResolver;
  private final ThreadPoolExecutor clusterExecutor;
  private final Set<CompletableFuture<PubSubProduceResult>> pendingClusterOperations = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean closed = new AtomicBoolean();

  private static final PubSubProducerCallback PUSH_STATUS_UPDATE_LOGGER_CALLBACK = new PubSubProducerCallback() {
    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      if (exception != null) {
        LOGGER.error("Failed to update push status. Error: ", exception);
      } else {
        LOGGER.info(
            "Updated push status into topic: {} at position: {}",
            Utils.getReplicaId(produceResult.getTopic(), produceResult.getPartition()),
            produceResult.getPubSubPosition());
      }
    }
  };

  public PushStatusStoreWriter(
      VeniceWriterFactory writerFactory,
      String instanceName,
      SchemaEntry valueSchemaEntry,
      DerivedSchemaEntry updateSchemaEntry) {
    this(writerFactory, instanceName, valueSchemaEntry, updateSchemaEntry, storeName -> null);
  }

  public PushStatusStoreWriter(
      VeniceWriterFactory writerFactory,
      String instanceName,
      SchemaEntry valueSchemaEntry,
      DerivedSchemaEntry updateSchemaEntry,
      Function<String, String> clusterResolver) {
    this(
        new PushStatusStoreVeniceWriterCache(
            writerFactory,
            valueSchemaEntry.getSchema(),
            updateSchemaEntry.getSchema()),
        instanceName,
        updateSchemaEntry.getValueSchemaID(),
        updateSchemaEntry.getId(),
        updateSchemaEntry.getSchema(),
        clusterResolver);
  }

  PushStatusStoreWriter(
      PushStatusStoreVeniceWriterCache veniceWriterCache,
      String instanceName,
      int valueSchemaId,
      int derivedSchemaId,
      Schema updateSchema) {
    this(veniceWriterCache, instanceName, valueSchemaId, derivedSchemaId, updateSchema, storeName -> null);
  }

  PushStatusStoreWriter(
      PushStatusStoreVeniceWriterCache veniceWriterCache,
      String instanceName,
      int valueSchemaId,
      int derivedSchemaId,
      Schema updateSchema,
      Function<String, String> clusterResolver) {
    this(veniceWriterCache, instanceName, valueSchemaId, derivedSchemaId, updateSchema, clusterResolver, 1024);
  }

  PushStatusStoreWriter(
      PushStatusStoreVeniceWriterCache veniceWriterCache,
      String instanceName,
      int valueSchemaId,
      int derivedSchemaId,
      Schema updateSchema,
      Function<String, String> clusterResolver,
      int clusterQueueCapacity) {
    this.veniceWriterCache = veniceWriterCache;
    this.instanceName = instanceName;
    this.valueSchemaId = valueSchemaId;
    this.derivedSchemaId = derivedSchemaId;
    this.updateSchema = updateSchema;
    this.clusterResolver = clusterResolver;
    // A single lazy worker orders cluster updates and tombstones across stores without blocking legacy reporters.
    this.clusterExecutor = new ThreadPoolExecutor(
        1,
        1,
        0,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(clusterQueueCapacity),
        new DaemonThreadFactory("push-status-cluster-writer")) {
      @Override
      protected void terminated() {
        // Creation can ignore interruption. Close only after it exits so no writer is cached after cache cleanup.
        veniceWriterCache.close();
      }
    };
  }

  private void update(String storeName, PushStatusKey key, GenericRecord value, PubSubProducerCallback callback) {
    if (closed.get()) {
      throw new IllegalStateException("Push status writer is closed");
    }
    veniceWriterCache.prepareVeniceWriter(storeName).update(key, value, valueSchemaId, derivedSchemaId, callback);
    try {
      String clusterName = clusterResolver.apply(storeName);
      if (clusterName != null) {
        submitCluster(() -> {
          VeniceWriter writer = veniceWriterCache.prepareClusterVeniceWriter(clusterName);
          if (closed.get()) {
            throw new RejectedExecutionException("Push status writer is closed");
          }
          writer.update(
              PushStatusStoreUtils.getClusterPushStatusKey(storeName, key),
              value,
              valueSchemaId,
              derivedSchemaId,
              PUSH_STATUS_UPDATE_LOGGER_CALLBACK);
          return CompletableFuture.completedFuture(null);
        }).whenComplete((result, failure) -> {
          if (failure != null) {
            LOGGER.error("Dropped cluster push status update for store: {} and key: {}", storeName, key, failure);
          }
        });
      }
    } catch (Exception e) {
      LOGGER.error("Failed to update cluster push status for store: {} and key: {}", storeName, key, e);
    }
  }

  private CompletableFuture<PubSubProduceResult> deleteCluster(String clusterName, PushStatusKey namespacedKey) {
    return submitCluster(() -> {
      VeniceWriter writer = veniceWriterCache.prepareClusterVeniceWriter(clusterName);
      if (closed.get()) {
        throw new RejectedExecutionException("Push status writer is closed");
      }
      return writer.delete(namespacedKey, PUSH_STATUS_UPDATE_LOGGER_CALLBACK);
    });
  }

  /**
   * Cluster reporting is best effort: saturation and shutdown fail submissions instead of delaying legacy reports.
   * Deletion callers retain the failure and the actual producer acknowledgement through the returned future.
   */
  private CompletableFuture<PubSubProduceResult> submitCluster(
      Supplier<CompletableFuture<PubSubProduceResult>> operation) {
    CompletableFuture<PubSubProduceResult> result = new CompletableFuture<>();
    pendingClusterOperations.add(result);
    result.whenComplete((ignored, failure) -> pendingClusterOperations.remove(result));
    try {
      if (closed.get()) {
        throw new RejectedExecutionException("Push status writer is closed");
      }
      clusterExecutor.execute(() -> {
        try {
          if (closed.get()) {
            throw new RejectedExecutionException("Push status writer is closed");
          }
          operation.get().whenComplete((produced, failure) -> {
            if (failure == null) {
              result.complete(produced);
            } else {
              result.completeExceptionally(failure);
            }
          });
        } catch (Exception e) {
          result.completeExceptionally(e);
        }
      });
    } catch (RejectedExecutionException e) {
      LOGGER.error("Rejected cluster push status operation: worker queue is full or writer is closed", e);
      result.completeExceptionally(e);
    }
    return result;
  }

  private CompletableFuture<PubSubProduceResult> delete(String storeName, PushStatusKey key) {
    return delete(clusterResolver.apply(storeName), storeName, key, false);
  }

  private CompletableFuture<PubSubProduceResult> delete(
      String clusterName,
      String storeName,
      PushStatusKey key,
      boolean isMigrationCleanup) {
    if (closed.get()) {
      throw new IllegalStateException("Push status store writer is closed");
    }
    CompletableFuture<PubSubProduceResult> legacyDelete = isMigrationCleanup
        ? CompletableFuture.completedFuture(null)
        : veniceWriterCache.prepareVeniceWriter(storeName).delete(key, null);
    if (clusterName != null) {
      CompletableFuture<PubSubProduceResult> clusterDelete =
          deleteCluster(clusterName, PushStatusStoreUtils.getClusterPushStatusKey(storeName, key));
      return legacyDelete.thenCombine(clusterDelete, (legacyResult, clusterResult) -> legacyResult);
    }
    return legacyDelete;
  }

  public void writeHeartbeat(String storeName) {
    writeHeartbeat(storeName, System.currentTimeMillis());
  }

  /**
   * This function will write `-1` to indicate the node is bootstrapping and Controller
   * should ignore all the reports from this instance.
   * @param storeName
   */
  public void writeHeartbeatForBootstrappingInstance(String storeName) {
    writeHeartbeat(storeName, -1);
  }

  public void writeHeartbeat(String storeName, long heartbeat) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setNewFieldValue("reportTimestamp", heartbeat);
    LOGGER.info(
        "Sending heartbeat: {} with instance name: {} for push status store of: {}",
        heartbeat,
        instanceName,
        storeName);
    update(storeName, pushStatusKey, updateBuilder.build(), null);
  }

  public void writePushStatus(
      String storeName,
      int version,
      int partitionId,
      ExecutionStatus status,
      Optional<String> incrementalPushVersion) {
    writePushStatus(storeName, version, partitionId, status, incrementalPushVersion, Optional.empty());
  }

  public void writePushStatus(
      String storeName,
      int version,
      int partitionId,
      ExecutionStatus status,
      Optional<String> incrementalPushVersion,
      Optional<String> incrementalPushPrefix) {
    PushStatusKey pushStatusKey =
        PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion, incrementalPushPrefix);
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setEntriesToAddToMapField("instances", Collections.singletonMap(instanceName, status.getValue()));
    LOGGER.info(
        "Updating pushStatus of {} to {}. Store: {}, version: {}, partition: {}",
        instanceName,
        status,
        storeName,
        version,
        partitionId);
    update(storeName, pushStatusKey, updateBuilder.build(), null);

    // If this is a server side SOIP status update then add this incremental
    // push to the ongoing incremental pushes in push status store.
    if (status == ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED && incrementalPushVersion.isPresent()
        && incrementalPushPrefix.isPresent()) {
      addToSupposedlyOngoingIncrementalPushVersions(storeName, version, incrementalPushVersion.get(), status);
    }
  }

  /**
   * This only works for "batch push" status update.
   * Write one single push status for all partitions on this node, which assumes that all partitions are on the same
   * state. The key only contains version number.
   */
  public void writeVersionLevelPushStatus(
      String storeName,
      int version,
      ExecutionStatus status,
      Set<Integer> partitionIds,
      Optional<String> incrementalPushVersion) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, incrementalPushVersion);
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setEntriesToAddToMapField("instances", Collections.singletonMap(instanceName, status.getValue()));
    LOGGER.info(
        "Updating pushStatus of {} to {}. Store: {}, version: {}, for all partition on this node: {}",
        instanceName,
        status,
        storeName,
        version,
        partitionIds);
    update(storeName, pushStatusKey, updateBuilder.build(), PUSH_STATUS_UPDATE_LOGGER_CALLBACK);
  }

  // For storing ongoing incremental push versions, we are (re)using 'instances' field of the PushStatusValue record.
  public void addToSupposedlyOngoingIncrementalPushVersions(
      String storeName,
      int storeVersion,
      String incrementalPushVersion,
      ExecutionStatus status) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder
        .setEntriesToAddToMapField("instances", Collections.singletonMap(incrementalPushVersion, status.getValue()));
    LOGGER.info(
        "Adding incremental push version: {} to ongoingIncrementalPushes of store: {} from instance: {}",
        incrementalPushVersion,
        storeName,
        instanceName);
    update(storeName, pushStatusKey, updateBuilder.build(), null);
  }

  public void removeFromSupposedlyOngoingIncrementalPushVersions(
      String storeName,
      int storeVersion,
      String incrementalPushVersion) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setKeysToRemoveFromMapField("instances", Collections.singletonList(incrementalPushVersion));
    LOGGER.info(
        "Removing incremental push version: {} from ongoingIncrementalPushes of store: {} from instance: {}",
        incrementalPushVersion,
        storeName,
        instanceName);
    update(storeName, pushStatusKey, updateBuilder.build(), null);
  }

  public void deletePushStatus(
      String storeName,
      int version,
      Optional<String> incrementalPushVersion,
      int partitionCount) {
    deletePushStatus(clusterResolver.apply(storeName), storeName, version, incrementalPushVersion, partitionCount)
        .whenComplete((ignored, failure) -> {
          if (failure != null) {
            LOGGER.error("Failed to delete push status for store: {} and version: {}", storeName, version, failure);
          }
        });
  }

  /**
   * Deletes records in the owning cluster, independently of current store discovery.
   * A null cluster name limits deletion to the legacy store.
   */
  public CompletableFuture<Void> deletePushStatus(
      String clusterName,
      String storeName,
      int version,
      Optional<String> incrementalPushVersion,
      int partitionCount) {
    return deletePushStatus(clusterName, storeName, version, incrementalPushVersion, partitionCount, false);
  }

  /**
   * Migration cleanup preserves legacy records shared with the surviving cluster.
   * With no cluster target, migration cleanup does not delete any records.
   */
  public CompletableFuture<Void> deletePushStatus(
      String clusterName,
      String storeName,
      int version,
      Optional<String> incrementalPushVersion,
      int partitionCount,
      boolean isMigrationCleanup) {
    LOGGER.info("Deleting pushStatus of storeName: {}, version: {}", storeName, version);
    CompletableFuture<?>[] deletions = new CompletableFuture<?>[partitionCount + 1];
    deletions[0] = delete(
        clusterName,
        storeName,
        PushStatusStoreUtils.getPushKey(version, incrementalPushVersion),
        isMigrationCleanup);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion);
      deletions[partitionId + 1] = delete(clusterName, storeName, pushStatusKey, isMigrationCleanup);
    }
    return CompletableFuture.allOf(deletions);
  }

  /**
   * N.B.: Currently used by tests only.
   * @return
   */
  public Future<PubSubProduceResult> deletePartitionIncrementalPushStatus(
      String storeName,
      int version,
      String incrementalPushVersion,
      int partitionId) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(
        version,
        partitionId,
        Optional.ofNullable(incrementalPushVersion),
        Optional.of(PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX));
    LOGGER.info(
        "Deleting incremental push status belonging to a partition:{}. pushStatusKey:{}",
        partitionId,
        pushStatusKey);
    return delete(storeName, pushStatusKey);
  }

  public void removePushStatusStoreVeniceWriter(String storeName) {
    LOGGER.info("Removing push status store writer for store {}", storeName);
    long veniceWriterRemovingStartTimeInNs = System.nanoTime();
    veniceWriterCache.removeVeniceWriter(storeName);
    LOGGER.info(
        "Removed push status store writer for store {} in {}ms.",
        storeName,
        LatencyUtils.getElapsedTimeFromNSToMS(veniceWriterRemovingStartTimeInNs));
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      RejectedExecutionException failure = new RejectedExecutionException("Push status writer is closed");
      pendingClusterOperations.forEach(operation -> operation.completeExceptionally(failure));
      clusterExecutor.shutdownNow();
      if (!clusterExecutor.isTerminated()) {
        LOGGER.warn("Cluster push status work is still stopping; writer cache cleanup is deferred until it exits");
      }
    }
  }
}
