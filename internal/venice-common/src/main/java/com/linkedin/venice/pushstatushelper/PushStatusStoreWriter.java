package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
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

  private static final PubSubProducerCallback PUSH_STATUS_UPDATE_LOGGER_CALLBACK = new PubSubProducerCallback() {
    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      if (exception != null) {
        LOGGER.error("Failed to update push status. Error: ", exception);
      } else {
        LOGGER.info(
            "Updated push status into topic {} at offset {}.",
            produceResult.getTopic(),
            produceResult.getOffset());
      }
    }
  };

  public PushStatusStoreWriter(
      VeniceWriterFactory writerFactory,
      String instanceName,
      SchemaEntry valueSchemaEntry,
      DerivedSchemaEntry updateSchemaEntry) {
    this(
        new PushStatusStoreVeniceWriterCache(
            writerFactory,
            valueSchemaEntry.getSchema(),
            updateSchemaEntry.getSchema()),
        instanceName,
        updateSchemaEntry.getValueSchemaID(),
        updateSchemaEntry.getId(),
        updateSchemaEntry.getSchema());
  }

  PushStatusStoreWriter(
      PushStatusStoreVeniceWriterCache veniceWriterCache,
      String instanceName,
      int valueSchemaId,
      int derivedSchemaId,
      Schema updateSchema) {
    this.veniceWriterCache = veniceWriterCache;
    this.instanceName = instanceName;
    this.valueSchemaId = valueSchemaId;
    this.derivedSchemaId = derivedSchemaId;
    this.updateSchema = updateSchema;
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
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setNewFieldValue("reportTimestamp", heartbeat);
    LOGGER.info("Sending heartbeat of {}", instanceName);
    writer.update(pushStatusKey, updateBuilder.build(), valueSchemaId, derivedSchemaId, null);
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
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
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
    writer.update(pushStatusKey, updateBuilder.build(), valueSchemaId, derivedSchemaId, null);

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
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
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
    writer.update(
        pushStatusKey,
        updateBuilder.build(),
        valueSchemaId,
        derivedSchemaId,
        PUSH_STATUS_UPDATE_LOGGER_CALLBACK);
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
    veniceWriterCache.prepareVeniceWriter(storeName)
        .update(pushStatusKey, updateBuilder.build(), valueSchemaId, derivedSchemaId, null);
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
    veniceWriterCache.prepareVeniceWriter(storeName)
        .update(pushStatusKey, updateBuilder.build(), valueSchemaId, derivedSchemaId, null);
  }

  public void deletePushStatus(
      String storeName,
      int version,
      Optional<String> incrementalPushVersion,
      int partitionCount) {
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
    LOGGER.info("Deleting pushStatus of storeName: {}, version: {}", storeName, version);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion);
      writer.delete(pushStatusKey, null);
    }
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
    return veniceWriterCache.prepareVeniceWriter(storeName).delete(pushStatusKey, null);
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
    veniceWriterCache.close();
  }
}
