package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Collections;
import java.util.Optional;
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
  private final int derivedSchemaId;
  private final Schema updateSchema;

  /**
   * @param writerFactory Used for instantiate veniceWriterCache
   * @param instanceName format = hostAddress,appName
   * @param derivedSchemaId update schema protocol id for updating push status
   * @param updateSchema update schema for updating push status
   */
  public PushStatusStoreWriter(
      VeniceWriterFactory writerFactory,
      String instanceName,
      int derivedSchemaId,
      Schema updateSchema) {
    this(
        new PushStatusStoreVeniceWriterCache(writerFactory, updateSchema),
        instanceName,
        derivedSchemaId,
        updateSchema);
  }

  PushStatusStoreWriter(
      PushStatusStoreVeniceWriterCache veniceWriterCache,
      String instanceName,
      int derivedSchemaId,
      Schema updateSchema) {
    this.veniceWriterCache = veniceWriterCache;
    this.instanceName = instanceName;
    this.derivedSchemaId = derivedSchemaId;
    this.updateSchema = updateSchema;
  }

  public void writeHeartbeat(String storeName, long heartbeat) {
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    PushStatusValue pushStatusValue = new PushStatusValue();
    pushStatusValue.reportTimestamp = heartbeat;
    pushStatusValue.instances = Collections.emptyMap();
    LOGGER.info("Sending heartbeat of {}", instanceName);
    writer.put(
        pushStatusKey,
        pushStatusValue,
        AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion());

  }

  public void writeHeartbeat(String storeName) {
    writeHeartbeat(storeName, System.currentTimeMillis());
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
    writer.update(
        pushStatusKey,
        updateBuilder.build(),
        AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion(),
        derivedSchemaId,
        null);

    // If this is a server side SOIP status update then add this incremental
    // push to the ongoing incremental pushes in push status store.
    if (status == ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED && incrementalPushVersion.isPresent()
        && incrementalPushPrefix.isPresent()) {
      addToSupposedlyOngoingIncrementalPushVersions(storeName, version, incrementalPushVersion.get(), status);
    }
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
        .update(
            pushStatusKey,
            updateBuilder.build(),
            AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion(),
            derivedSchemaId,
            null);
  }

  public void removeFromSupposedlyOngoingIncrementalPushVersions(
      String storeName,
      int storeVersion,
      String incrementalPushVersion) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setElementsToRemoveFromListField("instances", Collections.singletonList(incrementalPushVersion));
    LOGGER.info(
        "Removing incremental push version: {} from ongoingIncrementalPushes of store: {} from instance: {}",
        incrementalPushVersion,
        storeName,
        instanceName);
    veniceWriterCache.prepareVeniceWriter(storeName)
        .update(
            pushStatusKey,
            updateBuilder.build(),
            AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion(),
            derivedSchemaId,
            null);
  }

  @Override
  public void close() {
    veniceWriterCache.close();
  }
}
