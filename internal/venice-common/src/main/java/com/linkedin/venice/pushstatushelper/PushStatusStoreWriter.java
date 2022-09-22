package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatus.NoOp;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.pushstatus.PushStatusValueWriteOpRecord;
import com.linkedin.venice.pushstatus.instancesMapOps;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Collections;
import java.util.Optional;
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

  /**
   * @param writerFactory Used for instantiate veniceWriterCache
   * @param instanceName format = hostAddress,appName
   * @param derivedSchemaId writeCompute schema for updating push status
   */
  public PushStatusStoreWriter(VeniceWriterFactory writerFactory, String instanceName, int derivedSchemaId) {
    this(new PushStatusStoreVeniceWriterCache(writerFactory), instanceName, derivedSchemaId);
  }

  PushStatusStoreWriter(PushStatusStoreVeniceWriterCache veniceWriterCache, String instanceName, int derivedSchemaId) {
    this.veniceWriterCache = veniceWriterCache;
    this.instanceName = instanceName;
    this.derivedSchemaId = derivedSchemaId;
  }

  public void writeHeartbeat(String storeName) {
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    PushStatusValue pushStatusValue = new PushStatusValue();
    pushStatusValue.reportTimestamp = System.currentTimeMillis();
    pushStatusValue.instances = Collections.emptyMap();
    LOGGER.info("Sending heartbeat of {}", instanceName);
    writer.put(
        pushStatusKey,
        pushStatusValue,
        AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion());
  }

  public void writePushStatus(String storeName, int version, int partitionId, ExecutionStatus status) {
    writePushStatus(storeName, version, partitionId, status, Optional.empty());
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
    PushStatusValueWriteOpRecord writeComputeRecord = new PushStatusValueWriteOpRecord();
    instancesMapOps instances = new instancesMapOps();
    instances.mapUnion = Collections.singletonMap(instanceName, status.getValue());
    instances.mapDiff = Collections.emptyList();
    writeComputeRecord.instances = instances;
    writeComputeRecord.reportTimestamp = new NoOp();
    LOGGER.info(
        "Updating pushStatus of {} to {}. Store: {}, version: {}, partition: {}",
        instanceName,
        status,
        storeName,
        version,
        partitionId);
    writer.update(
        pushStatusKey,
        writeComputeRecord,
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
    PushStatusValueWriteOpRecord writeComputeRecord = new PushStatusValueWriteOpRecord();
    instancesMapOps incrementalPushes = new instancesMapOps();
    incrementalPushes.mapUnion = Collections.singletonMap(incrementalPushVersion, status.getValue());
    incrementalPushes.mapDiff = Collections.emptyList();
    writeComputeRecord.instances = incrementalPushes;
    writeComputeRecord.reportTimestamp = new NoOp();
    LOGGER.info(
        "Adding incremental push version: {} to ongoingIncrementalPushes of store: {} from instance: {}",
        incrementalPushVersion,
        storeName,
        instanceName);
    veniceWriterCache.prepareVeniceWriter(storeName)
        .update(
            pushStatusKey,
            writeComputeRecord,
            AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion(),
            derivedSchemaId,
            null);
  }

  public void removeFromSupposedlyOngoingIncrementalPushVersions(
      String storeName,
      int storeVersion,
      String incrementalPushVersion) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusValueWriteOpRecord writeComputeRecord = new PushStatusValueWriteOpRecord();
    instancesMapOps incrementalPushes = new instancesMapOps();
    incrementalPushes.mapUnion = Collections.emptyMap();
    incrementalPushes.mapDiff = Collections.singletonList(incrementalPushVersion);
    writeComputeRecord.instances = incrementalPushes;
    writeComputeRecord.reportTimestamp = new NoOp();
    LOGGER.info(
        "Removing incremental push version: {} from ongoingIncrementalPushes of store: {} from instance: {}",
        incrementalPushVersion,
        storeName,
        instanceName);
    veniceWriterCache.prepareVeniceWriter(storeName)
        .update(
            pushStatusKey,
            writeComputeRecord,
            AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion(),
            derivedSchemaId,
            null);
  }

  @Override
  public void close() {
    veniceWriterCache.close();
  }
}
