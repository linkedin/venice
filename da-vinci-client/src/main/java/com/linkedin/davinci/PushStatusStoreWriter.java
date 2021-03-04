package com.linkedin.davinci;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusStoreVeniceWriterCache;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Collections;
import java.util.Optional;
import org.apache.avro.generic.GenericData;
import org.apache.log4j.Logger;

import static com.linkedin.venice.pushstatus.PushStatusStoreVeniceWriterCache.*;


/**
 * PushStatusStoreWriter is a helper class for Da Vinci to write PushStatus and heartbeat message into PushStatus store
 * real-time topic.
 * Heartbeat update is a normal Venice write.
 * PushStatus update is via a map-merge of Write-Compute.
 */
public class PushStatusStoreWriter implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(PushStatusStoreWriter.class);
  private final String instanceName;
  private final PushStatusStoreVeniceWriterCache veniceWriterCache;
  private final ReadOnlySchemaRepository schemaRepository;

  /**
   * @param writerFactory Used for instantiate veniceWriterCache
   * @param schemaRepository Used for retrieving schemaId
   * @param instanceName format = hostAddress,appName
   */
  public PushStatusStoreWriter(VeniceWriterFactory writerFactory, ReadOnlySchemaRepository schemaRepository, String instanceName) {
    this.veniceWriterCache = new PushStatusStoreVeniceWriterCache(writerFactory);
    this.schemaRepository = schemaRepository;
    this.instanceName = instanceName;
  }

  public void writeHeartbeat(String storeName) {
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    PushStatusValue pushStatusValue = new PushStatusValue();
    pushStatusValue.reportTimestamp = System.currentTimeMillis();
    pushStatusValue.instances = Collections.emptyMap();
    int valueSchemaId = schemaRepository.getValueSchemaId(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName), PushStatusValue.SCHEMA$.toString());
    logger.info("Sending heartbeat of " + instanceName);
    writer.put(pushStatusKey, pushStatusValue, valueSchemaId);
  }

  public void writePushStatus(String storeName, int version, int partitionId, ExecutionStatus status) {
    writePushStatus(storeName, version, partitionId, status, Optional.empty());
  }

  public void writePushStatus(String storeName, int version, int partitionId, ExecutionStatus status, Optional<String> incrementalPushVersion) {
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion);
    GenericData.Record writeComputeRecord = new GenericData.Record(WRITE_COMPUTE_SCHEMA);
    GenericData.Record instancesRecord = new GenericData.Record(WRITE_COMPUTE_SCHEMA.getField("instances").schema().getTypes().get(1));
    instancesRecord.put(WriteComputeSchemaAdapter.MAP_UNION, Collections.singletonMap(instanceName, status.ordinal()));
    instancesRecord.put(WriteComputeSchemaAdapter.MAP_DIFF, Collections.emptyList());
    writeComputeRecord.put("instances", instancesRecord);
    writeComputeRecord.put("reportTimestamp", new GenericData.Record(WRITE_COMPUTE_SCHEMA.getField("reportTimestamp").schema().getTypes().get(0)));
    Pair<Integer, Integer> schemaIds = schemaRepository.getDerivedSchemaId(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName), WRITE_COMPUTE_SCHEMA.toString());
    int valueSchemaId = schemaIds.getFirst();
    int derivedSchemaId = schemaIds.getSecond();
    logger.info("Updating pushStatus of " + instanceName + " to " + status.toString() +
        ". storeName: " + storeName + " , version: " + version + " , partition: " + partitionId);
    writer.update(pushStatusKey, writeComputeRecord, valueSchemaId, derivedSchemaId, null);
  }

  @Override
  public void close() {
    veniceWriterCache.close();
  }
}
