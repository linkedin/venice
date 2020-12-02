package com.linkedin.davinci;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.log4j.Logger;


/**
 * PushStatusStoreWriter is a helper class for Da Vinci to write PushStatus and heartbeat message into PushStatus store
 * real-time topic.
 * Heartbeat update is a normal Venice write.
 * PushStatus update is via a map-merge of Write-Compute.
 */
public class PushStatusStoreWriter implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(PushStatusStoreWriter.class);
  private static final Schema WRITE_COMPUTE_SCHEMA = WriteComputeSchemaAdapter.parse(PushStatusValue.SCHEMA$.toString()).getTypes().get(0);

  private final VeniceWriterFactory writerFactory;
  private final String instanceName;
  // Local cache of VeniceWriters.
  private final Map<String, VeniceWriter> veniceWriters = new VeniceConcurrentHashMap<>();
  private final ReadOnlySchemaRepository schemaRepository;

  /**
   * @param writerFactory Used for instantiating VeniceWriter
   * @param schemaRepository Used for retrieving schemaId
   * @param instanceName format = hostAddress,appName
   */
  public PushStatusStoreWriter(VeniceWriterFactory writerFactory, ReadOnlySchemaRepository schemaRepository, String instanceName) {
    this.writerFactory = writerFactory;
    this.schemaRepository = schemaRepository;
    this.instanceName = instanceName;
  }

  public void writeHeartbeat(String storeName) {
    VeniceWriter writer = getVeniceWriter(storeName);
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    PushStatusValue pushStatusValue = new PushStatusValue();
    pushStatusValue.reportTimestamp = System.currentTimeMillis();
    pushStatusValue.instances = Collections.emptyMap();
    int valueSchemaId = schemaRepository.getValueSchemaId(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName), PushStatusValue.SCHEMA$.toString());
    writer.put(pushStatusKey, pushStatusValue, valueSchemaId);
  }

  public void writePushStatus(String storeName, int version, int partitionId, ExecutionStatus status) {
    writePushStatus(storeName, version, partitionId, status, Optional.empty());
  }

  public void writePushStatus(String storeName, int version, int partitionId, ExecutionStatus status, Optional<String> incrementalPushVersion) {
    VeniceWriter writer = getVeniceWriter(storeName);
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
    writer.update(pushStatusKey, writeComputeRecord, valueSchemaId, derivedSchemaId, null);
  }

  private VeniceWriter getVeniceWriter(String storeName) {
    return veniceWriters.computeIfAbsent(storeName, s -> {
      String rtTopic = Version.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName));
      VeniceWriter writer = writerFactory.createVeniceWriter(rtTopic, new VeniceAvroKafkaSerializer(PushStatusKey.SCHEMA$.toString()),
          new VeniceAvroKafkaSerializer(PushStatusValue.SCHEMA$.toString()), new VeniceAvroKafkaSerializer(WRITE_COMPUTE_SCHEMA.toString()), Optional.empty(), SystemTime.INSTANCE);
      return writer;
    });
  }

  @Override
  public void close() {
    veniceWriters.forEach((k, v) -> {
      try {
        v.close();
      } catch (Exception e) {
        logger.error("Can not close VeniceWriter. ", e);
      }
    });
  }
}
