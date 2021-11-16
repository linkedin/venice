package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Lazy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.VeniceConstants.*;


/**
 * TODO schema validation of old and new schema for WC enabled stores.
 * The workflow is
 * Query old replication metadata. If it's null (and running in first batch push merge policy), then write the new value directly.
 * If the old replication metadata exists, then deserialize it and run Merge<BB>.
 * If the incoming TS is higher than the entirety of the old replication metadata, then write the new value directly.
 * If the incoming TS is lower than the entirety of the old replication metadata, then drop the new value.
 * If the incoming TS is partially higher, partially lower, than the old replication metadata, then query the old value, deserialize it, and pass it to Merge<GR>, Merge<Map> or Merge<List> .
 */
public class MergeConflictResolver {
  private final String storeName;
  private final ReadOnlySchemaRepository schemaRepository;
  private final int replicationMetadataVersionId;

  public MergeConflictResolver(ReadOnlySchemaRepository schemaRepository, String storeName, int replicationMetadataVersionId) {
    this.schemaRepository = schemaRepository;
    this.storeName = storeName;
    this.replicationMetadataVersionId = replicationMetadataVersionId;
  }

  /**
   * Perform conflict resolution when the incoming operation is a PUT operation.
   * @param oldValue A Lazy supplier of currently persisted value.
   * @param oldReplicationMetadata The replication metadata of the currently persisted value.
   * @param newValue The value in the incoming record.
   * @param writeOperationTimestamp The logical timestamp of the incoming record.
   * @param oldValueSchemaId The schema id of the currently persisted value.
   * @param newValueSchemaId The schema id of the value in the incoming record.
   * @param newValueSourceOffset The offset from which the new value originates in the realtime stream.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param newValueSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                               to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @return A MergeConflictResult which denotes what update should be applied or if the operation should be ignored.
   */
  public MergeConflictResult put(Lazy<ByteBuffer> oldValue, ByteBuffer oldReplicationMetadata, ByteBuffer newValue,
      long writeOperationTimestamp, int oldValueSchemaId, int newValueSchemaId, long newValueSourceOffset, int newValueSourceBrokerID) {
    /**
     * oldReplicationMetadata can be null in two cases:
     * 1. There is no value corresponding to the key
     * 2. There is a value corresponding to the key but it came from the batch push and the BatchConflictResolutionPolicy
     * specifies that no per-record replication metadata should be persisted for batch push data.
     *
     * In such cases, the incoming PUT operation will be applied directly and we should store the updated RMD for it.
     */
    // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
    if (oldReplicationMetadata == null) {
      GenericRecord newReplicationMetadata = new GenericData.Record(getReplicationMetadataSchema(newValueSchemaId));
      newReplicationMetadata.put(TIMESTAMP_FIELD, writeOperationTimestamp);
      // A record which didn't come from an RT topic or has null metadata should have no offset vector.
      newReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
          Merge.mergeOffsetVectors(new ArrayList<>(), newValueSourceOffset, newValueSourceBrokerID));
      byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(newValueSchemaId).serialize(newReplicationMetadata);

      return new MergeConflictResult(newValue, newValueSchemaId, ByteBuffer.wrap(newReplicationMetadataBytes), true);
    }

    // If oldReplicationMetadata exists, then oldValueSchemaId should never be negative.
    if (oldValueSchemaId <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = " + storeName + "; schema ID = " + oldValueSchemaId);
    }

    GenericRecord replicationMetadataRecord = getReplicationMetadataDeserializer(oldValueSchemaId).deserialize(oldReplicationMetadata);
    Object existingTimestampObject = replicationMetadataRecord.get(TIMESTAMP_FIELD);
    Merge.ReplicationMetadataType replicationMetadataType = Merge.getReplicationMetadataType(existingTimestampObject);

    if (replicationMetadataType == Merge.ReplicationMetadataType.ROOT_LEVEL_TIMESTAMP) {
      final long existingTimestamp = (long) existingTimestampObject;
      // if new write timestamp is larger, new value wins.
      if (existingTimestamp < writeOperationTimestamp) {
        replicationMetadataRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
            Merge.mergeOffsetVectors((List<Long>)replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD), newValueSourceOffset, newValueSourceBrokerID));
        replicationMetadataRecord.put(TIMESTAMP_FIELD, writeOperationTimestamp);
        byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(newValueSchemaId).serialize(replicationMetadataRecord);
        return new MergeConflictResult(newValue, newValueSchemaId, ByteBuffer.wrap(newReplicationMetadataBytes), true);
      } else if (existingTimestamp > writeOperationTimestamp) { // no-op as new ts is stale, ignore update
        return MergeConflictResult.getIgnoredResult();
      } else { // tie in old and new ts, decide persistence based on object comparison
        ByteBuffer compareResult = (ByteBuffer) Merge.compareAndReturn(oldValue.get(), newValue);
        if (compareResult != newValue) {
          return MergeConflictResult.getIgnoredResult();
        }
        // no need to update RMD ts, but do need to update the offset vector, so return a new set of metadata
        replicationMetadataRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
            Merge.mergeOffsetVectors((List<Long>)replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD), newValueSourceOffset, newValueSourceBrokerID));
        byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(newValueSchemaId).serialize(replicationMetadataRecord);
        return new MergeConflictResult(compareResult, newValueSchemaId, ByteBuffer.wrap(newReplicationMetadataBytes), true);
      }
    } else if (replicationMetadataType == Merge.ReplicationMetadataType.PER_FIELD_TIMESTAMP) {
      throw new VeniceUnsupportedOperationException("Field level replication metadata not supported");
    } else {
      throw new VeniceUnsupportedOperationException("Not supported replication metadata type: " + replicationMetadataType);
    }

    // TODO: Handle conflict resolution for write-compute
  }

  public String printReplicationMetadata(ByteBuffer replicationMetadata, int schemaIdOfValue) {
    if (replicationMetadata == null) {
      return "";
    }
    GenericRecord replicationMetadataRecord = getReplicationMetadataDeserializer(schemaIdOfValue).deserialize(replicationMetadata);
    return replicationMetadataRecord.toString();
  }

  public long extractOffsetVectorSumFromReplicationMetadata(ByteBuffer replicationMetadata, int schemaIdOfValue) {
    if (replicationMetadata == null) {
      return 0;
    }
    GenericRecord replicationMetadataRecord = getReplicationMetadataDeserializer(schemaIdOfValue).deserialize(replicationMetadata);
    Object offsetVectorObject = replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD);
    return Merge.sumOffsetVector(offsetVectorObject);
  }

  public List<Long> extractTimestampFromReplicationMetadata(ByteBuffer replicationMetadata, int schemaIdOfValue) {
    // TODO: This function needs a heuristic to work on field level timestamps.  At time of writing, this function
    // is only for recording the previous value of a record's timestamp, so we could consider specifying the incoming
    // operation to identify if we care about the record level timestamp, or, certain fields and then returning an ordered
    // list of those timestamps to compare post resolution.  I hesitate to commit to an implementation here prior to putting
    // the full write compute resolution into uncommented fleshed out glory.  So we'll effectively ignore operations
    // that aren't root level until then.
    if (replicationMetadata == null) {
      return Arrays.asList(0L);
    }
    GenericRecord replicationMetadataRecord = getReplicationMetadataDeserializer(schemaIdOfValue).deserialize(replicationMetadata);
    Object timestampObject = replicationMetadataRecord.get(TIMESTAMP_FIELD);
    Merge.ReplicationMetadataType replicationMetadataType = Merge.getReplicationMetadataType(timestampObject);

    if (replicationMetadataType == Merge.ReplicationMetadataType.ROOT_LEVEL_TIMESTAMP) {
      return Arrays.asList((long) timestampObject);
    } else {
      // not supported yet so ignore it
      return Arrays.asList(0L);
    }
  }

  /**
   * Perform conflict resolution when the incoming operation is a PUT operation.
   * @param oldReplicationMetadata The replication metadata of the currently persisted value.
   * @param schemaIdOfOldValue The schema id of the currently persisted value.
   * @param writeOperationTimestamp The logical timestamp of the incoming record.
   * @param newValueSourceOffset The offset from which the new value originates in the realtime stream.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param newValueSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                                 to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                                 the ReplicationMetadata for the newly inserted record.
   * @return A MergeConflictResult which denotes what update should be applied or if the operation should be ignored.
   */
  public MergeConflictResult delete(ByteBuffer oldReplicationMetadata, int schemaIdOfOldValue, long writeOperationTimestamp, long newValueSourceOffset, int newValueSourceBrokerID) {
    /**
     * oldReplicationMetadata can be null in two cases:
     * 1. There is no value corresponding to the key
     * 2. There is a value corresponding to the key but it came from the batch push and the BatchConflictResolutionPolicy
     * specifies that no per-record replication metadata should be persisted for batch push data.
     *
     * In such cases, the incoming Delete operation will be applied directly and we should store a tombstone for it.
     */
    // TODO: Honor BatchConflictResolutionPolicy when replication metadata is null
    if (oldReplicationMetadata == null) {
      // If there is no existing record for the key or if the previous operation was a PUT from a batch push, it is
      // possible that schema Id of Old value will not be available. In that case, use largest value schema id to
      // serialize RMD.
      if (schemaIdOfOldValue <= 0) {
        schemaIdOfOldValue = schemaRepository.getLatestValueSchema(storeName).getId();
      }

      GenericRecord newReplicationMetadata = new GenericData.Record(getReplicationMetadataSchema(schemaIdOfOldValue));
      newReplicationMetadata.put(TIMESTAMP_FIELD, writeOperationTimestamp);
      newReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
          Merge.mergeOffsetVectors(null, newValueSourceOffset, newValueSourceBrokerID));
      byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(schemaIdOfOldValue).serialize(newReplicationMetadata);

      return new MergeConflictResult(null, schemaIdOfOldValue, ByteBuffer.wrap(newReplicationMetadataBytes), false);
    }

    if (schemaIdOfOldValue <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = " + storeName + "; schema ID = " + schemaIdOfOldValue);
    }

    GenericRecord replicationMetadataRecord = getReplicationMetadataDeserializer(schemaIdOfOldValue).deserialize(oldReplicationMetadata);
    Object tsObject = replicationMetadataRecord.get(TIMESTAMP_FIELD);
    Merge.ReplicationMetadataType replicationMetadataType = Merge.getReplicationMetadataType(tsObject);

    if (replicationMetadataType == Merge.ReplicationMetadataType.ROOT_LEVEL_TIMESTAMP) {
      long oldTimestamp = (long) tsObject;
      // delete wins on tie
      if (oldTimestamp <= writeOperationTimestamp) {
        // update RMD ts
        replicationMetadataRecord.put(TIMESTAMP_FIELD, writeOperationTimestamp);
        replicationMetadataRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
            Merge.mergeOffsetVectors((List<Long>)replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD), newValueSourceOffset, newValueSourceBrokerID));
        byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(schemaIdOfOldValue).serialize(replicationMetadataRecord);

        return new MergeConflictResult(null, schemaIdOfOldValue, ByteBuffer.wrap(newReplicationMetadataBytes), false);
      } else { // keep the old value
        return MergeConflictResult.getIgnoredResult();
      }
    } else {
      throw new VeniceUnsupportedOperationException("Field level MD not supported");
    }

    // TODO: Handle conflict resolution for write-compute
  }

  public long getWriteTimestampFromKME(KafkaMessageEnvelope kme) {
    if (kme.producerMetadata.logicalTimestamp >= 0) {
      return kme.producerMetadata.logicalTimestamp;
    } else {
      return kme.producerMetadata.messageTimestamp;
    }
  }

  private RecordDeserializer<GenericRecord> getReplicationMetadataDeserializer(int valueSchemaId) {
    Schema replicationMetadataSchema = getReplicationMetadataSchema(valueSchemaId);
    return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(replicationMetadataSchema, replicationMetadataSchema);
  }

  private RecordSerializer<GenericRecord> getReplicationMetadataSerializer(int valueSchemaId) {
    Schema replicationMetadataSchema = getReplicationMetadataSchema(valueSchemaId);
    return FastSerializerDeserializerFactory.getFastAvroGenericSerializer(replicationMetadataSchema);
  }

  private Schema getReplicationMetadataSchema(int valueSchemaId) {
    ReplicationMetadataSchemaEntry
        replicationMetadataSchemaEntry = schemaRepository.getReplicationMetadataSchema(storeName, valueSchemaId,
        replicationMetadataVersionId);
    if (replicationMetadataSchemaEntry == null) {
      throw new VeniceException("Unable to fetch replication metadata schema from schema repository");
    }
    return replicationMetadataSchemaEntry.getSchema();
  }
}