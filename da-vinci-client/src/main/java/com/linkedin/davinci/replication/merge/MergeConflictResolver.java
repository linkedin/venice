package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.TimestampMetadataSchemaEntry;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Lazy;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.VeniceConstants.*;


/**
 * TODO schema validation of old and new schema for WC enabled stores.
 * The workflow is
 * Query old TSMD. If it's null (and running in first batch push merge policy), then write the new value directly.
 * If the old TSMD exists, then deserialize it and run Merge<BB>.
 * If the incoming TS is higher than the entirety of the old TSMD, then write the new value directly.
 * If the incoming TS is lower than the entirety of the old TSMD, then drop the new value.
 * If the incoming TS is partially higher, partially lower, than the old TSMD, then query the old value, deserialize it, and pass it to Merge<GR>, Merge<Map> or Merge<List> .
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
   * @param schemaIdOfOldValue The schema id of the currently persisted value.
   * @param schemaIdOfNewValue The schema id of the value in the incoming record.
   * @return A MergeConflictResult which denotes what update should be applied or if the operation should be ignored.
   */
  public MergeConflictResult put(Lazy<ByteBuffer> oldValue, ByteBuffer oldReplicationMetadata, ByteBuffer newValue,
      long writeOperationTimestamp, int schemaIdOfOldValue, int schemaIdOfNewValue) {
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
      GenericRecord newReplicationMetadata = new GenericData.Record(getReplicationMetadataSchema(schemaIdOfNewValue));
      newReplicationMetadata.put(TIMESTAMP_FIELD, writeOperationTimestamp);
      byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(schemaIdOfNewValue).serialize(newReplicationMetadata);

      return new MergeConflictResult(newValue, schemaIdOfNewValue, ByteBuffer.wrap(newReplicationMetadataBytes), true);
    }

    // If oldReplicationMetadata exists, then schemaIdOfOldValue should never be negative.
    if (schemaIdOfOldValue <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = " + storeName + "; schema ID = " + schemaIdOfOldValue);
    }

    GenericRecord replicationMetadataRecord = getReplicationMetadataDeserializer(schemaIdOfOldValue).deserialize(oldReplicationMetadata);
    Object tsObject = replicationMetadataRecord.get(TIMESTAMP_FIELD);
    Merge.TSMDType tsmdType = Merge.getTSMDType(tsObject);

    if (tsmdType == Merge.TSMDType.ROOT_LEVEL_TS) {
      long oldTimestamp = (long) tsObject;

      // if new write timestamp is larger, new value wins.
      if (oldTimestamp < writeOperationTimestamp) {
        replicationMetadataRecord.put(TIMESTAMP_FIELD, writeOperationTimestamp);
        byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(schemaIdOfNewValue).serialize(replicationMetadataRecord);

        return new MergeConflictResult(newValue, schemaIdOfNewValue, ByteBuffer.wrap(newReplicationMetadataBytes), true);
      } else if (oldTimestamp > writeOperationTimestamp) { // no-op as new ts is stale, ignore update
        return MergeConflictResult.getIgnoredResult();
      } else { // tie in old and new ts, decide persistence based on object comparison
        ByteBuffer compareResult = (ByteBuffer) Merge.compareAndReturn(oldValue.get(), newValue);
        if (compareResult != newValue) {
          return MergeConflictResult.getIgnoredResult();
        }

        // no need to update RMD ts
        return new MergeConflictResult(compareResult, schemaIdOfNewValue, oldReplicationMetadata, true);
      }
    } else {
      throw new VeniceUnsupportedOperationException("Field level replication metadata not supported");
    }

    /*
    // check each field's timestamp
    GenericRecord timestampRecordForOldValue = (GenericRecord) tsObject;
    List<Schema.Field> fields = timestampRecordForOldValue.getSchema().getFields();
    boolean newTsLarger = false, newTsSmaller = false;
    for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
      Schema.Field field = fields.get(i);
      long fieldTimestamp = (long) timestampRecordForOldValue.get(field.pos());
      if (fieldTimestamp > writeOperationTimestamp) {
        newTsLarger = true;
      } else if (fieldTimestamp < writeOperationTimestamp) {
        newTsSmaller = true;
      }
    }
    // we need to deserialize both new and old value
    if (newTsLarger && newTsSmaller) {
      GenericRecord oldRecord = getValueRecordDeserializer(oldSchemaID, newSchemaID).deserialize(oldValueAndTimestampMetadata.getValue());
      GenericRecord newRecord = getValueRecordDeserializer(oldSchemaID, newSchemaID).deserialize(newValue.get());
      ValueAndTimestampMetadata<GenericRecord> valueAndTimestampMetadata = new ValueAndTimestampMetadata<>(oldRecord, oldValueAndTimestampMetadata.getTimestampMetadata());

      valueAndTimestampMetadata = mergeGenericRecord.put(valueAndTimestampMetadata, newRecord, writeOperationTimestamp);

      // serialize back to ByteBuffer to put in storage engine
      oldValueAndTimestampMetadata.setValue(ByteBuffer.wrap(getRecordSerializer(newSchemaID).serialize(valueAndTimestampMetadata.getValue())));
    } else if (newTsLarger) { // newValue wins TODO schema validation
      oldValueAndTimestampMetadata.setValue(newValue.get());
      oldValueAndTimestampMetadata.setUpdateIgnored(false);
    }  else if (newTsSmaller){ // old ts larger keep the old value.
     oldValueAndTimestampMetadata.setUpdateIgnored(true);
    } else {
      oldValueAndTimestampMetadata.setValue((ByteBuffer) Merge.compareAndReturn(oldValueAndTimestampMetadata.getValue(), newValue.get()));
    }

    return oldValueAndTimestampMetadata;
  }
    private RecordDeserializer<GenericRecord> getValueRecordDeserializer(int oldSchemaID, int newSchemaID) {
    int schemaID = oldSchemaID*10000 + newSchemaID;
    return recordDeSerializerMap.computeIfAbsent(schemaID, schemaId -> {
      Schema oldSchema = schemaRepository.getValueSchema(storeName, oldSchemaID).getSchema();
      Schema newSchema  = schemaRepository.getValueSchema(storeName, newSchemaID).getSchema();

      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(newSchema, oldSchema);
    });
  }

  private RecordSerializer<GenericRecord> getValueRecordSerializer(int schemaID) {
    return recordSerializerMap.computeIfAbsent(schemaID, schemaId -> {
      Schema schema  = schemaRepository.getValueSchema(storeName, schemaId).getSchema();
      return FastSerializerDeserializerFactory.getFastAvroGenericSerializer(schema);
    });
  */
  }

  /**
   * Perform conflict resolution when the incoming operation is a PUT operation.
   * @param oldReplicationMetadata The replication metadata of the currently persisted value.
   * @param schemaIdOfOldValue The schema id of the currently persisted value.
   * @param writeOperationTimestamp The logical timestamp of the incoming record.
   * @return A MergeConflictResult which denotes what update should be applied or if the operation should be ignored.
   */
  public MergeConflictResult delete(ByteBuffer oldReplicationMetadata, int schemaIdOfOldValue, long writeOperationTimestamp) {
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
      byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(schemaIdOfOldValue).serialize(newReplicationMetadata);

      return new MergeConflictResult(null, schemaIdOfOldValue, ByteBuffer.wrap(newReplicationMetadataBytes), false);
    }

    if (schemaIdOfOldValue <= 0) {
      throw new VeniceException("Invalid schema Id of old value found when replication metadata exists for store = " + storeName + "; schema ID = " + schemaIdOfOldValue);
    }

    GenericRecord replicationMetadataRecord = getReplicationMetadataDeserializer(schemaIdOfOldValue).deserialize(oldReplicationMetadata);
    Object tsObject = replicationMetadataRecord.get(TIMESTAMP_FIELD);
    Merge.TSMDType tsmdType = Merge.getTSMDType(tsObject);

    if (tsmdType == Merge.TSMDType.ROOT_LEVEL_TS) {
      long oldTimestamp = (long) tsObject;
      // delete wins on tie
      if (oldTimestamp <= writeOperationTimestamp) {
        // update RMD ts
        replicationMetadataRecord.put(TIMESTAMP_FIELD, writeOperationTimestamp);
        byte[] newReplicationMetadataBytes = getReplicationMetadataSerializer(schemaIdOfOldValue).serialize(replicationMetadataRecord);

        return new MergeConflictResult(null, schemaIdOfOldValue, ByteBuffer.wrap(newReplicationMetadataBytes), false);
      } else { // keep the old value
        return MergeConflictResult.getIgnoredResult();
      }
    } else {
      throw new VeniceUnsupportedOperationException("Field level MD not supported");
    }

    /*
    GenericRecord timestampRecordForOldValue = (GenericRecord) tsObject;
    List<Schema.Field> fields = timestampRecordForOldValue.getSchema().getFields();
    boolean newTsLarger = false, newTsSmaller = false;
    for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
      Schema.Field field = fields.get(i);
      long fieldTimestamp = (long) timestampRecordForOldValue.get(field.pos());
      if (fieldTimestamp > writeOperationTimestamp) {
        newTsLarger = true;
      } else if (fieldTimestamp < writeOperationTimestamp) {
        newTsSmaller = true;
      }
    }

    // deserialize old record to update field-level timestamps
    if (newTsLarger && newTsSmaller) {
      GenericRecord oldRecord = getValueRecordDeserializer(oldSchemaID, newSchemaID).deserialize(oldValueAndTimestampMetadata.getValue());
      ValueAndTimestampMetadata<GenericRecord> valueAndTimestampMetadata = new ValueAndTimestampMetadata<>(oldRecord, oldValueAndTimestampMetadata.getTimestampMetadata());
      valueAndTimestampMetadata = mergeGenericRecord.delete(valueAndTimestampMetadata, writeOperationTimestamp);

      // serialize back to ByteBuffer to put in storage engine
      ValueAndTimestampMetadata<GenericRecord> finalValueAndTimestampMetadata = valueAndTimestampMetadata;
      oldValueAndTimestampMetadata.setValue(ByteBuffer.wrap(getRecordSerializer(newSchemaID).serialize(finalValueAndTimestampMetadata.getValue())));
    } else if (newTsLarger) { // delete wins
      oldValueAndTimestampMetadata.setValue(null);
      oldValueAndTimestampMetadata.setUpdateIgnored(false);
    } else { // old ts larger  keep the old value.
      oldValueAndTimestampMetadata.setUpdateIgnored(true);
    }
    return oldValueAndTimestampMetadata;
     */
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
    TimestampMetadataSchemaEntry
        replicationMetadataSchemaEntry = schemaRepository.getTimestampMetadataSchema(storeName, valueSchemaId,
        replicationMetadataVersionId);
    if (replicationMetadataSchemaEntry == null) {
      throw new VeniceException("Unable to fetch replication metadata schema from schema repository");
    }
    return replicationMetadataSchemaEntry.getSchema();
  }
}