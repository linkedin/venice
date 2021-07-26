package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.utils.Lazy;
import java.nio.ByteBuffer;
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
public class MergeConfictResolver {
  private final String storeName;
  private final ReadOnlySchemaRepository schemaRepository;

  public MergeConfictResolver(ReadOnlySchemaRepository schemaRepository, String storeName) {
    this.schemaRepository = schemaRepository;
    this.storeName = storeName;
  }

  /**
   * TODO: replicationMD should be ByteBuffer type and RMD deserializer util should be used to deser that. Also handle when replicationMD is null in which case new write wins
   */
  public MergeConflictResult put(Lazy<ByteBuffer> oldValue, GenericRecord replicationMD, ByteBuffer newValue,
      long writeOperationTimestamp, int schemaIDOfOldValue, int schemaIDOfNewValue) {
    MergeConflictResult mergeConflictResult = new MergeConflictResult();
    Object tsObject = replicationMD.get(TIMESTAMP_FIELD);
    Merge.TSMDType tsmdType = Merge.getTSMDType(tsObject);

    if (tsmdType == Merge.TSMDType.ROOT_LEVEL_TS) {
      long oldTimestamp = (long) tsObject;

      // if new write timestamp is larger, new value wins.
      if (oldTimestamp < writeOperationTimestamp) {
        mergeConflictResult.setResultByteBuffer(newValue);
        mergeConflictResult.setUpdateIgnored(false);
        replicationMD.put(TIMESTAMP_FIELD, writeOperationTimestamp);
        mergeConflictResult.setReplicationMetadata(replicationMD);
        mergeConflictResult.setResultSchemaID(schemaIDOfNewValue);
      } else if (oldTimestamp > writeOperationTimestamp) { // no-op as new ts is stale, ignore update
        mergeConflictResult.setUpdateIgnored(true);
      } else { // tie in old and new ts, decide persistence based on object comparison
        ByteBuffer compareResult = (ByteBuffer) Merge.compareAndReturn(oldValue.get(), newValue);
        mergeConflictResult.setResultByteBuffer(compareResult);
        mergeConflictResult.setUpdateIgnored(compareResult != newValue);
        mergeConflictResult.setReplicationMetadata(replicationMD);  // no need to update RMD ts
        mergeConflictResult.setResultSchemaID(compareResult == newValue ? schemaIDOfNewValue : schemaIDOfOldValue);
      }
      return mergeConflictResult;
    } else {
      throw new VeniceUnsupportedOperationException("Field level MD not supported");
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
      Schema oldSchemna = schemaRepository.getValueSchema(storeName, oldSchemaID).getSchema();
      Schema newSchema  = schemaRepository.getValueSchema(storeName, newSchemaID).getSchema();

      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(newSchema, oldSchemna);
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
   * TODO: replicationMD should be ByteBuffer type and RMD deserializer util should be used to deser that. Also handle when replicationMD is null in which case delete wins
   */
  public MergeConflictResult delete(GenericRecord replicationMD, long writeOperationTimestamp) {
    Object tsObject = replicationMD.get(TIMESTAMP_FIELD);
    Merge.TSMDType tsmdType = Merge.getTSMDType(tsObject);
    MergeConflictResult mergeConflictResult = new MergeConflictResult();

    if (tsmdType == Merge.TSMDType.ROOT_LEVEL_TS) {
      long oldTimestamp = (long) tsObject;
      // delete wins on tie
      if (oldTimestamp <= writeOperationTimestamp) {
        mergeConflictResult.setResultByteBuffer(null);
        mergeConflictResult.setUpdateIgnored(false);
        // update RMD ts
        replicationMD.put(TIMESTAMP_FIELD, writeOperationTimestamp);
        mergeConflictResult.setReplicationMetadata(replicationMD);
      } else { // keep the old value
        mergeConflictResult.setUpdateIgnored(true);
      }
      return mergeConflictResult;
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
}
