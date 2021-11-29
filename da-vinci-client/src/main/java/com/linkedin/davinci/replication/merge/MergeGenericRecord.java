package com.linkedin.davinci.replication.merge;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.schema.ReplicationMetadataSchemaGeneratorV1;
import com.linkedin.venice.utils.Lazy;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.VeniceConstants.*;


/**
 * Implementations of the API defined in {@link Merge} based on V1 metadata timestamp Schema generated by
 * {@link ReplicationMetadataSchemaGeneratorV1}.
 * All the implementations assume replication metadata format is union record type [long, record] where record
 * is top-level fieldName:timestamp format.
 * 1. Currently collection merging is not supported as replication metadata does not support it yet.
 * 2. schema evolution is not supported, so it assumes incoming and old schema are same else else throws VeniceException
 * 3. Assumes new value to be GenericRecord type, does not support non-record values.
 */
class MergeGenericRecord implements Merge<GenericRecord> {
  private static final MergeGenericRecord INSTANCE = new MergeGenericRecord();

  private static final AvroVersion RUNTIME_AVRO_VERSION = AvroCompatibilityHelper.getRuntimeAvroVersion();

  private MergeGenericRecord() {}

  static MergeGenericRecord getInstance() {
    return INSTANCE;
  }

  @Override
  public ValueAndReplicationMetadata<GenericRecord> put(
      ValueAndReplicationMetadata<GenericRecord> oldValueAndReplicationMetadata, GenericRecord newValue,
      long writeOperationTimestamp, long sourceOffsetOfNewValue, int sourceBrokerIDOfNewValue) {
    final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();
    final GenericRecord oldValue = oldValueAndReplicationMetadata.getValue();

    // TODO support schema evolution and caching the result of schema validation.
    if (oldValue != null && !oldValue.getSchema().equals(newValue.getSchema())) {
      throw new VeniceException("Incoming schema " + newValue.getSchema() + " is not same as existing schema" + oldValue.getSchema());
    }

    final Object tsObject = oldReplicationMetadata.get(TIMESTAMP_FIELD_NAME);
    ReplicationMetadataType replicationMetadataType = Merge.getReplicationMetadataType(tsObject);

    switch (replicationMetadataType) {
      case ROOT_LEVEL_TIMESTAMP:
        long oldTimeStamp = (long) tsObject;
        if (oldTimeStamp < writeOperationTimestamp) {
          // New value wins
          oldValueAndReplicationMetadata.setValue(newValue);
          oldReplicationMetadata.put(TIMESTAMP_FIELD_NAME, writeOperationTimestamp);
          oldReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
              Merge.mergeOffsetVectors((List<Long>)oldReplicationMetadata.get(REPLICATION_CHECKPOINT_VECTOR_FIELD), sourceOffsetOfNewValue, sourceBrokerIDOfNewValue));
        } else if (oldTimeStamp == writeOperationTimestamp) {
          // for timestamp tie, if old value was null persist new value.
          if (oldValue == null) {
            oldValueAndReplicationMetadata.setValue(newValue);
            oldReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
                Merge.mergeOffsetVectors((List<Long>) oldReplicationMetadata.get(REPLICATION_CHECKPOINT_VECTOR_FIELD), sourceOffsetOfNewValue, sourceBrokerIDOfNewValue));
          } else {
            // else let compare decide which one to store.
            oldValueAndReplicationMetadata.setValue((GenericRecord) Merge.compareAndReturn(oldValue, newValue));
            oldReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
                Merge.mergeOffsetVectors((List<Long>) oldReplicationMetadata.get(REPLICATION_CHECKPOINT_VECTOR_FIELD), sourceOffsetOfNewValue, sourceBrokerIDOfNewValue));
          }
        } else {
          oldValueAndReplicationMetadata.setValue(oldValue);
        }
        return oldValueAndReplicationMetadata;

      case PER_FIELD_TIMESTAMP:
        GenericRecord timestampRecordForOldValue = (GenericRecord) tsObject;

        // TODO: Support schema evolution, as the following assumes old/new schema are same.
        oldValueAndReplicationMetadata.setValue(newValue);
        oldReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
            Merge.mergeOffsetVectors((List<Long>) oldReplicationMetadata.get(REPLICATION_CHECKPOINT_VECTOR_FIELD), sourceOffsetOfNewValue, sourceBrokerIDOfNewValue));

        // update the field values based on replication metadata
        List<Schema.Field> oldTimestampRecordFields = timestampRecordForOldValue.getSchema().getFields();
        boolean allFieldsNew = true;
        for (Schema.Field oldTimestampRecordField : oldTimestampRecordFields) {
          long oldFieldTimestamp = (long) timestampRecordForOldValue.get(oldTimestampRecordField.pos());

          if (oldFieldTimestamp > writeOperationTimestamp) {
            // Old value field wins
            newValue.put(oldTimestampRecordField.name(), oldValue.get(oldTimestampRecordField.pos()));
            allFieldsNew = false;
          } else if (oldFieldTimestamp == writeOperationTimestamp) {
            Object o1 = oldValue.get(oldTimestampRecordField.name());
            Object o2 = newValue.get(oldTimestampRecordField.name());

            // keep the old value in case of timestamp tie
            newValue.put(oldTimestampRecordField.name(), Merge.compareAndReturn(o1, o2));
            allFieldsNew = false;
          } else {
            // update the timestamp since writeOperationTimestamp wins
            timestampRecordForOldValue.put(oldTimestampRecordField.name(), writeOperationTimestamp);
          }
        }
        if (allFieldsNew) {
          oldReplicationMetadata.put(TIMESTAMP_FIELD_NAME, writeOperationTimestamp);
        }
        return oldValueAndReplicationMetadata;

      default:
        throw new VeniceException("Invalid replication metadata type"  + replicationMetadataType);
    }
  }

  @Override
  public ValueAndReplicationMetadata<GenericRecord> delete(
      ValueAndReplicationMetadata<GenericRecord> oldValueAndReplicationMetadata,
      long writeOperationTimestamp, long sourceOffsetOfNewValue, int sourceBrokerIDOfNewValue) {
    if (RUNTIME_AVRO_VERSION.earlierThan(AvroVersion.AVRO_1_7)) {
      throw new VeniceException("'delete' operation won't work properly with Avro version before 1.7 and"
          + " the runtime Avro version is: " + RUNTIME_AVRO_VERSION);
    }

    final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();
    final Object tsObject = oldReplicationMetadata.get(TIMESTAMP_FIELD_NAME);
    ReplicationMetadataType replicationMetadataType = Merge.getReplicationMetadataType(tsObject);

    // Always update the vector field
    oldReplicationMetadata.put(REPLICATION_CHECKPOINT_VECTOR_FIELD,
        Merge.mergeOffsetVectors((List<Long>)oldReplicationMetadata.get(REPLICATION_CHECKPOINT_VECTOR_FIELD), sourceOffsetOfNewValue, sourceBrokerIDOfNewValue));

    switch (replicationMetadataType) {
      case ROOT_LEVEL_TIMESTAMP:
        long oldTimeStamp = (long)tsObject;
        // delete wins when old and new write operation timestamps are equal.
        if (oldTimeStamp <= writeOperationTimestamp) {
          oldValueAndReplicationMetadata.setValue(null);
          oldReplicationMetadata.put(TIMESTAMP_FIELD_NAME, writeOperationTimestamp);
        }
        return oldValueAndReplicationMetadata;

      case PER_FIELD_TIMESTAMP:
        final GenericRecord oldValue = oldValueAndReplicationMetadata.getValue();
        final GenericRecord oldTimestampRecord = (GenericRecord)tsObject;
        boolean anyOldFieldWon = false;

        List<Schema.Field> oldTimestampRecordFields = oldTimestampRecord.getSchema().getFields();
        for (Schema.Field oldTimestampRecordField : oldTimestampRecordFields) {
          long oldFieldTimestamp = (long) oldTimestampRecord.get(oldTimestampRecordField.pos());
          if (oldFieldTimestamp <= writeOperationTimestamp) {
            Schema.Field oldValueField = oldValue.getSchema().getField(oldTimestampRecordField.name());
            // When a field is deleted, its default value is set.
            oldValue.put(oldTimestampRecordField.name(),
                GenericData.get().deepCopy(oldValueField.schema(), GenericData.get().getDefaultValue(oldValueField)));
            oldTimestampRecord.put(oldTimestampRecordField.name(), writeOperationTimestamp);
          } else {
            anyOldFieldWon = true;
          }
        }

        if (anyOldFieldWon) {
          oldValueAndReplicationMetadata.setValue(oldValue);
        } else {
          // all fields are older than write timestamp, do full delete
          oldValueAndReplicationMetadata.setValue(null);
          // update the timestamp since writeOperationTimestamp wins
          oldReplicationMetadata.put(TIMESTAMP_FIELD_NAME, writeOperationTimestamp);
        }
        return oldValueAndReplicationMetadata;

      default:
        throw new VeniceException("Invalid replication metadata type type"  + replicationMetadataType);
    }
  }

  @Override
  public ValueAndReplicationMetadata<GenericRecord> update(
      ValueAndReplicationMetadata<GenericRecord> oldValueAndReplicationMetadata,
      Lazy<GenericRecord> writeComputeRecord, long writeOperationTimestamp, long sourceOffsetOfNewValue, int sourceBrokerIDOfNewValue) {
    throw new VeniceUnsupportedOperationException("update operation not yet supported.");
  }
}
