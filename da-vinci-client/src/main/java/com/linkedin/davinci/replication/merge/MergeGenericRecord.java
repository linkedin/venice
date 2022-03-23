package com.linkedin.davinci.replication.merge;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.venice.schema.merge.UpdateResultStatus;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.merge.ValueAndReplicationMetadata;
import com.linkedin.venice.schema.rmd.v1.ReplicationMetadataSchemaGeneratorV1;
import com.linkedin.venice.schema.writecompute.WriteComputeProcessor;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;

import static com.linkedin.venice.schema.rmd.ReplicationMetadataConstants.*;


/**
 * Implementations of the API defined in {@link Merge} based on V1 metadata timestamp Schema generated by
 * {@link ReplicationMetadataSchemaGeneratorV1}.
 * All the implementations assume replication metadata format is union record type [long, record] where record
 * is top-level fieldName:timestamp format.
 * 1. Currently collection merging is not supported as replication metadata does not support it yet.
 * 2. schema evolution is not supported, so it assumes incoming and old schema are same else else throws VeniceException
 * 3. Assumes new value to be GenericRecord type, does not support non-record values.
 */
class MergeGenericRecord extends AbstractMerge<GenericRecord> {
  private static final AvroVersion RUNTIME_AVRO_VERSION = AvroCompatibilityHelper.getRuntimeAvroVersion();
  private final WriteComputeProcessor writeComputeProcessor;
  private final MergeRecordHelper mergeRecordHelper;

  MergeGenericRecord(WriteComputeProcessor writeComputeProcessor, MergeRecordHelper mergeRecordHelper) {
    Validate.notNull(writeComputeProcessor);
    Validate.notNull(mergeRecordHelper);
    this.writeComputeProcessor = writeComputeProcessor;
    this.mergeRecordHelper = mergeRecordHelper;
  }

  @Override
  public ValueAndReplicationMetadata<GenericRecord> put(
      ValueAndReplicationMetadata<GenericRecord> oldValueAndReplicationMetadata,
      GenericRecord newValue,
      long putOperationTimestamp,
      int putOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID
  ) {
    final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();
    final GenericRecord oldValue = oldValueAndReplicationMetadata.getValue();

    // TODO support schema evolution and caching the result of schema validation.
    if (oldValue != null && !oldValue.getSchema().equals(newValue.getSchema())) {
      throw new VeniceException("Incoming schema " + newValue.getSchema() + " is not same as existing schema" + oldValue.getSchema());
    }

    final Object tsObject = oldReplicationMetadata.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(tsObject);

    switch (rmdTimestampType) {
      case VALUE_LEVEL_TIMESTAMP:
        return putWithRecordLevelTimestamp(
            (long) tsObject,
            oldValueAndReplicationMetadata,
            putOperationTimestamp,
            newValueSourceOffset,
            newValueSourceBrokerID,
            newValue
        );

      case PER_FIELD_TIMESTAMP:
        return handlePutWithPerFieldLevelTimestamp(
            (GenericRecord) tsObject,
            putOperationTimestamp,
            newValueSourceOffset,
            newValueSourceBrokerID,
            putOperationColoID,
            oldValueAndReplicationMetadata,
            newValue
        );

      default:
        throw new VeniceException("Invalid replication metadata type"  + rmdTimestampType);
    }
  }

  private ValueAndReplicationMetadata<GenericRecord> handlePutWithPerFieldLevelTimestamp(
      final GenericRecord timestampRecordForOldValue,
      final long putOperationTimestamp,
      final long sourceOffsetOfNewValue,
      final int newValueSourceBrokerID,
      final int putOperationColoID,
      ValueAndReplicationMetadata<GenericRecord> oldValueAndReplicationMetadata,
      GenericRecord newValue
  ) {
    final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();
    final GenericRecord oldValue = oldValueAndReplicationMetadata.getValue();

    // TODO: Support schema evolution, as the following assumes old/new schema are same.
    updateReplicationCheckpointVector(oldReplicationMetadata, sourceOffsetOfNewValue, newValueSourceBrokerID);

    // update the field values based on replication metadata
    List<Schema.Field> oldTimestampRecordFields = timestampRecordForOldValue.getSchema().getFields();
    boolean allFieldsNew = true;
    boolean noFieldUpdated = true;
    for (Schema.Field oldTimestampRecordField : oldTimestampRecordFields) {
      final String fieldName = oldTimestampRecordField.name();
      UpdateResultStatus fieldUpdateResult = mergeRecordHelper.putOnField(
          oldValue,
          timestampRecordForOldValue,
          fieldName,
          newValue.get(fieldName),
          putOperationTimestamp,
          putOperationColoID
      );

      allFieldsNew &= (fieldUpdateResult == UpdateResultStatus.COMPLETELY_UPDATED);
      noFieldUpdated &= (fieldUpdateResult == UpdateResultStatus.NOT_UPDATE);
    }
    if (allFieldsNew) {
      oldReplicationMetadata.put(TIMESTAMP_FIELD_NAME, putOperationTimestamp);
    }
    if (noFieldUpdated) {
      oldValueAndReplicationMetadata.setUpdateIgnored(true);
    }
    return oldValueAndReplicationMetadata;
  }

  @Override
  public ValueAndReplicationMetadata<GenericRecord> delete(
      ValueAndReplicationMetadata<GenericRecord> oldValueAndReplicationMetadata,
      long deleteOperationTimestamp,
      int deleteOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID
  ) {
    if (RUNTIME_AVRO_VERSION.earlierThan(AvroVersion.AVRO_1_7)) {
      throw new VeniceException("'delete' operation won't work properly with Avro version before 1.7 and"
          + " the runtime Avro version is: " + RUNTIME_AVRO_VERSION);
    }

    final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();
    final Object tsObject = oldReplicationMetadata.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(tsObject);

    switch (rmdTimestampType) {
      case VALUE_LEVEL_TIMESTAMP:
        return deleteWithValueLevelTimestamp(
            (long) tsObject,
            deleteOperationTimestamp,
            newValueSourceOffset,
            newValueSourceBrokerID,
            oldValueAndReplicationMetadata
        );

      case PER_FIELD_TIMESTAMP:
        updateReplicationCheckpointVector(oldReplicationMetadata, newValueSourceOffset, newValueSourceBrokerID);
        UpdateResultStatus recordDeleteResultStatus = mergeRecordHelper.deleteRecord(
            oldValueAndReplicationMetadata.getValue(),
            (GenericRecord) tsObject,
            deleteOperationTimestamp,
            deleteOperationColoID
        );

        if (recordDeleteResultStatus == UpdateResultStatus.COMPLETELY_UPDATED) {
          // Full delete
          oldValueAndReplicationMetadata.setValue(null);
          oldReplicationMetadata.put(TIMESTAMP_FIELD_NAME, deleteOperationTimestamp);
        } else if (recordDeleteResultStatus == UpdateResultStatus.NOT_UPDATE) {
          oldValueAndReplicationMetadata.setUpdateIgnored(true);
        }
        return oldValueAndReplicationMetadata;

      default:
        throw new VeniceException("Invalid replication metadata type type"  + rmdTimestampType);
    }
  }

  @Override
  public ValueAndReplicationMetadata<GenericRecord> update(
      ValueAndReplicationMetadata<GenericRecord> oldValueAndReplicationMetadata,
      Lazy<GenericRecord> writeComputeRecord,
      Schema currValueSchema, // Schema of the current value that is to-be-updated here.
      Schema writeComputeSchema,
      long updateOperationTimestamp,
      int updateOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID
  ) {
    updateReplicationCheckpointVector(oldValueAndReplicationMetadata.getReplicationMetadata(), newValueSourceOffset, newValueSourceBrokerID);
    return writeComputeProcessor.updateRecordWithRmd(
        currValueSchema,
        writeComputeSchema,
        oldValueAndReplicationMetadata,
        writeComputeRecord.get(),
        updateOperationTimestamp,
        updateOperationColoID
    );
  }

  @Override
  GenericRecord compareAndReturn(GenericRecord oldValue, GenericRecord newValue) {
    return (GenericRecord) MergeUtils.compareAndReturn(oldValue, newValue); // TODO: use a object-content-based comparator.
  }
}
