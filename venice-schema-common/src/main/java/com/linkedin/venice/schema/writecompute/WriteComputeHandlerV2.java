package com.linkedin.venice.schema.writecompute;

import com.linkedin.venice.schema.merge.ValueAndReplicationMetadata;
import com.linkedin.venice.schema.merge.AvroCollectionElementComparator;
import com.linkedin.venice.schema.merge.CollectionFieldOperationHandler;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.schema.merge.SortBasedCollectionFieldOpHandler;
import com.linkedin.venice.schema.merge.UpdateResultStatus;
import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.schema.SchemaUtils;
import com.linkedin.venice.schema.rmd.v2.CollectionReplicationMetadata;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;

import static com.linkedin.venice.VeniceConstants.*;


/**
 * Write compute V2 handles value records with replication metadata.
 */
public class WriteComputeHandlerV2 extends WriteComputeHandlerV1 {

  private final MergeRecordHelper mergeRecordHelper;
  private final CollectionFieldOperationHandler collectionFieldOperationHandler;

  WriteComputeHandlerV2(MergeRecordHelper mergeRecordHelper) {
    Validate.notNull(mergeRecordHelper);
    this.mergeRecordHelper = mergeRecordHelper;
    // TODO: get this variable as a argument passed to this constructor.
    this.collectionFieldOperationHandler = new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);
  }

  public ValueAndReplicationMetadata<GenericRecord> updateRecord(
      @Nonnull Schema currValueSchema,
      @Nonnull Schema writeComputeSchema,
      @Nonnull ValueAndReplicationMetadata<GenericRecord> currRecordAndRmd,
      @Nonnull GenericRecord writeComputeRecord,
      final long updateOperationTimestamp,
      int coloID
  ) {

    // For now we always create a record if the current one is null. But there could be a case where the created record
    // does not get updated as a result of this update method. In this case, the current record should stay being null instead
    // of being all record with all fields having their default value. TODO: handle this case.
    GenericRecord currValueRecord = currRecordAndRmd.getValue();
    if (currValueRecord == null) {
      currRecordAndRmd.setValue(SchemaUtils.constructGenericRecord(currValueSchema));
    }

    // TODO: RMD could be null or not have per-field timestamp. Caller of this method will handle these cases and ensure
    // that RMD here does have per-field timestamp.
    Object timestampObject = currRecordAndRmd.getReplicationMetadata().get(TIMESTAMP_FIELD_NAME);
    if (!(timestampObject instanceof GenericRecord)) {
      throw new IllegalStateException(
          String.format("Expect the %s field to have a generic record. Got replication metadata: %s",
              TIMESTAMP_FIELD_NAME,
              currRecordAndRmd.getReplicationMetadata()
          ));
    }

    final GenericRecord timestampRecord = (GenericRecord) timestampObject;
    if (writeComputeSchema.getType() == Schema.Type.UNION) {
      if (WriteComputeSchemaConverter.isDeleteRecordOp(writeComputeRecord)) {
        // Full delete
        UpdateResultStatus recordDeleteResultStatus = mergeRecordHelper.deleteRecord(
            currRecordAndRmd.getValue(),
            timestampRecord,
            updateOperationTimestamp,
            coloID
        );
        if (recordDeleteResultStatus == UpdateResultStatus.COMPLETELY_UPDATED) {
          // All fields are deleted.
          currRecordAndRmd.setValue(null);
        } else if (recordDeleteResultStatus == UpdateResultStatus.NOT_UPDATE) {
          currRecordAndRmd.setUpdateIgnored(true);
        }
        return currRecordAndRmd;

      } else {
        // The first one in union schema list should be the value schema.
        writeComputeSchema = writeComputeSchema.getTypes().get(0);
      }
    }

    for (Schema.Field writeComputeField : writeComputeSchema.getFields()) {
      final String writeComputeFieldName = writeComputeField.name();
      if (currRecordAndRmd.getValue().getSchema().getField(writeComputeFieldName) == null) {
        throw new IllegalStateException("Current value record must have a schema that has the same field names as the "
            + "write compute schema because the current value's schema should be the schema that is used to generate "
            + "the write-compute schema. Got missing field: " + writeComputeFieldName);
      }

      Object writeComputeFieldValue = writeComputeRecord.get(writeComputeFieldName);
      WriteComputeOperation operationType = WriteComputeSchemaConverter.getFieldOperationType(writeComputeFieldValue);
      switch (operationType) {
        case NO_OP_ON_FIELD:
          break; // Do nothing

        case PUT_NEW_FIELD:
          mergeRecordHelper.putOnField(
              currRecordAndRmd.getValue(),
              timestampRecord,
              writeComputeFieldName,
              writeComputeFieldValue,
              updateOperationTimestamp,
              coloID
          );
          break;

        case LIST_OPS:
        case MAP_OPS:
          modifyCollectionField(
              assertAndGetTsRecordFieldIsRecord(timestampRecord, writeComputeFieldName),
              (GenericRecord) writeComputeFieldValue,
              updateOperationTimestamp,
              currRecordAndRmd.getValue(),
              writeComputeFieldName
          );
          break;
        default:
          throw new IllegalStateException("Unexpected write-compute operation: " + operationType);
      }
    }
    return currRecordAndRmd;
  }

  private void modifyCollectionField(
      GenericRecord fieldTimestampRecord,
      GenericRecord fieldWriteComputeRecord,
      long modifyTimestamp,
      GenericRecord currValueRecord,
      String fieldName
  ) {
    Object fieldValue = currValueRecord.get(fieldName);
    if (fieldValue instanceof List) {

      collectionFieldOperationHandler.handleModifyList(
          modifyTimestamp,
          new CollectionReplicationMetadata(fieldTimestampRecord),
          currValueRecord,
          fieldName,
          (List<Object>) fieldWriteComputeRecord.get(WriteComputeConstants.SET_UNION),
          (List<Object>) fieldWriteComputeRecord.get(WriteComputeConstants.SET_DIFF)
      );

    } else if (fieldValue instanceof Map) {
      if (!(fieldValue instanceof IndexedHashMap)) {
        throw new IllegalStateException("Expect value of field " + fieldName + " to be an IndexedHashMap. Got: " + fieldValue.getClass());
      }

      collectionFieldOperationHandler.handleModifyMap(
          modifyTimestamp,
          new CollectionReplicationMetadata(fieldTimestampRecord),
          currValueRecord,
          fieldName,
          (Map<String, Object>) fieldWriteComputeRecord.get(WriteComputeConstants.MAP_UNION),
          (List<String>) fieldWriteComputeRecord.get(WriteComputeConstants.MAP_DIFF)
      );

    } else {
      throw new IllegalArgumentException(
          String.format("Expect value field %s to be either a List or a Map. Got value record: %s", fieldName, currValueRecord)
      );
    }
  }

  private GenericRecord assertAndGetTsRecordFieldIsRecord(GenericRecord timestampRecord, String fieldName) {
    Object fieldTimestamp = timestampRecord.get(fieldName);
    if (!(fieldTimestamp instanceof GenericRecord)) {
      throw new IllegalStateException(
          String.format("Expect field %s in the timestamp record to be a generic record. Got timestamp record: %s",
              fieldTimestamp,
              timestampRecord
          ));
    }
    return (GenericRecord) fieldTimestamp;
  }
}
