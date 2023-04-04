package com.linkedin.venice.schema.writecompute;

import static com.linkedin.venice.schema.SchemaUtils.isArrayField;
import static com.linkedin.venice.schema.SchemaUtils.isMapField;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.venice.schema.SchemaUtils;
import com.linkedin.venice.schema.merge.AvroCollectionElementComparator;
import com.linkedin.venice.schema.merge.CollectionFieldOperationHandler;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.schema.merge.SortBasedCollectionFieldOpHandler;
import com.linkedin.venice.schema.merge.UpdateResultStatus;
import com.linkedin.venice.schema.merge.ValueAndRmd;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


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
    this.collectionFieldOperationHandler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);
  }

  /**
   * Handle partial update request on a value record that has associated replication metadata.
   */
  public ValueAndRmd<GenericRecord> updateRecordWithRmd(
      @Nonnull Schema currValueSchema,
      @Nonnull ValueAndRmd<GenericRecord> currRecordAndRmd,
      @Nonnull GenericRecord writeComputeRecord,
      final long updateOperationTimestamp,
      final int coloID) {
    // For now we always create a record if the current one is null. But there could be a case where the created record
    // does not get updated as a result of this update method. In this case, the current record should stay being null
    // instead
    // of being all record with all fields having their default value. TODO: handle this case.
    GenericRecord currValueRecord = currRecordAndRmd.getValue();
    if (currValueRecord == null) {
      currRecordAndRmd.setValue(SchemaUtils.createGenericRecord(currValueSchema));
    }

    Object timestampObject = currRecordAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME);
    if (!(timestampObject instanceof GenericRecord)) {
      throw new IllegalStateException(
          String.format(
              "Expect the %s field to have a generic record. Got replication metadata: %s",
              TIMESTAMP_FIELD_NAME,
              currRecordAndRmd.getRmd()));
    }

    final GenericRecord timestampRecord = (GenericRecord) timestampObject;
    if (!WriteComputeOperation.isPartialUpdateOp(writeComputeRecord)) {
      // This Write Compute record could be a Write Compute Delete request which is not supported and there should be no
      // one using it.
      throw new IllegalStateException(
          "Write Compute only support partial update. Got unexpected Write Compute record: " + writeComputeRecord);
    }
    boolean notUpdated = true;
    final Schema writeComputeSchema = writeComputeRecord.getSchema();
    for (Schema.Field writeComputeField: writeComputeSchema.getFields()) {
      final String writeComputeFieldName = writeComputeField.name();
      if (currRecordAndRmd.getValue().getSchema().getField(writeComputeFieldName) == null) {
        throw new IllegalStateException(
            "Current value record must have a schema that has the same field names as the "
                + "write compute schema because the current value's schema should be the schema that is used to generate "
                + "the write-compute schema. Got missing field: " + writeComputeFieldName);
      }

      Object writeComputeFieldValue = writeComputeRecord.get(writeComputeFieldName);
      WriteComputeOperation operationType = WriteComputeOperation.getFieldOperationType(writeComputeFieldValue);
      switch (operationType) {
        case NO_OP_ON_FIELD:
          continue; // Do nothing

        case PUT_NEW_FIELD:
          UpdateResultStatus putResult = mergeRecordHelper.putOnField(
              currRecordAndRmd.getValue(),
              timestampRecord,
              writeComputeFieldName,
              writeComputeFieldValue,
              updateOperationTimestamp,
              coloID);
          notUpdated &= (putResult.equals(UpdateResultStatus.NOT_UPDATED_AT_ALL));
          continue;

        case LIST_OPS:
        case MAP_OPS:
          UpdateResultStatus collectionMergeResult = modifyCollectionField(
              assertAndGetTsRecordFieldIsRecord(timestampRecord, writeComputeFieldName),
              (GenericRecord) writeComputeFieldValue,
              updateOperationTimestamp,
              currRecordAndRmd.getValue(),
              writeComputeFieldName);
          notUpdated &= (collectionMergeResult.equals(UpdateResultStatus.NOT_UPDATED_AT_ALL));
          continue;
        default:
          throw new IllegalStateException("Unexpected write-compute operation: " + operationType);
      }
    }
    if (notUpdated) {
      currRecordAndRmd.setUpdateIgnored(true);
    }
    return currRecordAndRmd;
  }

  private UpdateResultStatus modifyCollectionField(
      GenericRecord fieldTimestampRecord,
      GenericRecord fieldWriteComputeRecord,
      long modifyTimestamp,
      GenericRecord currValueRecord,
      String fieldName) {
    if (isArrayField(currValueRecord, fieldName)) {
      return collectionFieldOperationHandler.handleModifyList(
          modifyTimestamp,
          new CollectionRmdTimestamp(fieldTimestampRecord),
          currValueRecord,
          fieldName,
          (List<Object>) fieldWriteComputeRecord.get(WriteComputeConstants.SET_UNION),
          (List<Object>) fieldWriteComputeRecord.get(WriteComputeConstants.SET_DIFF));

    } else if (isMapField(currValueRecord, fieldName)) {
      Object fieldValue = currValueRecord.get(fieldName);
      if (fieldValue != null && !(fieldValue instanceof IndexedHashMap)) {
        // if the current map field is not of IndexedHashMap type and is empty then replace this field with an empty
        // IndexedHashMap
        if (((Map) fieldValue).isEmpty()) {
          currValueRecord.put(fieldName, new IndexedHashMap<>());
        } else {
          throw new IllegalStateException(
              "Expect value of field " + fieldName + " to be an IndexedHashMap. Got: " + fieldValue.getClass());
        }
      }
      return collectionFieldOperationHandler.handleModifyMap(
          modifyTimestamp,
          new CollectionRmdTimestamp(fieldTimestampRecord),
          currValueRecord,
          fieldName,
          (Map<String, Object>) fieldWriteComputeRecord.get(WriteComputeConstants.MAP_UNION),
          (List<String>) fieldWriteComputeRecord.get(WriteComputeConstants.MAP_DIFF));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Expect value field %s to be either a List or a Map. Got value record: %s",
              fieldName,
              currValueRecord));
    }
  }

  private GenericRecord assertAndGetTsRecordFieldIsRecord(GenericRecord timestampRecord, String fieldName) {
    Object fieldTimestamp = timestampRecord.get(fieldName);
    if (!(fieldTimestamp instanceof GenericRecord)) {
      throw new IllegalStateException(
          String.format(
              "Expect field %s in the timestamp record to be a generic record. Got timestamp record: %s",
              fieldTimestamp,
              timestampRecord));
    }
    return (GenericRecord) fieldTimestamp;
  }
}
