package com.linkedin.davinci.schema.writecompute;

import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_POS;

import com.linkedin.davinci.schema.SchemaUtils;
import com.linkedin.davinci.schema.merge.AvroCollectionElementComparator;
import com.linkedin.davinci.schema.merge.CollectionFieldOperationHandler;
import com.linkedin.davinci.schema.merge.MergeRecordHelper;
import com.linkedin.davinci.schema.merge.SortBasedCollectionFieldOpHandler;
import com.linkedin.davinci.schema.merge.UpdateResultStatus;
import com.linkedin.davinci.schema.merge.ValueAndRmd;
import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.schema.writecompute.WriteComputeConstants;
import com.linkedin.venice.schema.writecompute.WriteComputeHandlerV1;
import com.linkedin.venice.schema.writecompute.WriteComputeOperation;
import com.linkedin.venice.utils.AvroSchemaUtils;
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
      currRecordAndRmd.setValue(AvroSchemaUtils.createGenericRecord(currValueSchema));
    }

    Object timestampObject = currRecordAndRmd.getRmd().get(TIMESTAMP_FIELD_POS);
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
      Schema.Field currentValueField = currRecordAndRmd.getValue().getSchema().getField(writeComputeFieldName);
      if (currentValueField == null) {
        throw new IllegalStateException(
            "Current value record must have a schema that has the same field names as the "
                + "write compute schema because the current value's schema should be the schema that is used to generate "
                + "the write-compute schema. Got missing field: " + writeComputeFieldName);
      }

      Object writeComputeFieldValue = writeComputeRecord.get(writeComputeField.pos());
      WriteComputeOperation operationType = WriteComputeOperation.getFieldOperationType(writeComputeFieldValue);
      switch (operationType) {
        case NO_OP_ON_FIELD:
          continue; // Do nothing

        case PUT_NEW_FIELD:
          UpdateResultStatus putResult = mergeRecordHelper.putOnField(
              currRecordAndRmd.getValue(),
              timestampRecord,
              currentValueField,
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
              currentValueField);
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
      Schema.Field currentValueField) {
    switch (SchemaUtils.unwrapOptionalUnion(currentValueField.schema())) {
      case ARRAY:
        return collectionFieldOperationHandler.handleModifyList(
            modifyTimestamp,
            new CollectionRmdTimestamp(fieldTimestampRecord),
            currValueRecord,
            currentValueField,
            (List<Object>) fieldWriteComputeRecord.get(WriteComputeConstants.SET_UNION),
            (List<Object>) fieldWriteComputeRecord.get(WriteComputeConstants.SET_DIFF));
      case MAP:
        Object fieldValue = currValueRecord.get(currentValueField.pos());
        if (fieldValue != null && !(fieldValue instanceof IndexedHashMap)) {
          // if the current map field is not of IndexedHashMap type and is empty then replace this field with an empty
          // IndexedHashMap
          if (((Map) fieldValue).isEmpty()) {
            currValueRecord.put(currentValueField.pos(), new IndexedHashMap<>());
          } else {
            throw new IllegalStateException(
                "Expect value of field " + currentValueField.name() + " to be an IndexedHashMap. Got: "
                    + fieldValue.getClass());
          }
        }
        return collectionFieldOperationHandler.handleModifyMap(
            modifyTimestamp,
            new CollectionRmdTimestamp(fieldTimestampRecord),
            currValueRecord,
            currentValueField,
            (Map<String, Object>) fieldWriteComputeRecord.get(WriteComputeConstants.MAP_UNION),
            (List<String>) fieldWriteComputeRecord.get(WriteComputeConstants.MAP_DIFF));
      default:
        throw new IllegalArgumentException(
            String.format(
                "Expect value field %s to be either a List or a Map. Got value record: %s",
                currentValueField.name(),
                currValueRecord));
    }
  }

  private GenericRecord assertAndGetTsRecordFieldIsRecord(GenericRecord timestampRecord, String fieldName) {
    Object fieldTimestamp = timestampRecord.get(fieldName);
    if (!(fieldTimestamp instanceof GenericRecord)) {
      throw new IllegalStateException(
          String.format(
              "Expect field '%s' in the timestamp record to be a generic record. Got timestamp record: %s",
              fieldName,
              timestampRecord));
    }
    return (GenericRecord) fieldTimestamp;
  }
}
