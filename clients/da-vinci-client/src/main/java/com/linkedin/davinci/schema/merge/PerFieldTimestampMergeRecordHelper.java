package com.linkedin.davinci.schema.merge;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


/**
 * This class handles merges with replication metadata with per-field timestamp. If the replication does not have per-field
 * timestamp metadata, exceptions will be thrown.
 */
abstract class PerFieldTimestampMergeRecordHelper implements MergeRecordHelper {
  @Override
  public UpdateResultStatus putOnField(
      GenericRecord oldRecord,
      GenericRecord oldTimestampRecord,
      Schema.Field oldRecordField,
      Object newFieldValue,
      final long newPutTimestamp,
      final int putOperationColoID) {
    final long oldTimestamp = validateAndGetPrimitiveTimestamp(oldTimestampRecord, oldRecordField.name());
    if (oldTimestamp > newPutTimestamp) {
      // Current field does not change.
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;

    } else if (oldTimestamp == newPutTimestamp) {
      Object oldFieldValue = oldRecord.get(oldRecordField.pos());
      newFieldValue = compareAndReturn(oldFieldValue, newFieldValue, oldRecordField.schema());
      final boolean newFieldCompletelyReplaceOldField = newFieldValue != oldFieldValue;
      if (newFieldCompletelyReplaceOldField) {
        oldRecord.put(oldRecordField.pos(), newFieldValue);
      }
      return newFieldCompletelyReplaceOldField
          ? UpdateResultStatus.COMPLETELY_UPDATED
          : UpdateResultStatus.NOT_UPDATED_AT_ALL;

    } else {
      // New field value wins.
      oldRecord.put(oldRecordField.pos(), newFieldValue);
      oldTimestampRecord.put(oldRecordField.name(), newPutTimestamp);
      return UpdateResultStatus.COMPLETELY_UPDATED;
    }
  }

  private Object compareAndReturn(Object object1, Object object2, Schema schema) {
    final int compareResult = AvroCollectionElementComparator.INSTANCE.compare(object1, object2, schema);
    if (compareResult == 0) {
      return object1;
    }
    return compareResult > 0 ? object1 : object2;
  }

  /**
   * Note that a whole record deletion could result in partial deletion (one or multiple fields get deleted). When a field is
   * deleted, its default value becomes the field value. There could be a situation where several partial deletion resulted
   * from several whole record deletion deletes all fields so that all fields have their default values. This is the current
   * behavior.
   *
   * One optimization is that when all fields are deleted, the whole record could be marked as deleted and return an
   * Optional.empty().
   */
  @Override
  public UpdateResultStatus deleteRecord(
      GenericRecord currRecord,
      GenericRecord currTimestampRecord,
      long deleteTimestamp,
      int coloID) {
    boolean allFieldsDeleted = true;
    boolean allFieldsDeleteIgnored = true;
    for (Schema.Field currField: currRecord.getSchema().getFields()) {
      final UpdateResultStatus fieldUpdateResult =
          deleteRecordField(currRecord, currTimestampRecord, currField, deleteTimestamp, coloID);
      allFieldsDeleted &= (fieldUpdateResult == UpdateResultStatus.COMPLETELY_UPDATED);
      allFieldsDeleteIgnored &= (fieldUpdateResult == UpdateResultStatus.NOT_UPDATED_AT_ALL);
    }

    if (allFieldsDeleted) {
      return UpdateResultStatus.COMPLETELY_UPDATED;
    }
    if (allFieldsDeleteIgnored) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    return UpdateResultStatus.PARTIALLY_UPDATED;
  }

  protected UpdateResultStatus deleteRecordField(
      GenericRecord currentRecord,
      GenericRecord currentTimestampRecord,
      Schema.Field currentRecordField,
      long deleteTimestamp,
      int coloID) {
    // Must have per-field timestamp with Long type
    final long currFieldTimestamp = validateAndGetPrimitiveTimestamp(currentTimestampRecord, currentRecordField.name());
    if (currFieldTimestamp <= deleteTimestamp) {
      // Delete current field.
      Object curFieldDefaultValue = GenericData.get()
          .deepCopy(currentRecordField.schema(), AvroCompatibilityHelper.getGenericDefaultValue(currentRecordField));
      currentRecord.put(currentRecordField.pos(), curFieldDefaultValue);
      if (currFieldTimestamp < deleteTimestamp) {
        currentTimestampRecord.put(currentRecordField.name(), deleteTimestamp);
      }
      return UpdateResultStatus.COMPLETELY_UPDATED;
    } else {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
  }

  private long validateAndGetPrimitiveTimestamp(GenericRecord timestampRecord, String fieldName) {
    final Object timestampObj = timestampRecord.get(fieldName);

    if (timestampObj == null) {
      throw new IllegalArgumentException(
          "Expect timestamp field " + fieldName + " to be non-null in timestamp record: " + timestampRecord);
    }

    if (!(timestampObj instanceof Long)) {
      throw new IllegalArgumentException(
          String.format(
              "Expect timestamp field %s to be a Long. But got timestamp record: %s",
              fieldName,
              timestampRecord));
    }
    return (long) timestampObj;
  }
}
