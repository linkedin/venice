package com.linkedin.davinci.schema.merge;

import static com.linkedin.davinci.schema.SchemaUtils.isArrayField;
import static com.linkedin.davinci.schema.SchemaUtils.isMapField;

import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class handles merges with replication metadata with collection metadata timestamp {@link CollectionRmdTimestamp}
 * for collection field. If the replication does not have collection metadata timestamp for collection field, exceptions
 * will be thrown.
 *
 * If a record does not have any collection field, this class behavior should be identical to {@link PerFieldTimestampMergeRecordHelper}
 */
public class CollectionTimestampMergeRecordHelper extends PerFieldTimestampMergeRecordHelper {
  private final CollectionFieldOperationHandler collectionFieldOperationHandler;

  public CollectionTimestampMergeRecordHelper() {
    // TODO: get this variable as a argument passed to this constructor.
    this.collectionFieldOperationHandler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);
  }

  @Override
  public UpdateResultStatus putOnField(
      GenericRecord oldRecord,
      GenericRecord oldTimestampRecord,
      Schema.Field oldRecordField,
      Object newFieldValue,
      final long newPutTimestamp,
      final int putOperationColoID) {
    if (isMapField(oldRecordField.schema()) || isArrayField(oldRecordField.schema())) {
      // Collection field must have collection timestamp at this point.
      Object collectionFieldTimestampObj = oldTimestampRecord.get(oldRecordField.name());
      if (!(collectionFieldTimestampObj instanceof GenericRecord)) {
        throw new IllegalStateException(
            String.format(
                "Expect field %s to be a generic record from timestamp record %s",
                oldRecordField.name(),
                oldTimestampRecord));
      }
      GenericRecord collectionFieldTimestamp = (GenericRecord) collectionFieldTimestampObj;
      return putOnFieldWithCollectionTimestamp(
          collectionFieldTimestamp,
          oldRecord,
          newFieldValue,
          oldRecordField,
          newPutTimestamp,
          putOperationColoID);
    }
    return super.putOnField(
        oldRecord,
        oldTimestampRecord,
        oldRecordField,
        newFieldValue,
        newPutTimestamp,
        putOperationColoID);
  }

  /**
   * Put a new collection field value to a collection field with collection timestamp replication metadata.
   *
   * @param collectionFieldTimestampRecord Collection timestamp replication metadata.
   * @param currValueRecord Current record that contains the field.
   * @param putFieldValue New collection field value to be put.
   * @param currValueRecordField collection field to be put.
   * @param putOperationTimestamp Timestamp of the PUT operation.
   *
   * @return
   */
  private UpdateResultStatus putOnFieldWithCollectionTimestamp(
      GenericRecord collectionFieldTimestampRecord,
      GenericRecord currValueRecord,
      Object putFieldValue,
      Schema.Field currValueRecordField,
      final long putOperationTimestamp,
      final int putOperationColoID) {
    final CollectionRmdTimestamp collectionRmd = new CollectionRmdTimestamp(collectionFieldTimestampRecord);
    if (isArrayField(currValueRecordField.schema())) {
      return collectionFieldOperationHandler.handlePutList(
          putOperationTimestamp,
          putOperationColoID,
          (List<Object>) putFieldValue,
          collectionRmd,
          currValueRecord,
          currValueRecordField);
    } else if (isMapField(currValueRecordField.schema())) {
      if ((putFieldValue != null) && (!(putFieldValue instanceof IndexedHashMap))) {
        throw new IllegalStateException(
            "Expect the value to put on the field to be an IndexedHashMap. Got: " + putFieldValue.getClass());
      }
      return collectionFieldOperationHandler.handlePutMap(
          putOperationTimestamp,
          putOperationColoID,
          (IndexedHashMap<String, Object>) putFieldValue,
          collectionRmd,
          currValueRecord,
          currValueRecordField);
    }
    throw new IllegalStateException(
        "Expect a field that is of a collection type (Map or List). Got: "
            + currValueRecord.get(currValueRecordField.pos()) + " for field " + currValueRecordField.name());
  }

  @Override
  protected UpdateResultStatus deleteRecordField(
      GenericRecord currentRecord,
      GenericRecord currentTimestampRecord,
      Schema.Field currentRecordField,
      final long deleteTimestamp,
      final int coloID) {
    boolean isMapField = isMapField(currentRecordField.schema());
    boolean isArrayField = isArrayField(currentRecordField.schema());
    if (isMapField || isArrayField) {
      Object timestamp = currentTimestampRecord.get(currentRecordField.name());
      if (timestamp instanceof Long) {
        throw new IllegalStateException(
            String.format(
                "Expect collection timestamp record for field %s. But got timestamp: %d",
                currentRecordField.name(),
                timestamp));
      }
      if (isMapField) {
        return collectionFieldOperationHandler.handleDeleteMap(
            deleteTimestamp,
            coloID,
            new CollectionRmdTimestamp((GenericRecord) timestamp),
            currentRecord,
            currentRecordField);

      } else {
        return collectionFieldOperationHandler.handleDeleteList(
            deleteTimestamp,
            coloID,
            new CollectionRmdTimestamp((GenericRecord) timestamp),
            currentRecord,
            currentRecordField);
      }
    } else {
      return super.deleteRecordField(
          currentRecord,
          currentTimestampRecord,
          currentRecordField,
          deleteTimestamp,
          coloID);
    }
  }
}
