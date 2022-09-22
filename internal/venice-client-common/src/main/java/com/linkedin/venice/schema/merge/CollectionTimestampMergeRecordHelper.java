package com.linkedin.venice.schema.merge;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.UNION;

import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.List;
import java.util.Map;
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
      String fieldName,
      Object newFieldValue,
      final long newPutTimestamp,
      final int putOperationColoID) {
    Object oldFieldValue = oldRecord.get(fieldName);
    if (oldFieldValue instanceof List || oldFieldValue instanceof Map) {
      // Collection field must have collection timestamp at this point.
      Object collectionFieldTimestampObj = oldTimestampRecord.get(fieldName);
      if (!(collectionFieldTimestampObj instanceof GenericRecord)) {
        throw new IllegalStateException(
            String.format(
                "Expect field %s to be a generic record from timestamp record %s",
                fieldName,
                oldTimestampRecord));
      }
      GenericRecord collectionFieldTimestamp = (GenericRecord) collectionFieldTimestampObj;
      return putOnFieldWithCollectionTimestamp(
          collectionFieldTimestamp,
          oldRecord,
          newFieldValue,
          fieldName,
          newPutTimestamp,
          putOperationColoID);

    } else {
      return super.putOnField(
          oldRecord,
          oldTimestampRecord,
          fieldName,
          newFieldValue,
          newPutTimestamp,
          putOperationColoID);
    }
  }

  /**
   * Put a new collection field value to a collection field with collection timestamp replication metadata.
   *
   * @param collectionFieldTimestampRecord Collection timestamp replication metadata.
   * @param currValueRecord Current record that contains the field.
   * @param putFieldValue New collection field value to be put.
   * @param fieldName Name of the collection field to be deleted.
   * @param putOperationTimestamp Timestamp of the PUT operation.
   *
   * @return
   */
  private UpdateResultStatus putOnFieldWithCollectionTimestamp(
      GenericRecord collectionFieldTimestampRecord,
      GenericRecord currValueRecord,
      Object putFieldValue,
      String fieldName,
      final long putOperationTimestamp,
      final int putOperationColoID) {
    final CollectionRmdTimestamp collectionRmd = new CollectionRmdTimestamp(collectionFieldTimestampRecord);
    Object currFieldValue = currValueRecord.get(fieldName);

    if (currFieldValue instanceof List<?>) {
      return collectionFieldOperationHandler.handlePutList(
          putOperationTimestamp,
          putOperationColoID,
          (List<Object>) putFieldValue,
          collectionRmd,
          currValueRecord,
          fieldName);

    } else if (currFieldValue instanceof Map) {
      if (!(putFieldValue instanceof IndexedHashMap)) {
        throw new IllegalStateException(
            "Expect the value to put on the field to be an IndexedHashMap. Got: " + putFieldValue.getClass());
      }
      return collectionFieldOperationHandler.handlePutMap(
          putOperationTimestamp,
          putOperationColoID,
          (IndexedHashMap<String, Object>) putFieldValue,
          collectionRmd,
          currValueRecord,
          fieldName);

    } else {
      throw new IllegalStateException(
          "Expect a field that is of a collection type (Map or List). Got: " + currFieldValue + " for field "
              + fieldName);
    }
  }

  @Override
  protected UpdateResultStatus deleteRecordField(
      GenericRecord currValueRecord,
      GenericRecord currTimestampRecord,
      final String fieldName,
      final long deleteTimestamp,
      final int coloID) {
    if (isCollectionField(currValueRecord, fieldName)) {
      Object timestamp = currTimestampRecord.get(fieldName);
      if (timestamp instanceof Long) {
        throw new IllegalStateException(
            String.format(
                "Expect collection timestamp record for field %s. But got timestamp: %d",
                fieldName,
                timestamp));
      }
      Object currFieldValue = currValueRecord.get(fieldName);

      if (currFieldValue instanceof List<?>) {
        return collectionFieldOperationHandler.handleDeleteList(
            deleteTimestamp,
            coloID,
            new CollectionRmdTimestamp((GenericRecord) timestamp),
            currValueRecord,
            fieldName);

      } else if (currFieldValue instanceof Map) {
        return collectionFieldOperationHandler.handleDeleteMap(
            deleteTimestamp,
            coloID,
            new CollectionRmdTimestamp((GenericRecord) timestamp),
            currValueRecord,
            fieldName);

      } else {
        throw new IllegalStateException(
            "Expect a field that is of a collection type (Map or List). Got: " + currFieldValue + " for field "
                + fieldName);
      }

    } else {
      return super.deleteRecordField(currValueRecord, currTimestampRecord, fieldName, deleteTimestamp, coloID);
    }
  }

  private boolean isCollectionField(GenericRecord currRecord, String fieldName) {
    Object fieldValue = currRecord.get(fieldName);
    if (fieldValue != null) {
      return fieldValue instanceof List || fieldValue instanceof Map;
    }

    Schema fieldSchema = currRecord.getSchema().getField(fieldName).schema();
    if (fieldSchema.getType() == ARRAY) {
      return true;
    }
    // Check if field is a nullable map.
    if (fieldSchema.getType() == UNION && fieldSchema.getTypes().get(1).getType() == MAP) {
      return true;
    }
    return false;
  }
}
