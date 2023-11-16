package com.linkedin.davinci.schema.merge;

import com.linkedin.davinci.schema.SchemaUtils;
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
    switch (SchemaUtils.unwrapOptionalUnion(oldRecordField.schema())) {
      case ARRAY:
        return collectionFieldOperationHandler.handlePutList(
            newPutTimestamp,
            putOperationColoID,
            (List<Object>) newFieldValue,
            getCollectionRmdTimestamp(oldTimestampRecord, oldRecordField),
            oldRecord,
            oldRecordField);
      case MAP:
        if ((newFieldValue != null) && (!(newFieldValue instanceof IndexedHashMap))) {
          throw new IllegalStateException(
              "Expect the value to put on the field to be an IndexedHashMap. Got: " + newFieldValue.getClass());
        }
        return collectionFieldOperationHandler.handlePutMap(
            newPutTimestamp,
            putOperationColoID,
            (IndexedHashMap<String, Object>) newFieldValue,
            getCollectionRmdTimestamp(oldTimestampRecord, oldRecordField),
            oldRecord,
            oldRecordField);
      default:
        return super.putOnField(
            oldRecord,
            oldTimestampRecord,
            oldRecordField,
            newFieldValue,
            newPutTimestamp,
            putOperationColoID);
    }
  }

  @Override
  protected UpdateResultStatus deleteRecordField(
      GenericRecord currentRecord,
      GenericRecord currentTimestampRecord,
      Schema.Field currentRecordField,
      final long deleteTimestamp,
      final int coloID) {
    switch (SchemaUtils.unwrapOptionalUnion(currentRecordField.schema())) {
      case MAP:
        return collectionFieldOperationHandler.handleDeleteMap(
            deleteTimestamp,
            coloID,
            getCollectionRmdTimestamp(currentTimestampRecord, currentRecordField),
            currentRecord,
            currentRecordField);
      case ARRAY:
        return collectionFieldOperationHandler.handleDeleteList(
            deleteTimestamp,
            coloID,
            getCollectionRmdTimestamp(currentTimestampRecord, currentRecordField),
            currentRecord,
            currentRecordField);
      default:
        return super.deleteRecordField(
            currentRecord,
            currentTimestampRecord,
            currentRecordField,
            deleteTimestamp,
            coloID);
    }
  }

  private CollectionRmdTimestamp getCollectionRmdTimestamp(
      GenericRecord timestampRecord,
      Schema.Field valueRecordField) {
    // Collection field must have collection timestamp at this point.
    Object collectionFieldTimestampObj = timestampRecord.get(valueRecordField.name());
    if (!(collectionFieldTimestampObj instanceof GenericRecord)) {
      throw new IllegalStateException(
          String.format(
              "Expect field %s to be a generic record from timestamp record %s",
              valueRecordField.name(),
              timestampRecord));
    }
    return new CollectionRmdTimestamp((GenericRecord) collectionFieldTimestampObj);
  }
}
