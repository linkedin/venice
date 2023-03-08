package com.linkedin.venice.schema.merge;

import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import com.linkedin.venice.schema.SchemaUtils;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


/**
 * This class should only be used in tests for write compute to generate timestamp replication metadata generic record
 * for a collection field. It also ensures that the generated RMD generic record has valid content.
 *
 */
public class CollectionTimestampBuilder {
  private static final int DEFAULT_CAPACITY = 10;

  private Schema collectionTimestampSchema;
  private Long topLevelFieldTimestamp;
  private int topLevelColoID;
  private int putOnlyPartLength;
  private List<Long> activeElementTimestamps;
  private List<Object> deletedElements;
  private List<Long> deletedTimestamps;

  public CollectionTimestampBuilder(Schema elementSchema) {
    this.collectionTimestampSchema = null;
    this.topLevelFieldTimestamp = 0L;
    this.activeElementTimestamps =
        new GenericData.Array<>(DEFAULT_CAPACITY, Schema.createArray(Schema.create(Schema.Type.LONG)));
    this.deletedElements = new GenericData.Array<>(DEFAULT_CAPACITY, Schema.createArray(elementSchema));
    this.deletedTimestamps =
        new GenericData.Array<>(DEFAULT_CAPACITY, Schema.createArray(Schema.create(Schema.Type.LONG)));
  }

  public void setCollectionTimestampSchema(Schema collectionTimestampSchema) {
    validateCollectionTsRecord(collectionTimestampSchema);
    this.collectionTimestampSchema = collectionTimestampSchema;
  }

  public void setTopLevelTimestamps(long topLevelFieldTimestamp) {
    if (topLevelFieldTimestamp < 0) {
      throw new IllegalArgumentException("topLevelFieldTimestamp cannot be negative. Got: " + topLevelFieldTimestamp);
    }
    this.topLevelFieldTimestamp = topLevelFieldTimestamp;
  }

  public void setTopLevelColoID(int coloID) {
    this.topLevelColoID = coloID;
  }

  public void setPutOnlyPartLength(int putOnlyPartLength) {
    if (putOnlyPartLength < 0) {
      throw new IllegalArgumentException("Expect put-only length to be non-negative. Got: " + putOnlyPartLength);
    }
    this.putOnlyPartLength = putOnlyPartLength;
  }

  public void setActiveElementsTimestamps(List<Long> activeElementsTimestamps) {
    long prevTs = Long.MIN_VALUE;
    for (long ts: activeElementsTimestamps) {
      if (ts < prevTs) {
        throw new IllegalArgumentException("Expect active element timestamps to be an increasing sequence.");
      }
      prevTs = ts;
    }

    this.activeElementTimestamps =
        new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.LONG)), activeElementsTimestamps);
  }

  public void setDeletedElements(Schema elementSchema, List<Object> deletedElements) {
    this.deletedElements = new GenericData.Array<>(Schema.createArray(elementSchema), deletedElements);
  }

  public void setDeletedElementTimestamps(List<Long> deletedElementTimestamps) {
    this.deletedTimestamps =
        new GenericData.Array<>(Schema.createArray(Schema.create(Schema.Type.LONG)), deletedElementTimestamps);
  }

  public GenericRecord build() {
    if (collectionTimestampSchema == null) {
      throw new IllegalStateException("Must set collectionTimestampSchema before calling build()");
    }

    if (deletedElements.size() != deletedTimestamps.size()) {
      throw new IllegalStateException(
          "Expect deleted elements and deleted timestamps to have the same number of elements.");
    }

    for (long ts: activeElementTimestamps) {
      if (ts < topLevelFieldTimestamp) {
        throw new IllegalStateException("Active element timestamp cannot be smaller than the top level timestamp.");
      }
    }

    for (long ts: deletedTimestamps) {
      if (ts < topLevelFieldTimestamp) {
        throw new IllegalStateException("Deleted element timestamp cannot be smaller than the top level timestamp.");
      }
    }

    GenericRecord itemFieldTimestampRecord = SchemaUtils.createGenericRecord(collectionTimestampSchema);
    itemFieldTimestampRecord.put(TOP_LEVEL_TS_FIELD_NAME, topLevelFieldTimestamp);
    itemFieldTimestampRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, topLevelColoID);
    itemFieldTimestampRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, putOnlyPartLength);
    itemFieldTimestampRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, activeElementTimestamps);
    itemFieldTimestampRecord.put(DELETED_ELEM_FIELD_NAME, deletedElements);
    itemFieldTimestampRecord.put(DELETED_ELEM_TS_FIELD_NAME, deletedTimestamps);
    return itemFieldTimestampRecord;
  }

  private void validateCollectionTsRecord(Schema collectionTimestampSchema) {
    Validate.notNull(collectionTimestampSchema);
    validateFieldExists(TOP_LEVEL_TS_FIELD_NAME, collectionTimestampSchema);
    validateFieldExists(TOP_LEVEL_COLO_ID_FIELD_NAME, collectionTimestampSchema);
    validateFieldExists(PUT_ONLY_PART_LENGTH_FIELD_NAME, collectionTimestampSchema);
    validateFieldExists(ACTIVE_ELEM_TS_FIELD_NAME, collectionTimestampSchema);
    validateFieldExists(DELETED_ELEM_TS_FIELD_NAME, collectionTimestampSchema);
    validateFieldExists(DELETED_ELEM_FIELD_NAME, collectionTimestampSchema);
  }

  private void validateFieldExists(String fieldName, Schema collectionTimestampSchema) {
    if (collectionTimestampSchema.getField(fieldName) == null) {
      throw new IllegalArgumentException(
          String.format("Expect schema to contain %s. Got schema: %s", fieldName, collectionTimestampSchema));
    }
  }
}
