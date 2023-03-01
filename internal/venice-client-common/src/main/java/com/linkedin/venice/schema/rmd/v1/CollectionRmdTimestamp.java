package com.linkedin.venice.schema.rmd.v1;

import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


/**
 * This class centralizes the logic of creating a collection replication metadata schema and providing a POJO representation
 * upon a collection replication metadata generic record. Its purpose is to abstract details of collection replication
 * metadata schema and its generic record away for users.
 */
@NotThreadSafe
public class CollectionRmdTimestamp<DELETED_ELEMENT_TYPE> {
  // Constants that are used to construct collection field's timestamp RECORD
  public static final String TOP_LEVEL_TS_FIELD_NAME = "topLevelFieldTimestamp";
  public static final String TOP_LEVEL_COLO_ID_FIELD_NAME = "topLevelColoID";
  public static final String PUT_ONLY_PART_LENGTH_FIELD_NAME = "putOnlyPartLength";
  public static final String ACTIVE_ELEM_TS_FIELD_NAME = "activeElementsTimestamps"; // Client readable elements that
                                                                                     // haven't been deleted yet.
  public static final String DELETED_ELEM_FIELD_NAME = "deletedElementsIdentities";
  public static final String DELETED_ELEM_TS_FIELD_NAME = "deletedElementsTimestamps";
  public static final Schema COLLECTION_TS_ARRAY_SCHEMA = Schema.createArray(Schema.create(LONG));

  private final GenericRecord collectionRmdRecord;
  private final Map<DELETED_ELEMENT_TYPE, ElementTimestampAndIdx> deletedElementInfo;

  // Copy constructor for testing purpose.
  public CollectionRmdTimestamp(CollectionRmdTimestamp other) {
    this(GenericData.get().deepCopy(other.collectionRmdRecord.getSchema(), other.collectionRmdRecord));
  }

  public CollectionRmdTimestamp(GenericRecord collectionRmdRecord) {
    validateCollectionReplicationMetadataRecord(collectionRmdRecord);
    this.collectionRmdRecord = collectionRmdRecord;
    this.deletedElementInfo = new HashMap<>();
    populateDeletedElementSet();
  }

  private void populateDeletedElementSet() {
    if (!deletedElementInfo.isEmpty()) {
      deletedElementInfo.clear();
    }
    final List<DELETED_ELEMENT_TYPE> deletedElements = getDeletedElements();
    final List<Long> deletedElementTimestamps = getDeletedElementTimestamps();

    for (int i = 0; i < deletedElements.size(); i++) {
      deletedElementInfo.put(deletedElements.get(i), new ElementTimestampAndIdx(deletedElementTimestamps.get(i), i));
    }
  }

  private static void validateCollectionReplicationMetadataRecord(GenericRecord collectionReplicationMetadata) {
    Validate.notNull(collectionReplicationMetadata);
    if (!(collectionReplicationMetadata.get(TOP_LEVEL_TS_FIELD_NAME) instanceof Long)) {
      throw new IllegalArgumentException(
          String.format(
              "Expect %s field to be Long type. Got record: %s",
              TOP_LEVEL_TS_FIELD_NAME,
              collectionReplicationMetadata));
    }
    if (!(collectionReplicationMetadata.get(PUT_ONLY_PART_LENGTH_FIELD_NAME) instanceof Integer)) {
      throw new IllegalArgumentException(
          String.format(
              "Expect %s field to be Integer type. Got record: %s",
              PUT_ONLY_PART_LENGTH_FIELD_NAME,
              collectionReplicationMetadata));
    }
    if (!(collectionReplicationMetadata.get(TOP_LEVEL_COLO_ID_FIELD_NAME) instanceof Integer)) {
      throw new IllegalArgumentException(
          String.format(
              "Expect %s field to be Integer type. Got record: %s",
              TOP_LEVEL_COLO_ID_FIELD_NAME,
              collectionReplicationMetadata));
    }
    if (!(collectionReplicationMetadata.get(ACTIVE_ELEM_TS_FIELD_NAME) instanceof List)) {
      throw new IllegalArgumentException(
          String.format(
              "Expect %s field to be List type. Got record: %s",
              ACTIVE_ELEM_TS_FIELD_NAME,
              collectionReplicationMetadata));
    }
    if (!(collectionReplicationMetadata.get(DELETED_ELEM_FIELD_NAME) instanceof List)) {
      throw new IllegalArgumentException(
          String.format(
              "Expect %s field to be List type. Got record: %s",
              DELETED_ELEM_FIELD_NAME,
              collectionReplicationMetadata));
    }
    if (!(collectionReplicationMetadata.get(DELETED_ELEM_TS_FIELD_NAME) instanceof List)) {
      throw new IllegalArgumentException(
          String.format(
              "Expect %s field to be List type. Got record: %s",
              DELETED_ELEM_TS_FIELD_NAME,
              collectionReplicationMetadata));
    }
    List<?> deletedElements = (List<?>) collectionReplicationMetadata.get(DELETED_ELEM_FIELD_NAME);
    List<Long> deletedElementTimestamps = (List<Long>) collectionReplicationMetadata.get(DELETED_ELEM_TS_FIELD_NAME);

    if (deletedElements.size() != deletedElementTimestamps.size()) {
      throw new IllegalArgumentException(
          "Delete element list and the deleted element timestamp list should have the same"
              + " size. Got collection replication metadata: " + collectionReplicationMetadata);
    }
  }

  public long getTopLevelFieldTimestamp() {
    return (long) collectionRmdRecord.get(TOP_LEVEL_TS_FIELD_NAME);
  }

  public int getTopLevelColoID() {
    return (int) collectionRmdRecord.get(TOP_LEVEL_COLO_ID_FIELD_NAME);
  }

  public int getPutOnlyPartLength() {
    return (int) collectionRmdRecord.get(PUT_ONLY_PART_LENGTH_FIELD_NAME);
  }

  public boolean isInPutOnlyState() {
    return getActiveElementTimestamps().isEmpty() && getDeletedElementTimestamps().isEmpty();
  }

  public List<Long> getActiveElementTimestamps() {
    return (List<Long>) collectionRmdRecord.get(ACTIVE_ELEM_TS_FIELD_NAME);
  }

  /**
   * @param minTimestamp Remove all active timestamps that are smaller or equal to {@code minTimestamp}.
   * @return number of active timestamps removed from the beginning (index 0).
   */
  public int removeActiveTimestampsLowerOrEqualTo(final long minTimestamp) {
    final int nextLargerNumberIndex = findIndexOfNextLargerNumber(getActiveElementTimestamps(), minTimestamp);
    if (nextLargerNumberIndex == 0) {
      // Nothing needs to be removed.
      return nextLargerNumberIndex;
    }

    // May have too much overhead if the list is GenericData.Array.
    if (nextLargerNumberIndex > 0) {
      int activeElementCount = getActiveElementTimestamps().size();
      if (activeElementCount == nextLargerNumberIndex) {
        collectionRmdRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.emptyList());
      } else {
        List<Long> subList = getActiveElementTimestamps().subList(nextLargerNumberIndex, activeElementCount);
        collectionRmdRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, subList);
      }
    }
    return nextLargerNumberIndex;
  }

  public void removeDeletionInfoWithTimestampsLowerOrEqualTo(final long minTimestamp) {
    final int nextLargerNumberIndex = findIndexOfNextLargerNumber(getDeletedElementTimestamps(), minTimestamp);
    if (nextLargerNumberIndex < 0) {
      return;
    }
    getDeletedElementTimestamps().subList(0, nextLargerNumberIndex).clear();
    getDeletedElements().subList(0, nextLargerNumberIndex).clear();
    populateDeletedElementSet();
  }

  // Visible for test.
  static int findIndexOfNextLargerNumber(List<Long> sortedList, long num) {
    int left = 0;
    int right = sortedList.size() - 1;

    while (left <= right) {
      int mid = (left + right) / 2;
      long midVal = sortedList.get(mid);
      if (midVal <= num) {
        left = mid + 1;

      } else { // midVal > num
        right = mid - 1;
      }
    }
    return left;
  }

  public List<Long> getDeletedElementTimestamps() {
    return (List<Long>) collectionRmdRecord.get(DELETED_ELEM_TS_FIELD_NAME);
  }

  public List<DELETED_ELEMENT_TYPE> getDeletedElements() {
    return (List<DELETED_ELEMENT_TYPE>) collectionRmdRecord.get(DELETED_ELEM_FIELD_NAME);
  }

  // Setters
  public void setTopLevelFieldTimestamp(long topLevelFieldTimestamp) {
    collectionRmdRecord.put(TOP_LEVEL_TS_FIELD_NAME, topLevelFieldTimestamp);
  }

  public void setTopLevelColoID(int topLevelColoID) {
    collectionRmdRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, topLevelColoID);
  }

  public void setPutOnlyPartLength(int putOnlyPartLength) {
    collectionRmdRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, putOnlyPartLength);
  }

  public void setActiveElementTimestamps(List<Long> collectionActiveTimestamps) {
    collectionRmdRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, collectionActiveTimestamps);
  }

  public void setDeletedElementsAndTimestamps(
      List<DELETED_ELEMENT_TYPE> deletedElements,
      List<Long> deletedTimestamps) {
    if (deletedElements.size() != deletedTimestamps.size()) {
      throw new IllegalArgumentException(
          "There must be the same number of deleted elements as deleted timestamps. " + "Got: " + deletedElements.size()
              + " deleted element(s) and deleted timestamps are: " + Arrays.toString(deletedElements.toArray()));
    }
    collectionRmdRecord.put(DELETED_ELEM_TS_FIELD_NAME, deletedTimestamps);
    collectionRmdRecord.put(DELETED_ELEM_FIELD_NAME, deletedElements);
    populateDeletedElementSet();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CollectionRmdTimestamp)) {
      return false;
    }
    CollectionRmdTimestamp<DELETED_ELEMENT_TYPE> that = (CollectionRmdTimestamp<DELETED_ELEMENT_TYPE>) o;
    Schema thatCollectionRmdSchema = that.collectionRmdRecord.getSchema();
    if (!thatCollectionRmdSchema.equals(collectionRmdRecord.getSchema())) {
      return false;
    }
    return GenericData.get()
        .compare(collectionRmdRecord, that.collectionRmdRecord, collectionRmdRecord.getSchema()) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(collectionRmdRecord);
  }

  @Override
  public String toString() {
    return collectionRmdRecord.toString();
  }

  /**
   * Create a RECORD that keeps track of collection field's update metadata. There are 4 sub-fields here.
   * {@link #TOP_LEVEL_TS_FIELD_NAME} The top level timestamp. This field is updated when the whole
   * collections is replaced/removed (via partial update)
   * {@link #ACTIVE_ELEM_TS_FIELD_NAME} A timestamp array that holds the timestamps for each
   * elements in the collection.
   * {@link #DELETED_ELEM_FIELD_NAME} A tombstone array that holds deleted elements. If this collection is
   * an ARRAY, the array will hold elements the same type as the original array. If this collection is a MAP, the
   * array will hold string elements (keys in the original map).
   * {@link #DELETED_ELEM_TS_FIELD_NAME} A timestamp array that holds the timestamps for each deleted element
   * in the tombstone array
   *
   * <pre>
   * Example value schema:
   * {
   *   "type" : "record",
   *   "name" : "testRecord",
   *   "namespace" : "avro.example",
   *   "fields" : [ {
   *     "name" : "intArray",
   *     "type" : {
   *       "type" : "array",
   *       "items" : "int"
   *     },
   *     "default" : [ ]
   *   } ]
   * }
   *
   * Corresponding TS schema
   * {
   *   "type" : "record",
   *   "name" : "ARRAY_CollectionTimestampMetadata_1",
   *   "doc" : "structure that maintains all of the necessary metadata to perform deterministic conflict resolution on collection fields.",
   *   "fields" : [ {
   *     "name" : "topLevelFieldTimestamp",
   *     "type" : "long",
   *     "doc" : "Timestamp of the last partial update attempting to set every element of this collection.",
   *     "default" : 0
   *   },{
   *     "name" : "topLevelColoID",
   *     "type" : "int",
   *     "doc" : "ID of the colo from which the last successfully applied partial update was sent.",
   *     "default" : -1
   *   },{
   *     "name" : "putOnlyPartLength",
   *     "type" : "int",
   *     "doc" : "Length of the put-only part of the collection which starts from index 0.",
   *     "default" : 0
   *   },
   *   {
   *     "name" : "activeElementsTimestamps",
   *     "type" : {
   *       "type" : "array",
   *       "items" : "long"
   *     },
   *     "doc" : "Timestamps of each active element in the user's collection. This is a parallel array with the user's collection.",
   *     "default" : [ ]
   *   }, {
   *     "name" : "deletedElementsIdentities",
   *     "type" : {
   *       "type" : "array",
   *       "items" : "int"
   *     },
   *     "doc" : "The tomestone array of deleted elements. This is a parallel array with deletedElementsTimestamps",
   *     "default" : [ ]
   *   }, {
   *     "name" : "deletedElementsTimestamps",
   *     "type" : {
   *       "type" : "array",
   *       "items" : "long"
   *     },
   *     "doc" : "Timestamps of each deleted element. This is a parallel array with deletedElementsIdentity.",
   *     "default" : [ ]
   *   }
   * }
   * </pre>
   */
  public static Schema createCollectionTimeStampSchema(String metadataRecordName, String namespace, Schema elemSchema) {
    Schema.Field topLevelTSField = AvroCompatibilityHelper.newField(null)
        .setName(TOP_LEVEL_TS_FIELD_NAME)
        .setSchema(RmdSchemaGeneratorV1.LONG_TYPE_TIMESTAMP_SCHEMA)
        .setDoc("Timestamp of the last partial update attempting to set every element of this collection.")
        .setDefault(0)
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    Schema.Field topLevelColoIdField = AvroCompatibilityHelper.newField(null)
        .setName(TOP_LEVEL_COLO_ID_FIELD_NAME)
        .setSchema(Schema.create(INT))
        .setDoc("ID of the colo from which the last successfully applied partial update was sent.")
        .setDefault(-1)
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    Schema.Field putOnlyPartLengthField = AvroCompatibilityHelper.newField(null)
        .setName(PUT_ONLY_PART_LENGTH_FIELD_NAME)
        .setSchema(Schema.create(INT))
        .setDoc("Length of the put-only part of the collection which starts from index 0.")
        .setDefault(0)
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    Schema.Field activeElemTSField = AvroCompatibilityHelper.newField(null)
        .setName(ACTIVE_ELEM_TS_FIELD_NAME)
        .setSchema(COLLECTION_TS_ARRAY_SCHEMA)
        .setDoc(
            "Timestamps of each active element in the user's collection. This is a parallel array with the user's collection.")
        .setDefault(Collections.emptyList())
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    Schema.Field deletedElemField = AvroCompatibilityHelper.newField(null)
        .setName(DELETED_ELEM_FIELD_NAME)
        .setSchema(Schema.createArray(elemSchema))
        .setDoc("The tombstone array of deleted elements. This is a parallel array with deletedElementsTimestamps")
        .setDefault(Collections.emptyList())
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    Schema.Field deletedElemTSField = AvroCompatibilityHelper.newField(null)
        .setName(DELETED_ELEM_TS_FIELD_NAME)
        .setSchema(COLLECTION_TS_ARRAY_SCHEMA)
        .setDoc("Timestamps of each deleted element. This is a parallel array with deletedElementsIdentity.")
        .setDefault(Collections.emptyList())
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    final Schema collectionTSSchema = Schema.createRecord(
        metadataRecordName,
        "structure that maintains all of the necessary metadata to perform deterministic conflict resolution on collection fields.",
        namespace,
        false);
    collectionTSSchema.setFields(
        Arrays.asList(
            topLevelTSField,
            topLevelColoIdField,
            putOnlyPartLengthField,
            activeElemTSField,
            deletedElemField,
            deletedElemTSField));
    return collectionTSSchema;
  }
}
