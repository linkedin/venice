package com.linkedin.venice.schema.rmd.v2;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.schema.rmd.v1.ReplicationMetadataSchemaGeneratorV1;
import io.tehuti.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import static org.apache.avro.Schema.Type.*;


/**
 * This class centralizes the logic of creating a collection replication metadata schema and providing a POJO representation
 * upon a collection replication metadata generic record. Its purpose is to abstract details of collection replication
 * metadata schema and its generic record away for users.
 */
public class CollectionReplicationMetadata {
  // Constants that are used to construct collection field's timestamp RECORD
  public static final String COLLECTION_TOP_LEVEL_TS_FIELD_NAME = "topLevelTimestamp";
  public static final String COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME = "activeElementsTimestamps";
  public static final String COLLECTION_DELETED_ELEM_FIELD_NAME = "deletedElementsIdentities";
  public static final String COLLECTION_DELETED_ELEM_TS_FIELD_NAME = "deletedElementsTimestamps";
  public static final Schema COLLECTION_TS_ARRAY_SCHEMA = Schema.createArray(Schema.create(LONG));

  private final GenericRecord collectionReplicationMetadata;

  public CollectionReplicationMetadata(GenericRecord collectionReplicationMetadata) {
    validateCollectionReplicationMetadataRecord(collectionReplicationMetadata);
    this.collectionReplicationMetadata = collectionReplicationMetadata;
  }

  private void validateCollectionReplicationMetadataRecord(GenericRecord collectionReplicationMetadata) {
    Utils.notNull(collectionReplicationMetadata);
    if (!(collectionReplicationMetadata.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME) instanceof Long)) {
      throw new IllegalArgumentException(String.format("Expect %s field to be Long type. Got record: %s", COLLECTION_TOP_LEVEL_TS_FIELD_NAME, collectionReplicationMetadata));
    }
    if (!(collectionReplicationMetadata.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME) instanceof long[])) {
      throw new IllegalArgumentException(String.format("Expect %s field to be long[] type. Got record: %s", COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME, collectionReplicationMetadata));
    }
    if (!(collectionReplicationMetadata.get(COLLECTION_DELETED_ELEM_FIELD_NAME) instanceof Object[])) {
      throw new IllegalArgumentException(String.format("Expect %s field to be Object[] type. Got record: %s", COLLECTION_DELETED_ELEM_FIELD_NAME, collectionReplicationMetadata));
    }
    if (!(collectionReplicationMetadata.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME) instanceof long[])) {
      throw new IllegalArgumentException(String.format("Expect %s field to be long[] type. Got record: %s", COLLECTION_DELETED_ELEM_TS_FIELD_NAME, collectionReplicationMetadata));
    }
  }

  public long getTopLevelTimestamp() {
    return (long) collectionReplicationMetadata.get(COLLECTION_TOP_LEVEL_TS_FIELD_NAME);
  }

  public long[] getActiveElementTimestamps() {
    return (long[]) collectionReplicationMetadata.get(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME);
  }

  public long[] getDeletedElementTimestamps() {
    return (long[]) collectionReplicationMetadata.get(COLLECTION_DELETED_ELEM_TS_FIELD_NAME);
  }

  public Object[] getDeletedElements() {
    return (Object[]) collectionReplicationMetadata.get(COLLECTION_DELETED_ELEM_FIELD_NAME);
  }

  /**
   * Create a RECORD that keeps track of collection field's update metadata. There are 4 sub-fields here.
   * {@link #COLLECTION_TOP_LEVEL_TS_FIELD_NAME} The top level timestamp. This field is updated when the whole
   * collections is replaced/removed (via partial update)
   * {@link #COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME} A timestamp array that holds the timestamps for each
   * elements in the collection.
   * {@link #COLLECTION_DELETED_ELEM_FIELD_NAME} A tombstone array that holds deleted elements. If this collection is
   * an ARRAY, the array will hold elements the same type as the original array. If this collection is a MAP, the
   * array will hold string elements (keys in the original map).
   * {@link #COLLECTION_DELETED_ELEM_TS_FIELD_NAME} A timestamp array that holds the timestamps for each deleted element
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
   *     "name" : "topLevelTimestamp",
   *     "type" : "long",
   *     "doc" : "Timestamp of the last partial update attempting to set every element of this collection.",
   *     "default" : 0
   *   }, {
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
        .setName(COLLECTION_TOP_LEVEL_TS_FIELD_NAME)
        .setSchema(ReplicationMetadataSchemaGeneratorV1.TIMESTAMP_SCHEMA)
        .setDoc("Timestamp of the last partial update attempting to set every element of this collection.")
        .setDefault(0)
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    Schema.Field activeElemTSField = AvroCompatibilityHelper.newField(null)
        .setName(COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME)
        .setSchema(COLLECTION_TS_ARRAY_SCHEMA)
        .setDoc("Timestamps of each active element in the user's collection. This is a parallel array with the user's collection.")
        .setDefault(Collections.emptyList())
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    Schema.Field deletedElemField = AvroCompatibilityHelper.newField(null)
        .setName(COLLECTION_DELETED_ELEM_FIELD_NAME)
        .setSchema(Schema.createArray(elemSchema))
        .setDoc("The tombstone array of deleted elements. This is a parallel array with deletedElementsTimestamps")
        .setDefault(Collections.emptyList())
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    Schema.Field deletedElemTSField = AvroCompatibilityHelper.newField(null)
        .setName(COLLECTION_DELETED_ELEM_TS_FIELD_NAME)
        .setSchema(COLLECTION_TS_ARRAY_SCHEMA)
        .setDoc("Timestamps of each deleted element. This is a parallel array with deletedElementsIdentity.")
        .setDefault(Collections.emptyList())
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    final Schema collectionTSSchema = Schema.createRecord(
        metadataRecordName,
        "structure that maintains all of the necessary metadata to perform deterministic conflict resolution on collection fields.",
        namespace,
        false
    );
    collectionTSSchema.setFields(
        Arrays.asList(topLevelTSField, activeElemTSField, deletedElemField, deletedElemTSField));
    return collectionTSSchema;
  }
}