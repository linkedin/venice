package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import io.tehuti.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import static org.apache.avro.Schema.Type.*;


/**
 * Replication metadata schema adapter with nested collection support. Create finer metadata schema
 * when collections are encountered. Right now, we have 3 layers TS metadata.
 * 1. TS for the whole record.
 * 2. TS for each field.
 * 3. TS for each elements in the collections.
 *
 * {@link RecordMetadataSchemaBuilder#createCollectionTimeStampSchema} for more implementation details
 */

public class ReplicationMetadataSchemaGeneratorV2 extends ReplicationMetadataSchemaGeneratorV1 {

  public ReplicationMetadataSchemaGeneratorV2() {}

  @Override
  protected Schema generateMetadataSchemaFromRecord(Schema recordSchema, String namespace) {
    final RecordMetadataSchemaBuilder recordMetadataSchemaBuilder = new RecordMetadataSchemaBuilder();
    recordMetadataSchemaBuilder.setValueRecordSchema(recordSchema);
    recordMetadataSchemaBuilder.setNamespace(namespace);
    return recordMetadataSchemaBuilder.build();
  }

  /**
   * This class build replication metadata schema given a record schema of a value
   */
  private static class RecordMetadataSchemaBuilder {
    // Constants that are used to construct collection field's timestamp RECORD
    private static final String COLLECTION_TS_RECORD_SUFFIX = "CollectionMetadata";
    private static final String COLLECTION_TOP_LEVEL_TS_FIELD_NAME = "topLevelTimestamp";
    private static final String COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME = "activeElementsTimestamps";
    private static final String COLLECTION_DELETED_ELEM_FIELD_NAME = "deletedElementsIdentities";
    private static final String COLLECTION_DELETED_ELEM_TS_FIELD_NAME = "deletedElementsTimestamps";
    private static final Schema COLLECTION_TS_ARRAY_SCHEMA = Schema.createArray(Schema.create(LONG));

    private Schema valueRecordSchema;
    private String namespace;
    private Map<String, Schema> normalizedSchemaToMetadataSchemaMap;
    private int collectionFieldSchemaNameSuffix; // An increasing sequence number

    RecordMetadataSchemaBuilder() {
      this.valueRecordSchema = null;
      this.normalizedSchemaToMetadataSchemaMap = new HashMap<>();
      this.collectionFieldSchemaNameSuffix = 0;
    }

    void setValueRecordSchema(Schema valueRecordSchema) {
      if (Utils.notNull(valueRecordSchema).getType() != Schema.Type.RECORD) {
        throw new VeniceException(String.format("Expect schema with type %s. Got: %s with name %s",
            Schema.Type.RECORD, valueRecordSchema.getType(), valueRecordSchema.getName()));
      }
      this.valueRecordSchema = valueRecordSchema;
    }

    void setNamespace(String namespace) {
      this.namespace = namespace;
    }

    Schema build() {
      if (valueRecordSchema == null) {
        throw new VeniceException("Value record schema has not been set yet");
      }
      Schema rmdSchema = Schema.createRecord(valueRecordSchema.getName(), valueRecordSchema.getDoc(), valueRecordSchema.getNamespace(),
          valueRecordSchema.isError());
      List<Schema.Field> newFields = new ArrayList<>(valueRecordSchema.getFields().size());
      for (Schema.Field existingField : valueRecordSchema.getFields()) {
        newFields.add(generateMetadataField(existingField, namespace));
      }
      rmdSchema.setFields(newFields);
      return rmdSchema;
    }

    private Schema.Field generateMetadataField(Schema.Field existingField, String namespace) {
      Schema fieldSchema = generateMetadataSchemaForField(existingField, namespace);
      return AvroCompatibilityHelper.newField(null)
          .setName(existingField.name())
          .setSchema(fieldSchema)
          .setDoc("timestamp when " + existingField.name()  + " of the record was last updated")
          .setDefault(fieldSchema == TIMESTAMP_SCHEMA ? 0 : null)
          .setOrder(existingField.order())
          .build();
    }

    private Schema generateMetadataSchemaForField(Schema.Field existingField, String namespace) {
      final Schema mdSchemaForField;
      switch (existingField.schema().getType()) {
        case MAP:
          mdSchemaForField = generateSchemaForMapField(existingField, namespace);
          break;
        case ARRAY:
          mdSchemaForField = generateSchemaForArrayField(existingField, namespace);
          break;
        case UNION:
          mdSchemaForField = generateSchemaForUnionField(existingField, namespace);
          break;
        default:
          mdSchemaForField = TIMESTAMP_SCHEMA;
      }
      return mdSchemaForField;
    }

    private Schema generateSchemaForMapField(Schema.Field mapField, String namespace) {
      return normalizedSchemaToMetadataSchemaMap.computeIfAbsent(SchemaNormalization.toParsingForm(mapField.schema()),
          k -> createCollectionTimeStampSchema(
                  constructCollectionMetadataFieldSchemaName(mapField),
                  namespace,
                  Schema.create(STRING) // Map key type is always STRING in Avro
              ));
    }

    private Schema generateSchemaForArrayField(Schema.Field arrayField, String namespace) {
      return normalizedSchemaToMetadataSchemaMap.computeIfAbsent(SchemaNormalization.toParsingForm(arrayField.schema()),
          k -> createCollectionTimeStampSchema(
                  constructCollectionMetadataFieldSchemaName(arrayField),
                  namespace,
                  arrayField.schema().getElementType()
              ));
    }

    private Schema generateSchemaForUnionField(Schema.Field unionField, String namespace) {
      if (!DerivedSchemaUtils.isNullableUnionPair(unionField.schema())) {
        return TIMESTAMP_SCHEMA;
      }
      List<Schema> internalSchemas = unionField.schema().getTypes();
      Schema actualSchema = internalSchemas.get(0).getType() == NULL ?
          internalSchemas.get(1) : internalSchemas.get(0);

      Schema.Field fieldWithoutNull = AvroCompatibilityHelper.newField(null)
          .setName(unionField.name())
          .setSchema(actualSchema)
          .setDoc(unionField.doc())
          .setDefault(null)
          .setOrder(unionField.order())
          .build();

      // No need to worry about recursive call since Avro doesn't support nested union
      return generateMetadataSchemaForField(fieldWithoutNull, namespace);
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
    private Schema createCollectionTimeStampSchema(String metadataRecordName, String namespace, Schema elemSchema) {
      Schema.Field topLevelTSField = AvroCompatibilityHelper.newField(null)
          .setName(COLLECTION_TOP_LEVEL_TS_FIELD_NAME)
          .setSchema(TIMESTAMP_SCHEMA)
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
      collectionTSSchema.setFields(Arrays.asList(topLevelTSField, activeElemTSField, deletedElemField, deletedElemTSField));
      return collectionTSSchema;
    }

    private String constructCollectionMetadataFieldSchemaName(Schema.Field originalField) {
      final Schema.Type fieldSchemaType = originalField.schema().getType();
      if (fieldSchemaType != ARRAY && fieldSchemaType != MAP) {
        throw new VeniceException("Not support type: " + fieldSchemaType);
      }
      return String.format("%s_%s_%d", fieldSchemaType.getName(), COLLECTION_TS_RECORD_SUFFIX, collectionFieldSchemaNameSuffix++);
    }
  }
}
