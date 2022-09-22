package com.linkedin.venice.schema.rmd.v1;

import static com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1.LONG_TYPE_TIMESTAMP_SCHEMA;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.STRING;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaUtils;
import io.tehuti.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;


/**
 * This class build replication metadata schema given a record schema of a value
 */
class RecordMetadataSchemaBuilder {
  public static final String COLLECTION_TS_RECORD_SUFFIX = "CollectionMetadata";
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
      throw new VeniceException(
          String.format(
              "Expect schema with type %s. Got: %s with name %s",
              Schema.Type.RECORD,
              valueRecordSchema.getType(),
              valueRecordSchema.getName()));
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
    Schema rmdSchema = Schema.createRecord(
        valueRecordSchema.getName(),
        valueRecordSchema.getDoc(),
        valueRecordSchema.getNamespace(),
        valueRecordSchema.isError());
    List<Schema.Field> newFields = new ArrayList<>(valueRecordSchema.getFields().size());
    for (Schema.Field existingField: valueRecordSchema.getFields()) {
      newFields.add(generateMetadataField(existingField, namespace));
    }
    rmdSchema.setFields(newFields);
    return rmdSchema;
  }

  private Schema.Field generateMetadataField(Schema.Field existingField, String namespace) {
    final Schema fieldMetadataSchema = generateMetadataSchemaForField(existingField, namespace);
    final Object defaultValue;
    if (fieldMetadataSchema == LONG_TYPE_TIMESTAMP_SCHEMA) {
      defaultValue = 0;
    } else if (fieldMetadataSchema.getType() == RECORD) {
      defaultValue = SchemaUtils.createGenericRecord(fieldMetadataSchema);
    } else {
      throw new IllegalStateException(
          "Generated field metadata schema is expected to be either of a type Long or of a "
              + "type Record. But got schema: " + fieldMetadataSchema);
    }

    return AvroCompatibilityHelper.newField(null)
        .setName(existingField.name())
        .setSchema(fieldMetadataSchema)
        .setDoc("timestamp when " + existingField.name() + " of the record was last updated")
        .setDefault(defaultValue)
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
        mdSchemaForField = LONG_TYPE_TIMESTAMP_SCHEMA;
    }
    return mdSchemaForField;
  }

  private Schema generateSchemaForMapField(Schema.Field mapField, String namespace) {
    return normalizedSchemaToMetadataSchemaMap.computeIfAbsent(
        SchemaNormalization.toParsingForm(mapField.schema()),
        k -> CollectionRmdTimestamp.createCollectionTimeStampSchema(
            constructCollectionMetadataFieldSchemaName(mapField),
            namespace,
            Schema.create(STRING) // Map key type is always STRING in Avro
        ));
  }

  private Schema generateSchemaForArrayField(Schema.Field arrayField, String namespace) {
    return normalizedSchemaToMetadataSchemaMap.computeIfAbsent(
        SchemaNormalization.toParsingForm(arrayField.schema()),
        k -> CollectionRmdTimestamp.createCollectionTimeStampSchema(
            constructCollectionMetadataFieldSchemaName(arrayField),
            namespace,
            arrayField.schema().getElementType()));
  }

  private Schema generateSchemaForUnionField(Schema.Field unionField, String namespace) {
    if (!SchemaUtils.isNullableUnionPair(unionField.schema())) {
      return LONG_TYPE_TIMESTAMP_SCHEMA;
    }
    List<Schema> internalSchemas = unionField.schema().getTypes();
    Schema actualSchema = internalSchemas.get(0).getType() == NULL ? internalSchemas.get(1) : internalSchemas.get(0);

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

  private String constructCollectionMetadataFieldSchemaName(Schema.Field originalField) {
    final Schema.Type fieldSchemaType = originalField.schema().getType();
    if (fieldSchemaType != ARRAY && fieldSchemaType != MAP) {
      throw new VeniceException("Not support type: " + fieldSchemaType);
    }
    return String
        .format("%s_%s_%d", fieldSchemaType.getName(), COLLECTION_TS_RECORD_SUFFIX, collectionFieldSchemaNameSuffix++);
  }
}
