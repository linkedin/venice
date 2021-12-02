package com.linkedin.venice.schema.rmd.v1;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter.*;
import static org.apache.avro.Schema.Type.*;


/**
 * This class helps in generating a active-active metadata schema to hold update timestamps and offsets from a value schema of a store .
 *
 * If the value schema is a RECORD then the generated schema will be a RECORD having a UNION field and an array. The UNION will consists
 *   of 1 record to hold a single timestamp field for the entire record. This timestamp indicates when the full record was last update as a whole.
 *   The other field of the UNION will be a RECORD consisting of 1 timestamp field for each field of the original record.
 *   These timestamps will indicate when a particular field was updated through Partial update operations.
 *
 *   The array field contains the offsets of messages in remote colos which tried to update this record.
 *
 * If the value schema is not RECORD then the generated schema will be a RECORD to hold a single timestamp field, and the same offset array.
 *   This timestamp indicates when the full record was last update as a whole.
 *
 * Currently nested fields are not supported.
 */

public class ReplicationMetadataSchemaGeneratorV1 {
  protected static final String METADATA_RECORD_SUFFIX = "MetadataRecord";
  protected static final String NAME_SPACE = "com.linkedin.venice";
  public static final Schema TIMESTAMP_SCHEMA = Schema.create(LONG);
  protected static final Schema OFFSET_VECTOR_SCHEMA = Schema.createArray(Schema.create(LONG));

  public ReplicationMetadataSchemaGeneratorV1() {}

  public Schema generateMetadataSchema(String valueSchemaStr) {
    return generateMetadataSchema(Schema.parse(valueSchemaStr));
  }

  public Schema generateMetadataSchema(Schema originalSchema) {
    String nameSpace = (originalSchema.getType() == RECORD ? originalSchema.getNamespace() : NAME_SPACE);
    return generateMetadataSchema(originalSchema, nameSpace);
  }

  protected Schema generateMetadataSchemaFromRecord(Schema recordSchema, String namespace) {
    validateSchemaType(recordSchema, RECORD);
    Schema newSchema = Schema.createRecord(recordSchema.getName(), recordSchema.getDoc(), recordSchema.getNamespace(),
        recordSchema.isError());
    List<Schema.Field> newFields = new ArrayList<>(recordSchema.getFields().size());

    // Create an equivalent timestamp field for each record field.
    for (Schema.Field existingField : recordSchema.getFields()) {
      Schema.Field newField = AvroCompatibilityHelper.newField(null)
          .setName(existingField.name())
          .setSchema(TIMESTAMP_SCHEMA)
          .setDoc("timestamp when " + existingField.name()  + " of the record was last updated")
          .setDefault(0)
          .setOrder(existingField.order())
          .build();
      newFields.add(newField);
    }
    newSchema.setFields(newFields);
    return newSchema;
  }

  protected void validateSchemaType(Schema schema, Schema.Type expectedType) {
    if (schema.getType() != expectedType) {
      throw new VeniceException(String.format("Expect schema with type %s. Got: %s with name %s", expectedType, schema.getType(), schema.getName()));
    }
  }

  /**
   * This function creates a record having fields called "timestamp" and "replication_checkpoint_vector".
   * The timestamp field is a union of a "long" and a optional recordFieldsSchema.
   *
   * replication_checkpoint_vector is an array with the high watermark of events which tried to update this
   * record (which may or may not actually altered the state).
   *
   * @param originalSchema The original value schema.
   * @param namespace namespace
   */
  Schema generateMetadataSchema(Schema originalSchema, String namespace) {
    List<Schema> rawTimestampSchemas = new ArrayList<>();

    //Root level timestamp field to indicate when was the full record updated.
    rawTimestampSchemas.add(TIMESTAMP_SCHEMA);
    // for RECORD, generate timestamp for each field
    if (originalSchema.getType() == RECORD) {
      rawTimestampSchemas.add(generateMetadataSchemaFromRecord(originalSchema, namespace));
    }
    Schema tsUnionSchema = createFlattenedUnion(rawTimestampSchemas);

    Schema.Field timeStampField = AvroCompatibilityHelper.newField(null)
        .setName(TIMESTAMP_FIELD_NAME)
        .setSchema(tsUnionSchema)
        .setDoc("timestamp when the full record was last updated")
        .setDefault(0)
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    // Offset vector is only stored at the record level (NOT the field level)
    Schema.Field offsetVectorField = AvroCompatibilityHelper.newField(null)
        .setName(REPLICATION_CHECKPOINT_VECTOR_FIELD)
        .setSchema(OFFSET_VECTOR_SCHEMA)
        .setDoc("high watermark remote checkpoints which touched this record")
        .setDefault(new ArrayList<>())
        .setOrder(Schema.Field.Order.ASCENDING)
        .build();

    final Schema metadataRecord =
        Schema.createRecord(originalSchema.getName() + "_" + METADATA_RECORD_SUFFIX, null, namespace, false);
    metadataRecord.setFields(Arrays.asList(timeStampField, offsetVectorField));
    return metadataRecord;
  }
}
