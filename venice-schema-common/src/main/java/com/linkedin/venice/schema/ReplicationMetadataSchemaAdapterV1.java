package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.schema.WriteComputeSchemaAdapter.*;
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

public class ReplicationMetadataSchemaAdapterV1 {
  private static final String METADATA_RECORD_SUFFIX = "MetadataRecord";
  private static final Schema TIMESTAMP_SCHEMA = Schema.create(LONG);
  private static final Schema OFFSET_VECTOR_SCHEMA = Schema.createArray(Schema.create(LONG));

  public ReplicationMetadataSchemaAdapterV1() {}

  public Schema parse(String schemaStr) {
    return parse(Schema.parse(schemaStr));
  }

  public Schema parse(Schema schema) {
    String origSchemaName = schema.getName();
    String nameSpace = "com.linkedin.venice";
    if (schema.getType() == RECORD) {
      nameSpace = schema.getNamespace();
    }
    ReplicationMetadataSchemaAdapterV1 adapter = new ReplicationMetadataSchemaAdapterV1();
    return adapter.createMetadataRecord(origSchemaName, nameSpace, adapter.parse(schema,null));
  }

  /**
   * Currently it supports timestamps/offsets for only root level field of a record. Nested fields are not supported.
   * @param originSchema
   * @param namespace
   * @return
   */
  private Schema parse(Schema originSchema, String namespace) {
    ReplicationMetadataSchemaAdapterV1 adapter = new ReplicationMetadataSchemaAdapterV1();
    switch (originSchema.getType()) {
      case RECORD:
        return adapter.parseRecord(originSchema);
      default:
        return null;
    }
  }

  private Schema parseRecord(Schema recordSchema) {
    Schema newSchema = Schema.createRecord(recordSchema.getName(), recordSchema.getDoc(), recordSchema.getNamespace(),
        recordSchema.isError());
    List<Schema.Field> fieldList = new ArrayList<>();
    //Create an equivalent timestamp field for each record field.
    for (Schema.Field field : recordSchema.getFields()) {
      fieldList.add(AvroCompatibilityHelper.createSchemaField(field.name(), TIMESTAMP_SCHEMA, "timestamp when " + field.name()  + " of the record was last updated", 0,
          field.order()));
    }
    newSchema.setFields(fieldList);
    return newSchema;
  }

  /**
   * This function creates a record having fields called "timestamp" and "replication_checkpoint_vector". The timestamp field is a union of a "long" and a optional
   * recordFieldsSchema.  replication_checkpoint_vector is an array with the high watermark of events which tried to update this vector (which may or may not actually
   * altered the state).
   *
   * @param origSchemaName name of the original value schema.
   * @param nameSpace namespace
   * @param recordFieldsSchema a generated record schema containing a timestamp for each field of the original record schema. This should
   *                           be non-null only if the original value schema is a record schema type.
   * @return
   */
  private Schema createMetadataRecord(String origSchemaName, String nameSpace, Schema recordFieldsSchema) {
    LinkedList<Schema> list = new LinkedList<>();

    //Root level timestamp field to indicate when was the full record updated.
    list.add(TIMESTAMP_SCHEMA);
    if (recordFieldsSchema != null) {
      list.add(recordFieldsSchema);
    }
    Schema tsUnionSchema = createFlattenedUnion(list);

    Schema metadataRecord = Schema.createRecord(origSchemaName + "_" + METADATA_RECORD_SUFFIX, null, nameSpace, false);
    Schema.Field timeStampField = AvroCompatibilityHelper.createSchemaField(TIMESTAMP_FIELD, tsUnionSchema, "timestamp when the full record was last updated", 0);

    // Offset vector is only stored at the record level (NOT the field level)
    Schema.Field offsetVectorField = AvroCompatibilityHelper.createSchemaField(REPLICATION_CHECKPOINT_VECTOR_FIELD, OFFSET_VECTOR_SCHEMA, "high watermark remote checkpoints which touched this record",
        Collections.emptyList());
    metadataRecord.setFields(Arrays.asList(timeStampField, offsetVectorField));

    return metadataRecord;

  }

}
