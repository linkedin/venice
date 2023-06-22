package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.replication.merge.helper.utils.ValueAndDerivedSchemas;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaUtils;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.annotations.BeforeClass;


public class TestMergeConflictResolver {
  protected static final int RMD_VERSION_ID = 1;

  protected String storeName;
  protected ReadOnlySchemaRepository schemaRepository;
  protected Schema userSchemaV1;
  protected Schema userRmdSchemaV1;
  protected Schema userSchemaV2;
  protected Schema personSchemaV1;
  protected Schema personRmdSchemaV1;
  protected Schema personSchemaV2;
  protected Schema personRmdSchemaV2;
  protected Schema personSchemaV3;
  protected Schema personRmdSchemaV3;
  protected RecordSerializer<GenericRecord> serializer;
  protected RecordDeserializer<GenericRecord> deserializer;

  @BeforeClass
  public void setUp() {
    this.storeName = "store";
    this.schemaRepository = mock(ReadOnlySchemaRepository.class);
    ValueAndDerivedSchemas userV1Schema = new ValueAndDerivedSchemas(storeName, -1, "avro/UserV1.avsc");
    ValueAndDerivedSchemas userV2Schema = new ValueAndDerivedSchemas(storeName, -1, "avro/UserV2.avsc");
    ValueAndDerivedSchemas personV1Schema = new ValueAndDerivedSchemas(storeName, -1, "avro/PersonV1.avsc");
    ValueAndDerivedSchemas personV2Schema = new ValueAndDerivedSchemas(storeName, -1, "avro/PersonV2.avsc");
    ValueAndDerivedSchemas personV3Schema = new ValueAndDerivedSchemas(storeName, -1, "avro/PersonV3.avsc");
    this.userSchemaV1 = userV1Schema.getValueSchema();
    this.userRmdSchemaV1 = userV1Schema.getRmdSchema();
    this.userSchemaV2 = userV2Schema.getValueSchema();
    this.personSchemaV1 = personV1Schema.getValueSchema();
    this.personRmdSchemaV1 = personV1Schema.getRmdSchema();
    this.personSchemaV2 = personV2Schema.getValueSchema();
    this.personRmdSchemaV2 = personV2Schema.getRmdSchema();
    this.personSchemaV3 = personV3Schema.getValueSchema();
    this.personRmdSchemaV3 = personV3Schema.getRmdSchema();

    // TODO: Move or rename this part.
    this.serializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(userSchemaV1);
    this.deserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(userSchemaV1, userSchemaV1);

    validateTestInputSchemas();
    RmdSchemaEntry rmdSchemaEntry = new RmdSchemaEntry(1, RMD_VERSION_ID, userRmdSchemaV1);
    doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());
    SchemaEntry valueSchemaEntry = new SchemaEntry(1, userSchemaV1);
    doReturn(valueSchemaEntry).when(schemaRepository).getSupersetOrLatestValueSchema(anyString());
    doReturn(valueSchemaEntry).when(schemaRepository).getValueSchema(anyString(), anyInt());
  }

  private void validateTestInputSchemas() {
    if (!AvroSupersetSchemaUtils.isSupersetSchema(personSchemaV3, personSchemaV2)) {
      throw new IllegalStateException(
          "Person V3 schema should be superset schema of Person V2 schema. Please double check these 2 schemas");
    }
    if (!AvroSupersetSchemaUtils.isSupersetSchema(personSchemaV3, personSchemaV1)) {
      throw new IllegalStateException(
          "Person V3 schema should be superset schema of Person V1 schema. Please double check these 2 schemas");
    }
  }

  protected GenericRecord createRmdWithValueLevelTimestamp(Schema rmdSchema, long valueLevelTimestamp) {
    final GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, valueLevelTimestamp);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD, new ArrayList<>());
    return rmdRecord;
  }

  protected GenericRecord createRmdWithFieldLevelTimestamp(
      Schema rmdSchema,
      Map<String, Long> fieldNameToTimestampMap) {
    return createRmdWithFieldLevelTimestamp(rmdSchema, fieldNameToTimestampMap, Collections.emptyMap());
  }

  protected GenericRecord createRmdWithFieldLevelTimestamp(
      Schema rmdSchema,
      Map<String, Long> fieldNameToTimestampMap,
      Map<String, Integer> fieldNameToExistingPutPartLengthMap) {
    final GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    final Schema fieldLevelTimestampSchema = rmdSchema.getFields().get(0).schema().getTypes().get(1);
    GenericRecord fieldTimestampsRecord = new GenericData.Record(fieldLevelTimestampSchema);
    fieldNameToTimestampMap.forEach((fieldName, fieldTimestamp) -> {
      Schema.Field field = fieldLevelTimestampSchema.getField(fieldName);
      if (field.schema().getType().equals(Schema.Type.LONG)) {
        fieldTimestampsRecord.put(fieldName, fieldTimestamp);
      } else {
        GenericRecord collectionFieldTimestampRecord = SchemaUtils.createGenericRecord(field.schema());
        // Only need to set the top-level field timestamp on collection timestamp record.
        collectionFieldTimestampRecord.put(TOP_LEVEL_TS_FIELD_NAME, fieldTimestamp);
        // When a collection field metadata is created, its top-level colo ID is always -1.
        collectionFieldTimestampRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, -1);
        collectionFieldTimestampRecord
            .put(PUT_ONLY_PART_LENGTH_FIELD_NAME, fieldNameToExistingPutPartLengthMap.getOrDefault(fieldName, 0));
        fieldTimestampsRecord.put(field.name(), collectionFieldTimestampRecord);
      }
    });
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, fieldTimestampsRecord);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD, new ArrayList<>());
    return rmdRecord;
  }

  protected RecordSerializer<GenericRecord> getSerializer(Schema writerSchema) {
    return MapOrderingPreservingSerDeFactory.getSerializer(writerSchema);
  }

  protected RecordDeserializer<GenericRecord> getDeserializer(Schema writerSchema, Schema readerSchema) {
    return MapOrderingPreservingSerDeFactory.getDeserializer(writerSchema, readerSchema);
  }

  protected Utf8 toUtf8(String str) {
    return new Utf8(str);
  }
}
