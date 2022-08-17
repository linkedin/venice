package com.linkedin.davinci.replication.merge;

import static com.linkedin.davinci.replication.merge.TestMergeConflictSchemaConstants.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.linkedin.davinci.serialization.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataConstants;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import java.util.ArrayList;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.BeforeClass;


public class TestMergeConflictResolver {
  protected static final int RMD_VERSION_ID = 1;

  protected String storeName;
  protected ReadOnlySchemaRepository schemaRepository;
  protected Schema valueRecordSchemaV1;
  protected Schema rmdSchemaV1;
  protected Schema valueRecordSchemaV2;
  protected Schema rmdSchemaV2;
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
    this.valueRecordSchemaV1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_RECORD_SCHEMA_STR_V1);
    this.rmdSchemaV1 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(valueRecordSchemaV1, RMD_VERSION_ID);
    this.valueRecordSchemaV2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_RECORD_SCHEMA_STR_V2);
    this.rmdSchemaV2 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(valueRecordSchemaV2, RMD_VERSION_ID);
    this.personSchemaV1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(PERSON_SCHEMA_STR_V1);
    this.personRmdSchemaV1 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(personSchemaV1, RMD_VERSION_ID);
    this.personSchemaV2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(PERSON_SCHEMA_STR_V2);
    this.personRmdSchemaV2 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(personSchemaV2, RMD_VERSION_ID);
    this.personSchemaV3 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(PERSON_SCHEMA_STR_V3);
    this.personRmdSchemaV3 = ReplicationMetadataSchemaGenerator.generateMetadataSchema(personSchemaV3, RMD_VERSION_ID);
    this.serializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueRecordSchemaV1);
    this.deserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueRecordSchemaV1, valueRecordSchemaV1);

    validateTestInputSchemas();
    ReplicationMetadataSchemaEntry rmdSchemaEntry = new ReplicationMetadataSchemaEntry(1, RMD_VERSION_ID, rmdSchemaV1);
    doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());

    SchemaEntry valueSchemaEntry = new SchemaEntry(1, valueRecordSchemaV1);
    doReturn(valueSchemaEntry).when(schemaRepository).getLatestValueSchema(anyString());
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
    rmdRecord.put(ReplicationMetadataConstants.TIMESTAMP_FIELD_NAME, valueLevelTimestamp);
    rmdRecord.put(ReplicationMetadataConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD, new ArrayList<>());
    return rmdRecord;
  }

  protected GenericRecord createRmdWithFieldLevelTimestamp(
      Schema rmdSchema,
      Map<String, Long> fieldNameToTimestampMap) {
    final GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    final Schema fieldLevelTimestampSchema = rmdSchema.getFields().get(0).schema().getTypes().get(1);
    GenericRecord fieldTimestampsRecord = new GenericData.Record(fieldLevelTimestampSchema);
    fieldNameToTimestampMap.forEach((fieldName, fieldTimestamp) -> {
      fieldTimestampsRecord.put(fieldName, fieldTimestamp);
    });
    rmdRecord.put(ReplicationMetadataConstants.TIMESTAMP_FIELD_NAME, fieldTimestampsRecord);
    rmdRecord.put(ReplicationMetadataConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD, new ArrayList<>());
    return rmdRecord;
  }

  protected RecordSerializer<GenericRecord> getSerializer(Schema writerSchema) {
    return MapOrderingPreservingSerDeFactory.getSerializer(writerSchema);
  }

  protected RecordDeserializer<GenericRecord> getDeserializer(Schema writerSchema, Schema readerSchema) {
    return MapOrderingPreservingSerDeFactory.getDeserializer(writerSchema, readerSchema);
  }
}
