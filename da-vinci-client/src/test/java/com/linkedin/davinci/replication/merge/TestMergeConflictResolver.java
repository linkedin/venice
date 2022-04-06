package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.BeforeClass;

import static com.linkedin.davinci.replication.merge.TestMergeConflictSchemaConstants.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;


public class TestMergeConflictResolver {
  protected static final int RMD_VERSION_ID = 1;

  protected String storeName;
  protected ReadOnlySchemaRepository schemaRepository;
  protected Schema valueRecordSchemaV1;
  protected Schema rmdSchemaV1;
  protected Schema valueRecordSchemaV2;
  protected Schema rmdSchemaV2;
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

    this.serializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueRecordSchemaV1);
    this.deserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueRecordSchemaV1,
        valueRecordSchemaV1);

    ReplicationMetadataSchemaEntry rmdSchemaEntry
        = new ReplicationMetadataSchemaEntry(1, RMD_VERSION_ID, rmdSchemaV1);
    doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());

    SchemaEntry valueSchemaEntry = new SchemaEntry(1, valueRecordSchemaV1);
    doReturn(valueSchemaEntry).when(schemaRepository).getLatestValueSchema(anyString());
  }
}
