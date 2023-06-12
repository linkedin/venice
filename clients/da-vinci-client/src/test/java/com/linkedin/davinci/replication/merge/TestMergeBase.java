package com.linkedin.davinci.replication.merge;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.replication.merge.helper.utils.ValueAndDerivedSchemas;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.utils.IndexedHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.BeforeClass;


public class TestMergeBase {
  protected static final int UPDATE_SCHEMA_PROTOCOL_VERSION = 1;
  protected static final int RMD_SCHEMA_PROTOCOL_VERSION = RmdSchemaGenerator.getLatestVersion();
  protected static final String REGULAR_FIELD_NAME = "regularField";
  protected static final String STRING_ARRAY_FIELD_NAME = "stringArrayField";
  protected static final String STRING_MAP_FIELD_NAME = "stringMapField";
  protected static final String NULLABLE_STRING_ARRAY_FIELD_NAME = "nullableStringArrayField";
  protected static final String NULLABLE_STRING_MAP_FIELD_NAME = "nullableStringMapField";

  protected static final String storeName = "testStore";
  protected ValueAndDerivedSchemas schemaSet;
  protected ReadOnlySchemaRepository schemaRepository;
  protected StringAnnotatedStoreSchemaCache annotatedStoreSchemaCache;
  protected MergeConflictResolver mergeConflictResolver;
  protected RmdSerDe rmdSerDe;

  @BeforeClass
  public void setUp() {
    schemaRepository = mock(ReadOnlySchemaRepository.class);
    schemaSet = new ValueAndDerivedSchemas(storeName, 1, "avro/PartialUpdateWithMapField.avsc");
    setupSchemaRepoSchemaMock(schemaRepository, schemaSet);
    setupSchemaRepoSupersetSchemaMock(schemaRepository, schemaSet);
    annotatedStoreSchemaCache = new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);
    rmdSerDe = new RmdSerDe(annotatedStoreSchemaCache, RMD_SCHEMA_PROTOCOL_VERSION);
    mergeConflictResolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(annotatedStoreSchemaCache, rmdSerDe, storeName);
  }

  protected void setupSchemaRepoSchemaMock(
      ReadOnlySchemaRepository schemaRepo,
      ValueAndDerivedSchemas valueAndDerivedSchemas) {
    String storeName = valueAndDerivedSchemas.getStoreName();
    int version = valueAndDerivedSchemas.getValueSchemaId();
    when(schemaRepo.getValueSchema(storeName, version)).thenReturn(valueAndDerivedSchemas.getValueSchemaEntry());
    when(schemaRepo.getDerivedSchema(storeName, version, UPDATE_SCHEMA_PROTOCOL_VERSION))
        .thenReturn(valueAndDerivedSchemas.getUpdateSchemaEntry());
    when(schemaRepo.getReplicationMetadataSchema(storeName, version, RMD_SCHEMA_PROTOCOL_VERSION))
        .thenReturn(valueAndDerivedSchemas.getRmdSchemaEntry());
  }

  protected void setupSchemaRepoSupersetSchemaMock(
      ReadOnlySchemaRepository schemaRepo,
      ValueAndDerivedSchemas valueAndDerivedSchemas) {
    String storeName = valueAndDerivedSchemas.getStoreName();
    when(schemaRepo.getSupersetSchema(storeName)).thenReturn(valueAndDerivedSchemas.getValueSchemaEntry());
  }

  protected GenericRecord initiateFieldLevelRmdRecord(GenericRecord oldValueRecord, long initialTs) {
    GenericRecord rmdRecord = new GenericData.Record(schemaSet.getRmdSchema());
    GenericRecord fieldTimestampsRecord =
        mergeConflictResolver.createPerFieldTimestampRecord(schemaSet.getRmdSchema(), initialTs, oldValueRecord);
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, fieldTimestampsRecord);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD, new ArrayList<>());
    return createRmdRecord(rmdRecord);
  }

  protected GenericRecord initiateValueLevelRmdRecord(long initialTs) {
    GenericRecord rmdRecord = new GenericData.Record(schemaSet.getRmdSchema());
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, initialTs);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD, new ArrayList<>());
    return createRmdRecord(rmdRecord);
  }

  protected GenericRecord createRmdRecord(GenericRecord record) {
    int valueSchema = schemaSet.getValueSchemaId();
    return rmdSerDe.deserializeRmdBytes(valueSchema, valueSchema, rmdSerDe.serializeRmdRecord(valueSchema, record));
  }

  protected GenericRecord createValueRecord(Consumer<GenericRecord> recordConsumer) {
    GenericRecord oldValueRecord = new GenericData.Record(schemaSet.getValueSchema());
    recordConsumer.accept(oldValueRecord);
    return oldValueRecord;
  }

  protected GenericRecord createDefaultValueRecord() {
    return createValueRecord(r -> {
      r.put(REGULAR_FIELD_NAME, "defaultVenice");
      r.put(STRING_ARRAY_FIELD_NAME, Collections.emptyList());
      r.put(STRING_MAP_FIELD_NAME, new IndexedHashMap<>());
      r.put(NULLABLE_STRING_ARRAY_FIELD_NAME, null);
      r.put(NULLABLE_STRING_MAP_FIELD_NAME, null);
    });
  }

  protected void setRegularFieldTimestamp(GenericRecord rmdRecord, long timestamp) {
    GenericRecord fieldTimestampsRecord = (GenericRecord) rmdRecord.get(RmdConstants.TIMESTAMP_FIELD_NAME);
    fieldTimestampsRecord.put(REGULAR_FIELD_NAME, timestamp);
  }

  protected ByteBuffer serializeValueRecord(GenericRecord valueRecord) {
    return ByteBuffer.wrap(getSerializer(schemaSet.getValueSchema()).serialize(valueRecord));
  }

  protected ByteBuffer serializeUpdateRecord(GenericRecord updateRecord) {
    return ByteBuffer.wrap(getSerializer(schemaSet.getUpdateSchema()).serialize(updateRecord));
  }

  protected GenericRecord deserializeValueRecord(ByteBuffer valueByteBuffer) {
    return getDeserializer(schemaSet.getValueSchema(), schemaSet.getValueSchema()).deserialize(valueByteBuffer);
  }

  protected RecordSerializer<GenericRecord> getSerializer(Schema writerSchema) {
    return MapOrderingPreservingSerDeFactory.getAvroGenericSerializer(writerSchema);
  }

  protected RecordDeserializer<GenericRecord> getDeserializer(Schema writerSchema, Schema readerSchema) {
    return MapOrderingPreservingSerDeFactory.getAvroGenericDeserializer(writerSchema, readerSchema);
  }
}
