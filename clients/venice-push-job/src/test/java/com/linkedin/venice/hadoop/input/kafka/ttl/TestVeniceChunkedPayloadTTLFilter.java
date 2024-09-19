package com.linkedin.venice.hadoop.input.kafka.ttl;

import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static com.linkedin.venice.utils.ChunkingTestUtils.createChunkBytes;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_POLICY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.serializer.avro.MapOrderPreservingSerDeFactory;
import com.linkedin.davinci.serializer.avro.MapOrderPreservingSerializer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkAssembler;
import com.linkedin.venice.hadoop.schema.HDFSSchemaSource;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.ChunkedKeySuffixSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestVeniceChunkedPayloadTTLFilter {
  private final static String TEST_STORE = "test_store";
  private static final String VALUE_RECORD_SCHEMA_STR =
      "{\"type\":\"record\"," + "\"name\":\"User\"," + "\"namespace\":\"example.avro\"," + "\"fields\":["
          + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"venice\"}]}";
  private static final Schema VALUE_SCHEMA =
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_RECORD_SCHEMA_STR);
  private static final Schema RMD_SCHEMA = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA, 1);
  private VeniceChunkedPayloadTTLFilter filter;

  private final ChunkAssembler chunkAssembler = new ChunkAssembler(true);
  private static final ChunkedKeySuffixSerializer CHUNKED_KEY_SUFFIX_SERIALIZER = new ChunkedKeySuffixSerializer();
  private static final RecordSerializer<KafkaInputMapperValue> KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);

  @BeforeClass
  public void setUp() throws IOException {
    Properties validProps = new Properties();
    validProps.put(REPUSH_TTL_ENABLE, true);
    validProps.put(REPUSH_TTL_POLICY, TTLResolutionPolicy.RT_WRITE_ONLY.getValue());
    validProps.put(REPUSH_TTL_START_TIMESTAMP, 10L);
    validProps.put(RMD_SCHEMA_DIR, getTempDataDirectory().getAbsolutePath());
    validProps.put(VALUE_SCHEMA_DIR, getTempDataDirectory().getAbsolutePath());
    validProps.put(VENICE_STORE_NAME_PROP, TEST_STORE);
    validProps.put(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.toString());
    validProps.put(KAFKA_INPUT_BROKER_URL, "dummy");
    validProps.put(KAFKA_INPUT_TOPIC, TEST_STORE + "_v1");
    VeniceProperties valid = new VeniceProperties(validProps);
    // set up HDFS schema source to write dummy RMD schemas on temp directory
    setupHDFS(valid);
    this.filter = new VeniceChunkedPayloadTTLFilter(valid);
  }

  @Test
  public void testTTLFilterHandlesRecord() {
    final byte[] serializedKey = createChunkBytes(0, 5);
    MapOrderPreservingSerializer valueSerializer = MapOrderPreservingSerDeFactory.getSerializer(VALUE_SCHEMA);
    MapOrderPreservingSerializer rmdSerializer = MapOrderPreservingSerDeFactory.getSerializer(RMD_SCHEMA);

    // For field level TS, it will always retain even though field level TS is not fresh enough, as it will be a bit
    // complicated to check through.
    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(RMD_SCHEMA, Collections.singletonMap("name", 10L));
    List<byte[]> values = Collections
        .singletonList(createRegularValue(null, rmdSerializer.serialize(rmdRecord), 1, 1, MapperValueType.DELETE));
    Assert.assertFalse(
        filter.checkAndMaybeFilterValue(chunkAssembler.assembleAndGetValue(serializedKey, values.iterator())));

    rmdRecord = createRmdWithFieldLevelTimestamp(RMD_SCHEMA, Collections.singletonMap("name", 9L));
    values = Collections
        .singletonList(createRegularValue(null, rmdSerializer.serialize(rmdRecord), 1, 1, MapperValueType.DELETE));
    Assert.assertTrue(
        filter.checkAndMaybeFilterValue(chunkAssembler.assembleAndGetValue(serializedKey, values.iterator())));

    // For value level TS, it will filter based on RMD timestamp.
    rmdRecord = createRmdWithValueLevelTimestamp(RMD_SCHEMA, 10L);
    values = Collections
        .singletonList(createRegularValue(null, rmdSerializer.serialize(rmdRecord), 1, 1, MapperValueType.DELETE));
    Assert.assertFalse(
        filter.checkAndMaybeFilterValue(chunkAssembler.assembleAndGetValue(serializedKey, values.iterator())));

    rmdRecord = createRmdWithValueLevelTimestamp(RMD_SCHEMA, 9L);
    values = Collections
        .singletonList(createRegularValue(null, rmdSerializer.serialize(rmdRecord), 1, 1, MapperValueType.DELETE));
    Assert.assertTrue(
        filter.checkAndMaybeFilterValue(chunkAssembler.assembleAndGetValue(serializedKey, values.iterator())));

    // Handle PUT case.
    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put("name", "abc");
    rmdRecord = createRmdWithValueLevelTimestamp(RMD_SCHEMA, 9L);
    values = Collections.singletonList(
        createRegularValue(
            valueSerializer.serialize(valueRecord),
            rmdSerializer.serialize(rmdRecord),
            1,
            1,
            MapperValueType.PUT));
    Assert.assertTrue(
        filter.checkAndMaybeFilterValue(chunkAssembler.assembleAndGetValue(serializedKey, values.iterator())));

    rmdRecord = createRmdWithValueLevelTimestamp(RMD_SCHEMA, 10L);
    values = Collections.singletonList(
        createRegularValue(
            valueSerializer.serialize(valueRecord),
            rmdSerializer.serialize(rmdRecord),
            1,
            1,
            MapperValueType.PUT));
    Assert.assertFalse(
        filter.checkAndMaybeFilterValue(chunkAssembler.assembleAndGetValue(serializedKey, values.iterator())));

    rmdRecord = createRmdWithFieldLevelTimestamp(RMD_SCHEMA, Collections.singletonMap("name", 10L));
    values = Collections.singletonList(
        createRegularValue(
            valueSerializer.serialize(valueRecord),
            rmdSerializer.serialize(rmdRecord),
            1,
            1,
            MapperValueType.PUT));
    Assert.assertFalse(
        filter.checkAndMaybeFilterValue(chunkAssembler.assembleAndGetValue(serializedKey, values.iterator())));

    rmdRecord = createRmdWithFieldLevelTimestamp(RMD_SCHEMA, Collections.singletonMap("name", 9L));
    values = Collections.singletonList(
        createRegularValue(
            valueSerializer.serialize(valueRecord),
            rmdSerializer.serialize(rmdRecord),
            1,
            1,
            MapperValueType.PUT));
    Assert.assertTrue(
        filter.checkAndMaybeFilterValue(chunkAssembler.assembleAndGetValue(serializedKey, values.iterator())));

  }

  @Test
  public void testTTLFilterHandleValuePayloadOfDelete() {
    byte[] rmdBytes = "dummyString".getBytes();
    List<byte[]> values = Collections.singletonList(createRegularValue(null, rmdBytes, 1, 1, MapperValueType.DELETE));
    final byte[] serializedKey = createChunkBytes(0, 5);
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());
    VeniceChunkedPayloadTTLFilter veniceChunkedPayloadTTLFilter = mock(VeniceChunkedPayloadTTLFilter.class);
    when(veniceChunkedPayloadTTLFilter.getValuePayload(assembledValue)).thenCallRealMethod();
    Assert.assertNull(veniceChunkedPayloadTTLFilter.getValuePayload(assembledValue));
  }

  private byte[] createRegularValue(
      byte[] valueBytes,
      byte[] rmdBytes,
      int schemaId,
      int offset,
      MapperValueType valueType) {
    KafkaInputMapperValue regularValue = new KafkaInputMapperValue();
    regularValue.chunkedKeySuffix = ByteBuffer
        .wrap(CHUNKED_KEY_SUFFIX_SERIALIZER.serialize("", KeyWithChunkingSuffixSerializer.NON_CHUNK_KEY_SUFFIX));
    regularValue.schemaId = schemaId;
    regularValue.offset = offset;
    regularValue.value = valueBytes == null ? ByteBuffer.wrap(new byte[0]) : ByteBuffer.wrap(valueBytes);
    regularValue.valueType = valueType;
    regularValue.replicationMetadataPayload =
        rmdBytes == null ? ByteBuffer.wrap(new byte[0]) : ByteBuffer.wrap(rmdBytes);
    regularValue.replicationMetadataVersionId = 1;
    return serialize(regularValue);
  }

  private byte[] serialize(KafkaInputMapperValue value) {
    return KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_SERIALIZER.serialize(value);
  }

  private void setupHDFS(VeniceProperties props) throws IOException {
    ControllerClient client = mock(ControllerClient.class);
    MultiSchemaResponse rmdResponse = new MultiSchemaResponse();
    rmdResponse.setSchemas(generateRmdSchemas(1));
    doReturn(rmdResponse).when(client).getAllReplicationMetadataSchemas(TEST_STORE);
    MultiSchemaResponse valueResponse = new MultiSchemaResponse();
    valueResponse.setSchemas(generateValueSchema(1));
    doReturn(valueResponse).when(client).getAllValueSchema(TEST_STORE);
    HDFSSchemaSource source =
        new HDFSSchemaSource(props.getString(VALUE_SCHEMA_DIR), props.getString(RMD_SCHEMA_DIR), TEST_STORE);
    source.saveSchemasOnDisk(client);
  }

  private MultiSchemaResponse.Schema[] generateRmdSchemas(int n) {
    MultiSchemaResponse.Schema[] response = new MultiSchemaResponse.Schema[n];
    for (int i = 1; i <= n; i++) {
      MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
      schema.setRmdValueSchemaId(i);
      schema.setDerivedSchemaId(i);
      schema.setId(i);
      schema.setSchemaStr(RMD_SCHEMA.toString());
      response[i - 1] = schema;
    }
    return response;
  }

  private MultiSchemaResponse.Schema[] generateValueSchema(int n) {
    MultiSchemaResponse.Schema[] response = new MultiSchemaResponse.Schema[n];
    for (int i = 1; i <= n; i++) {
      MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
      schema.setId(i);
      schema.setSchemaStr(VALUE_SCHEMA.toString());
      response[i - 1] = schema;
    }
    return response;
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
        GenericRecord collectionFieldTimestampRecord = AvroSchemaUtils.createGenericRecord(field.schema());
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
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());
    return rmdRecord;
  }

  protected GenericRecord createRmdWithValueLevelTimestamp(Schema rmdSchema, long valueLevelTimestamp) {
    final GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, valueLevelTimestamp);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());
    return rmdRecord;
  }
}
