package com.linkedin.venice.hadoop.input.kafka.ttl;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
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

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.schema.HDFSSchemaSource;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestVeniceKafkaInputTTLFilter {
  private static final long TTL_IN_SECONDS_DEFAULT = Time.SECONDS_PER_HOUR;
  private final static String TEST_STORE = "test_store";
  private static final String VALUE_RECORD_SCHEMA_STR =
      "{\"type\":\"record\"," + "\"name\":\"User\"," + "\"namespace\":\"example.avro\"," + "\"fields\":["
          + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"venice\"}]}";
  private static final Schema VALUE_SCHEMA =
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_RECORD_SCHEMA_STR);
  private static final Schema RMD_SCHEMA = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA, 1);
  private VeniceKafkaInputTTLFilter filterWithSupportedPolicy;
  private static final long DUMMY_CURRENT_TIMESTAMP = System.currentTimeMillis();
  private FilterChain<KafkaInputMapperValue> filterChain;

  @BeforeClass
  public void setUp() throws IOException {
    Properties validProps = new Properties();
    validProps.put(REPUSH_TTL_ENABLE, true);
    validProps.put(REPUSH_TTL_POLICY, TTLResolutionPolicy.RT_WRITE_ONLY.getValue());
    validProps.put(REPUSH_TTL_START_TIMESTAMP, DUMMY_CURRENT_TIMESTAMP - TTL_IN_SECONDS_DEFAULT * Time.MS_PER_SECOND);
    validProps.put(RMD_SCHEMA_DIR, getTempDataDirectory().getAbsolutePath());
    validProps.put(VALUE_SCHEMA_DIR, getTempDataDirectory().getAbsolutePath());
    validProps.put(VENICE_STORE_NAME_PROP, TEST_STORE);
    validProps.put(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.toString());
    validProps.put(KAFKA_INPUT_BROKER_URL, "dummy");
    validProps.put(KAFKA_INPUT_TOPIC, TEST_STORE + "_v1");
    VeniceProperties valid = new VeniceProperties(validProps);
    // set up HDFS schema source to write dummy RMD schemas on temp directory
    setupHDFS(valid);

    this.filterWithSupportedPolicy = new VeniceKafkaInputTTLFilter(valid);
    this.filterChain = new FilterChain<>(filterWithSupportedPolicy);
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

  @Test
  public void testFilterChain() {
    Assert.assertFalse(filterChain.isEmpty());
  }

  @Test
  public void testFilterWithRTPolicyWithValidValues() {
    List<KafkaInputMapperValue> records = generateRecord(4, 6, 4, DUMMY_CURRENT_TIMESTAMP, TTL_IN_SECONDS_DEFAULT);
    int validCount = 0, expiredCount = 0;
    for (KafkaInputMapperValue value: records) {
      if (filterWithSupportedPolicy.checkAndMaybeFilterValue(value)) {
        expiredCount++;
      } else {
        validCount++;
      }
    }
    Assert.assertEquals(validCount, 8); // 4 valid records and 4 chunked records
    Assert.assertEquals(expiredCount, 6);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testFilterWithRTPolicyWithInvalidValues() {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    Assert.assertFalse(filterWithSupportedPolicy.checkAndMaybeFilterValue(value));
  }

  /**
   * Generate a collection of KafkaInputMapperValue that have valid timestamp or invalid timestamp.
   * @param valid, the number of valid records
   * @param expired, the number of expired records
   * @return, a collection of KafkaInputMapperValue that have valid timestamp or invalid timestamp.
   */
  private List<KafkaInputMapperValue> generateRecord(
      int valid,
      int expired,
      int chunked,
      long currentTimestamp,
      long ttlInSeconds) {
    List<KafkaInputMapperValue> records = new ArrayList<>();

    // generate valid records
    for (int i = 0; i < valid; i++) {
      records.add(generateKIMWithRmdTimeStamp(currentTimestamp, false));
    }

    // generate expired records
    long expiredTimestamp = currentTimestamp - (TimeUnit.SECONDS.toMillis(ttlInSeconds) + 1);
    for (int i = 0; i < expired; i++) {
      records.add(generateKIMWithRmdTimeStamp(expiredTimestamp, false));
    }

    // generate expired chunked records, which should be filtered by the filter in mapper
    for (int i = 0; i < chunked; i++) {
      records.add(generateKIMWithRmdTimeStamp(expiredTimestamp, true));
    }
    return records;
  }

  private KafkaInputMapperValue generateKIMWithRmdTimeStamp(long timestamp, boolean isChunkedRecord) {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    value.schemaId = isChunkedRecord ? -10 : 1;
    value.replicationMetadataVersionId = 1;
    value.replicationMetadataPayload = ByteBuffer.wrap(
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(RMD_SCHEMA)
            .serialize(generateRmdRecordWithValueLevelTimeStamp(timestamp)));
    return value;
  }

  private GenericRecord generateRmdRecordWithValueLevelTimeStamp(long timestamp) {
    List<Long> vectors = Arrays.asList(1L, 2L, 3L);
    GenericRecord record = new GenericData.Record(RMD_SCHEMA);
    record.put(TIMESTAMP_FIELD_NAME, timestamp);
    record.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, vectors);
    return record;
  }
}
