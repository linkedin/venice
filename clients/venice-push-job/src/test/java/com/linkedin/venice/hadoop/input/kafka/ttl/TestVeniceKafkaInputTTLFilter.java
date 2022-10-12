package com.linkedin.venice.hadoop.input.kafka.ttl;

import static com.linkedin.venice.hadoop.VenicePushJob.RMD_SCHEMA_DIR;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.utils.TestPushUtils.getTempDataDirectory;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.schema.HDFSRmdRmdSchemaSource;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestVeniceKafkaInputTTLFilter {
  private static final long TTL_IN_HOURS_DEFAULT = 10L;
  private final static String TEST_STORE = "test_store";
  private static final String VALUE_RECORD_SCHEMA_STR =
      "{\"type\":\"record\"," + "\"name\":\"User\"," + "\"namespace\":\"example.avro\"," + "\"fields\":["
          + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"venice\"}]}";
  private Schema valueSchema;
  private Schema rmdSchema;
  private HDFSRmdRmdSchemaSource source;
  private VeniceKafkaInputTTLFilter filterWithSupportedPolicy;
  private VeniceKafkaInputTTLFilter filterWithUnsupportedPolicy;

  @BeforeClass
  public void setUp() throws IOException {
    valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_RECORD_SCHEMA_STR);
    rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);

    File inputDir = getTempDataDirectory();

    Properties validProps = new Properties();
    validProps.put(VenicePushJob.REPUSH_TTL_IN_HOURS, TTL_IN_HOURS_DEFAULT);
    validProps.put(VenicePushJob.REPUSH_TTL_POLICY, 0);
    validProps.put(RMD_SCHEMA_DIR, inputDir.getAbsolutePath());
    validProps.put(VENICE_STORE_NAME_PROP, TEST_STORE);
    VeniceProperties valid = new VeniceProperties(validProps);

    Properties invalidProps = new Properties();
    invalidProps.put(VenicePushJob.REPUSH_TTL_IN_HOURS, TTL_IN_HOURS_DEFAULT);
    invalidProps.put(VenicePushJob.REPUSH_TTL_POLICY, 1);
    invalidProps.put(RMD_SCHEMA_DIR, inputDir.getAbsolutePath());
    VeniceProperties invalid = new VeniceProperties(invalidProps);

    // set up HDFS schema source to write dummy RMD schemas on temp directory
    setupHDFS(valid);

    this.filterWithSupportedPolicy = new VeniceKafkaInputTTLFilter(valid);
    this.filterWithUnsupportedPolicy = new VeniceKafkaInputTTLFilter(invalid);

  }

  private void setupHDFS(VeniceProperties props) throws IOException {
    ControllerClient client = mock(ControllerClient.class);
    // for simplicity of writing the test, we only have one schema on disk
    // so the both schemaId and valueSchemaID is 1
    MultiSchemaResponse.Schema[] schemas = generateMultiSchema(1);
    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setSchemas(schemas);
    doReturn(response).when(client).getAllReplicationMetadataSchemas(TEST_STORE);

    source = new HDFSRmdRmdSchemaSource(props.getString(VenicePushJob.RMD_SCHEMA_DIR), props, client);
    source.loadRmdSchemasOnDisk();
  }

  private MultiSchemaResponse.Schema[] generateMultiSchema(int n) {
    MultiSchemaResponse.Schema[] response = new MultiSchemaResponse.Schema[n];
    for (int i = 1; i <= n; i++) {
      MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
      schema.setValueSchemaId(i);
      schema.setDerivedSchemaId(i);
      schema.setId(i);
      schema.setSchemaStr(rmdSchema.toString());
      response[i - 1] = schema;
    }
    return response;
  }

  @Test
  public void testFilterChain() {
    filterWithSupportedPolicy.setNext(filterWithUnsupportedPolicy);
    Assert.assertTrue(filterWithSupportedPolicy.hasNext());
    Assert.assertEquals(filterWithSupportedPolicy.next(), filterWithUnsupportedPolicy);
    Assert.assertFalse(filterWithUnsupportedPolicy.hasNext());
    filterWithSupportedPolicy.setNext(null);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*policy is not supported.*")
  public void testFilterWithUnsupportedPolicy() {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    filterWithUnsupportedPolicy.applyRecursively(value);
  }

  @Test
  public void testFilterWithRejectBatchWritePolicyWithValidValues() {
    List<KafkaInputMapperValue> records = generateRecord(4, 6, Instant.now(), TTL_IN_HOURS_DEFAULT);
    int validCount = 0, expiredCount = 0;
    for (KafkaInputMapperValue value: records) {
      if (filterWithSupportedPolicy.applyRecursively(value)) {
        expiredCount++;
      } else {
        validCount++;
      }
    }
    Assert.assertEquals(validCount, 4);
    Assert.assertEquals(expiredCount, 6);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testFilterWithRejectBatchWritePolicyWithInValidValues() {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    Assert.assertFalse(filterWithSupportedPolicy.applyRecursively(value));
  }

  /**
   * Generate a collection of KafkaInputMapperValue that have valid timestamp or invalid timestamp.
   * @param valid, the number of valid records
   * @param expired, the number of expired records
   * @return, a collection of KafkaInputMapperValue that have valid timestamp or invalid timestamp.
   */
  private List<KafkaInputMapperValue> generateRecord(int valid, int expired, Instant curTime, long ttlInHours) {
    List<KafkaInputMapperValue> records = new ArrayList<>();

    // generate valid records
    for (int i = 0; i < valid; i++) {
      records.add(generateKIMWithRmdTimeStamp(curTime.toEpochMilli()));
    }

    // generate expired records
    Instant expiredTime = curTime.minus(ttlInHours + 1, ChronoUnit.HOURS); // add extra hour to get them expired
    for (int i = 0; i < expired; i++) {
      records.add(generateKIMWithRmdTimeStamp(expiredTime.toEpochMilli()));
    }
    return records;
  }

  private KafkaInputMapperValue generateKIMWithRmdTimeStamp(long timestamp) {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    value.schemaId = 1;
    value.replicationMetadataVersionId = 1;
    value.replicationMetadataPayload =
        RmdUtils.serializeRmdRecord(rmdSchema, generateRmdRecordWithValueLevelTimeStamp(timestamp));
    return value;
  }

  private GenericRecord generateRmdRecordWithValueLevelTimeStamp(long timestamp) {
    List<Long> vectors = Arrays.asList(1L, 2L, 3L);
    GenericRecord record = new GenericData.Record(rmdSchema);
    record.put(TIMESTAMP_FIELD_NAME, timestamp);
    record.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, vectors);
    return record;
  }
}
