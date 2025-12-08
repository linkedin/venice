package com.linkedin.venice.spark.input.kafka.ttl;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA_WITH_SCHEMA_ID;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_POLICY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_DIR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SparkChunkedPayloadTTLFilterTest {
  private static final String TEST_STORE = "test_store";
  private static final int VALUE_SCHEMA_ID = 1;
  private static final int RMD_VERSION_ID = 1;
  private static final long TTL_THRESHOLD = 1000L;

  private File valueSchemaTempDir;
  private File rmdSchemaTempDir;
  private Schema valueSchema;
  private Schema rmdSchema;

  @BeforeClass
  public void setUp() throws IOException {
    valueSchemaTempDir = Files.createTempDirectory("value_schema").toFile();
    rmdSchemaTempDir = Files.createTempDirectory("rmd_schema").toFile();

    // Create simple value schema with a map field for Active-Active testing
    valueSchema = Schema.createRecord("TestValue", "", "com.linkedin.test", false);
    valueSchema.setFields(
        Collections.singletonList(
            AvroCompatibilityHelper
                .createSchemaField("regions", Schema.createMap(Schema.create(Schema.Type.STRING)), "", null)));

    // Create RMD schema
    rmdSchema = createRmdSchema(valueSchema);

    // Write schemas to temp directories
    writeSchemaToFile(valueSchemaTempDir, VALUE_SCHEMA_ID, valueSchema);
    writeRmdSchemaToFile(rmdSchemaTempDir, VALUE_SCHEMA_ID, RMD_VERSION_ID, rmdSchema);
  }

  @AfterClass
  public void tearDown() {
    deleteDirectory(valueSchemaTempDir);
    deleteDirectory(rmdSchemaTempDir);
  }

  /**
   * Test that shouldFilter() method correctly filters completely stale records.
   */
  @Test
  public void testShouldFilterCompletelyStaleRecord() throws IOException {
    SparkChunkedPayloadTTLFilter filter = createFilter();

    try {
      // Create a record with value-level timestamp below TTL threshold
      GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(rmdSchema, TTL_THRESHOLD - 1);
      byte[] rmdBytes = serializeRmd(rmdRecord);

      Map<String, String> regions = new HashMap<>();
      regions.put("prod-lor1", "data1");
      byte[] valueBytes = serializeValue(createValueRecord(regions));

      Row assembledRow = createAssembledRow("key1".getBytes(), valueBytes, rmdBytes);

      SparkChunkedPayloadTTLFilter.FilterResult result = filter.filterAndUpdate(assembledRow);

      assertTrue(result.shouldFilter(), "Record with timestamp below TTL should be filtered");
    } finally {
      filter.close();
    }
  }

  /**
   * Test that shouldFilter() method keeps fresh records.
   */
  @Test
  public void testShouldFilterKeepsFreshRecord() throws IOException {
    SparkChunkedPayloadTTLFilter filter = createFilter();

    try {
      // Create a record with value-level timestamp above TTL threshold
      GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(rmdSchema, TTL_THRESHOLD + 1);
      byte[] rmdBytes = serializeRmd(rmdRecord);

      Map<String, String> regions = new HashMap<>();
      regions.put("prod-lor1", "data1");
      byte[] valueBytes = serializeValue(createValueRecord(regions));

      Row assembledRow = createAssembledRow("key1".getBytes(), valueBytes, rmdBytes);

      SparkChunkedPayloadTTLFilter.FilterResult result = filter.filterAndUpdate(assembledRow);

      assertFalse(result.shouldFilter(), "Record with timestamp above TTL should not be filtered");
    } finally {
      filter.close();
    }
  }

  /**
   * Test filterAndUpdate() with completely stale record (COMPLETELY_UPDATED case).
   */
  @Test
  public void testFilterAndUpdateCompletelyStale() throws IOException {
    SparkChunkedPayloadTTLFilter filter = createFilter();

    try {
      // Create a record with value-level timestamp below TTL threshold
      GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(rmdSchema, TTL_THRESHOLD - 1);
      byte[] rmdBytes = serializeRmd(rmdRecord);

      Map<String, String> regions = new HashMap<>();
      regions.put("prod-lor1", "data1");
      byte[] valueBytes = serializeValue(createValueRecord(regions));

      Row assembledRow = createAssembledRow("key1".getBytes(), valueBytes, rmdBytes);

      SparkChunkedPayloadTTLFilter.FilterResult result = filter.filterAndUpdate(assembledRow);

      assertNotNull(result, "FilterResult should not be null");
      assertTrue(result.shouldFilter(), "Completely stale record should be filtered");
    } finally {
      filter.close();
    }
  }

  /**
   * Test filterAndUpdate() with fresh record (NOT_UPDATED_AT_ALL case).
   */
  @Test
  public void testFilterAndUpdateFreshRecord() throws IOException {
    SparkChunkedPayloadTTLFilter filter = createFilter();

    try {
      // Create a record with value-level timestamp above TTL threshold
      GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(rmdSchema, TTL_THRESHOLD + 1);
      byte[] rmdBytes = serializeRmd(rmdRecord);

      Map<String, String> regions = new HashMap<>();
      regions.put("prod-lor1", "data1");
      byte[] valueBytes = serializeValue(createValueRecord(regions));

      Row assembledRow = createAssembledRow("key1".getBytes(), valueBytes, rmdBytes);

      SparkChunkedPayloadTTLFilter.FilterResult result = filter.filterAndUpdate(assembledRow);

      assertNotNull(result, "FilterResult should not be null");
      assertFalse(result.shouldFilter(), "Fresh record should not be filtered");

      // Verify wrapper contains original data
      SparkChunkedPayloadTTLFilter.AssembledRowWrapper wrapper = result.getWrapper();
      assertNotNull(wrapper, "Wrapper should not be null");
      assertEquals(wrapper.getValue(), valueBytes, "Value should be unchanged");
      assertEquals(wrapper.getRmd(), rmdBytes, "RMD should be unchanged");
    } finally {
      filter.close();
    }
  }

  /**
   * Test filterAndUpdate() correctly returns wrapper that can be modified.
   */
  @Test
  public void testFilterAndUpdateReturnsModifiableWrapper() throws IOException {
    SparkChunkedPayloadTTLFilter filter = createFilter();

    try {
      // Create a simple record (actual TTL filtering logic is tested elsewhere)
      GenericRecord rmdRecord = createRmdWithValueLevelTimestamp(rmdSchema, TTL_THRESHOLD + 1);
      byte[] originalRmdBytes = serializeRmd(rmdRecord);

      Map<String, String> regions = new HashMap<>();
      regions.put("prod-lor1", "data1");
      byte[] originalValueBytes = serializeValue(createValueRecord(regions));

      Row assembledRow = createAssembledRow("key1".getBytes(), originalValueBytes, originalRmdBytes);

      SparkChunkedPayloadTTLFilter.FilterResult result = filter.filterAndUpdate(assembledRow);

      assertNotNull(result, "FilterResult should not be null");
      SparkChunkedPayloadTTLFilter.AssembledRowWrapper wrapper = result.getWrapper();
      assertNotNull(wrapper, "Wrapper should not be null");

      byte[] newValue = "modified-value".getBytes();
      byte[] newRmd = "modified-rmd".getBytes();

      wrapper.setValue(newValue);
      wrapper.setRmd(newRmd);

      assertEquals(wrapper.getValue(), newValue, "Wrapper should support value mutation");
      assertEquals(wrapper.getRmd(), newRmd, "Wrapper should support RMD mutation");
    } finally {
      filter.close();
    }
  }

  /**
   * Test that AssembledRowWrapper setters correctly update values.
   */
  @Test
  public void testAssembledRowWrapperSetters() throws IOException {
    byte[] key = "test-key".getBytes();
    byte[] originalValue = "original-value".getBytes();
    byte[] originalRmd = "original-rmd".getBytes();

    Row assembledRow = createAssembledRow(key, originalValue, originalRmd);

    SparkChunkedPayloadTTLFilter.AssembledRowWrapper wrapper =
        new SparkChunkedPayloadTTLFilter.AssembledRowWrapper(assembledRow);

    // Modify values
    byte[] newValue = "new-value".getBytes();
    byte[] newRmd = "new-rmd".getBytes();

    wrapper.setValue(newValue);
    wrapper.setRmd(newRmd);

    // Verify setters worked
    assertEquals(wrapper.getValue(), newValue, "Value should be updated");
    assertEquals(wrapper.getRmd(), newRmd, "RMD should be updated");

    // Verify ByteBuffer is invalidated and recreated
    assertNotNull(wrapper.getValuePayload(), "Value payload should be recreated");
    assertNotNull(wrapper.getReplicationMetadataPayload(), "RMD payload should be recreated");
  }

  private SparkChunkedPayloadTTLFilter createFilter() throws IOException {
    Properties props = new Properties();
    props.put(REPUSH_TTL_POLICY, "0"); // RT_WRITE_ONLY
    props.put(REPUSH_TTL_START_TIMESTAMP, String.valueOf(TTL_THRESHOLD));
    props.put(VALUE_SCHEMA_DIR, valueSchemaTempDir.getAbsolutePath());
    props.put(RMD_SCHEMA_DIR, rmdSchemaTempDir.getAbsolutePath());
    props.put(KAFKA_INPUT_TOPIC, TEST_STORE + "_v1");
    props.put(KAFKA_INPUT_BROKER_URL, "localhost:9092");
    props.put(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.toString());

    return new SparkChunkedPayloadTTLFilter(new VeniceProperties(props));
  }

  private Row createAssembledRow(byte[] key, byte[] value, byte[] rmd) {
    return createAssembledRow(key, value, rmd, VALUE_SCHEMA_ID, RMD_VERSION_ID);
  }

  private Row createAssembledRow(byte[] key, byte[] value, byte[] rmd, int schemaId, int rmdVersionId) {
    return new GenericRowWithSchema(
        new Object[] { key, value, rmd, schemaId, rmdVersionId },
        DEFAULT_SCHEMA_WITH_SCHEMA_ID);
  }

  private GenericRecord createValueRecord(Map<String, String> regions) {
    GenericRecord record = new GenericData.Record(valueSchema);
    record.put("regions", regions);
    return record;
  }

  private GenericRecord createRmdWithValueLevelTimestamp(Schema rmdSchema, long timestamp) {
    GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, timestamp);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, Collections.emptyList());
    return rmdRecord;
  }

  private Schema createRmdSchema(Schema valueSchema) {
    // Use RmdSchemaGenerator to generate proper RMD schema
    return RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);
  }

  private byte[] serializeValue(GenericRecord record) {
    RecordSerializer<GenericRecord> serializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
    return serializer.serialize(record);
  }

  private byte[] serializeRmd(GenericRecord record) {
    RecordSerializer<GenericRecord> serializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(rmdSchema);
    return serializer.serialize(record);
  }

  private void writeSchemaToFile(File dir, int schemaId, Schema schema) throws IOException {
    File schemaFile = new File(dir, String.valueOf(schemaId));
    Files.write(schemaFile.toPath(), (schema.toString() + "\n").getBytes(StandardCharsets.UTF_8));
  }

  private void writeRmdSchemaToFile(File dir, int valueSchemaId, int rmdVersionId, Schema schema) throws IOException {
    // RMD schemas are named valueSchemaId_rmdVersionId (e.g., "1_1")
    File schemaFile = new File(dir, valueSchemaId + "_" + rmdVersionId);
    Files.write(schemaFile.toPath(), (schema.toString() + "\n").getBytes(StandardCharsets.UTF_8));
  }

  private void deleteDirectory(File dir) {
    if (dir != null && dir.exists()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file: files) {
          file.delete();
        }
      }
      dir.delete();
    }
  }
}
