package com.linkedin.venice.spark.chunk;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_VERSION_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_FOR_CHUNK_ASSEMBLY;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.Test;


public class SparkChunkAssemblerTest {
  private static final int REGULAR_SCHEMA_ID = 1;
  private static final int CHUNK_SCHEMA_ID = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();

  /**
   * Test assembling a regular non-chunked PUT record.
   * This should pass through without any assembly.
   */
  @Test
  public void testRegularPutRecord() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();
    byte[] rmd = "test-rmd".getBytes();
    int schemaId = REGULAR_SCHEMA_ID;
    int rmdVersionId = 1;
    long offset = 100L;

    Row row = createRow(key, value, rmd, schemaId, rmdVersionId, offset);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    assertNotNull(assembled, "Regular PUT should return assembled row");
    assertEquals(assembled.getAs(KEY_COLUMN_NAME), key, "Key should match");
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), value, "Value should match");
    assertEquals(assembled.getAs(RMD_COLUMN_NAME), rmd, "RMD should match");
    assertEquals((int) assembled.getAs(SCHEMA_ID_COLUMN_NAME), schemaId, "Schema ID should match");
    assertEquals((int) assembled.getAs(RMD_VERSION_ID_COLUMN_NAME), rmdVersionId, "RMD version ID should match");
  }

  /**
   * Test handling DELETE record with RMD.
   * DELETE records with RMD should return a row with empty value but preserved RMD.
   */
  @Test
  public void testDeleteRecordWithRmd() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "delete-key".getBytes();
    byte[] emptyValue = new byte[0];
    byte[] rmd = "delete-rmd".getBytes();
    int rmdVersionId = 1;
    long offset = 100L;

    Row row = createRow(key, emptyValue, rmd, REGULAR_SCHEMA_ID, rmdVersionId, offset);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    // DELETE with RMD should return a row
    assertNotNull(assembled, "DELETE with RMD should return a row");
    assertEquals(assembled.getAs(KEY_COLUMN_NAME), key, "Key should match");
    // ChunkAssembler returns the bytes it received (empty array for DELETE)
    byte[] assembledValue = assembled.getAs(VALUE_COLUMN_NAME);
    assertNotNull(assembledValue, "Value should not be null");
    assertEquals(assembledValue.length, 0, "Value should be empty for DELETE");
    assertEquals(assembled.getAs(RMD_COLUMN_NAME), rmd, "RMD should be preserved for DELETE");
  }

  /**
   * Test handling DELETE record without RMD.
   * DELETE records without RMD should still return a row with empty value
   */
  @Test
  public void testDeleteRecordWithoutRmd() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "delete-key-no-rmd".getBytes();
    byte[] emptyValue = new byte[0];
    byte[] emptyRmd = new byte[0];
    int deleteSchemaId = REGULAR_SCHEMA_ID;
    int rmdVersionId = 1;
    long offset = 100L;

    Row row = createRow(key, emptyValue, emptyRmd, deleteSchemaId, rmdVersionId, offset);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    // Even DELETE without meaningful RMD returns a row (empty RMD still has buffer)
    // This is expected behavior from ChunkAssembler
    assertNotNull(assembled, "DELETE should return a row");
    byte[] assembledValue = assembled.getAs(VALUE_COLUMN_NAME);
    assertEquals(assembledValue.length, 0, "Value should be empty for DELETE");
  }

  /**
   * Test handling chunk records without manifest.
   * Chunks without a manifest should be ignored and return null.
   */
  @Test
  public void testChunkWithoutManifest() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "chunk-key".getBytes();
    byte[] chunkValue = "chunk-data".getBytes();
    byte[] emptyRmd = new byte[0];

    // Create a chunk record (CHUNK schema ID)
    Row chunkRow = createRow(key, chunkValue, emptyRmd, CHUNK_SCHEMA_ID, -1, 100L);
    Iterator<Row> rows = Arrays.asList(chunkRow).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    // Orphan chunks without manifest should return null (assembly fails)
    assertNull(assembled, "Chunk without manifest should return null");
  }

  /**
   * Test handling empty row iterator.
   * Empty iterator should return null.
   */
  @Test
  public void testEmptyRowIterator() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "empty-key".getBytes();
    Iterator<Row> emptyRows = new ArrayList<Row>().iterator();

    Row assembled = assembler.assembleChunks(key, emptyRows);

    assertNull(assembled, "Empty iterator should return null");
  }

  /**
   * Test multiple records with offset being regular PUT.
   * The record with highest offset should be used.
   */
  @Test
  public void testMultipleRecordsHighestOffsetWins() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "multi-record-key".getBytes();
    byte[] newValue = "new-value".getBytes();
    byte[] oldValue = "old-value".getBytes();
    byte[] rmd = "rmd".getBytes();
    int schemaId = REGULAR_SCHEMA_ID;
    int rmdVersionId = 1;

    // Create rows in descending order by offset (highest first)
    List<Row> rows = new ArrayList<>();
    rows.add(createRow(key, newValue, rmd, schemaId, rmdVersionId, 102L)); // Highest offset - should be used
    rows.add(createRow(key, oldValue, rmd, schemaId, rmdVersionId, 101L)); // Lower offset - ignored

    Row assembled = assembler.assembleChunks(key, rows.iterator());

    assertNotNull(assembled, "Should return assembled row");
    // Should use the value with highest offset
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), newValue, "Should use value with highest offset");
  }

  /**
   * Test serialization of SparkChunkAssembler.
   * The assembler should be serializable for Spark.
   */
  @Test
  public void testSerializability() throws Exception {
    SparkChunkAssembler assembler = new SparkChunkAssembler(true);

    // Test that it can be serialized and deserialized
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(assembler);
    oos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    SparkChunkAssembler deserializedAssembler = (SparkChunkAssembler) ois.readObject();
    ois.close();

    assertNotNull(deserializedAssembler, "Deserialized assembler should not be null");

    // Test that the deserialized assembler still works
    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();
    byte[] rmd = "test-rmd".getBytes();
    Row row = createRow(key, value, rmd, REGULAR_SCHEMA_ID, 1, 100L);

    Row assembled = deserializedAssembler.assembleChunks(key, Arrays.asList(row).iterator());
    assertNotNull(assembled, "Deserialized assembler should work correctly");
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), value, "Value should match after deserialization");
  }

  /**
   * Test RMD chunking enabled flag.
   * The assembler should accept both enabled and disabled RMD chunking.
   */
  @Test
  public void testRmdChunkingFlag() {
    // Test with RMD chunking disabled
    SparkChunkAssembler assemblerDisabled = new SparkChunkAssembler(false);
    assertNotNull(assemblerDisabled, "Assembler with RMD chunking disabled should be created");

    // Test with RMD chunking enabled
    SparkChunkAssembler assemblerEnabled = new SparkChunkAssembler(true);
    assertNotNull(assemblerEnabled, "Assembler with RMD chunking enabled should be created");

    // Both should work for regular records
    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();
    byte[] rmd = "test-rmd".getBytes();
    Row row = createRow(key, value, rmd, REGULAR_SCHEMA_ID, 1, 100L, 0);

    Row assembledDisabled = assemblerDisabled.assembleChunks(key, Arrays.asList(row).iterator());
    Row assembledEnabled = assemblerEnabled.assembleChunks(key, Arrays.asList(row).iterator());

    assertNotNull(assembledDisabled, "Assembler with RMD chunking disabled should work");
    assertNotNull(assembledEnabled, "Assembler with RMD chunking enabled should work");
  }

  /**
   * Test that invalid message types are rejected.
   * Only PUT (0) and DELETE (1) are valid. CONTROL_MESSAGE (2), UPDATE (3), GLOBAL_RT_DIV (4)
   * should cause an exception.
   */
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Unexpected message type.*")
  public void testInvalidMessageTypeThrowsException() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();
    byte[] rmd = "test-rmd".getBytes();

    // messageType = 2 (CONTROL_MESSAGE) - should throw exception
    Row row = createRow(key, value, rmd, REGULAR_SCHEMA_ID, 1, 100L, 2);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    // This should throw IllegalStateException
    assembler.assembleChunks(key, rows);
  }

  private Row createRow(byte[] key, byte[] value, byte[] rmd, int schemaId, int rmdVersionId, long offset) {
    return createRow(key, value, rmd, schemaId, rmdVersionId, offset, 0); // Default to PUT
  }

  private Row createRow(
      byte[] key,
      byte[] value,
      byte[] rmd,
      int schemaId,
      int rmdVersionId,
      long offset,
      int messageType) {
    return new GenericRowWithSchema(
        new Object[] { key, value, rmd, schemaId, rmdVersionId, offset, messageType },
        SCHEMA_FOR_CHUNK_ASSEMBLY);
  }

  /**
   * Test that TTL filtering is correctly applied during chunk assembly.
   * This tests the integration between SparkChunkAssembler and SparkChunkedPayloadTTLFilter.
   */
  @Test
  public void testTTLFilteringDuringAssembly() throws Exception {
    // Create temp directories for schemas
    File valueSchemaTempDir = Files.createTempDirectory("value_schema_ttl2").toFile();
    File rmdSchemaTempDir = Files.createTempDirectory("rmd_schema_ttl2").toFile();

    try {
      // Create simple schema
      Schema valueSchema = Schema.createRecord("TestValue", "", "test", false);
      valueSchema.setFields(
          java.util.Collections.singletonList(
              AvroCompatibilityHelper.createSchemaField(
                  "name",
                  org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                  "",
                  null)));

      // Create RMD schema
      Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);

      // Write schemas
      // Value schemas are named just with schema ID
      writeSchemaToFile(valueSchemaTempDir, 1, valueSchema);
      // RMD schemas must be named valueSchemaId_rmdVersionId (e.g., "1_1")
      writeRmdSchemaToFile(rmdSchemaTempDir, 1, 1, rmdSchema);

      // Create RMD with value-level timestamp above TTL threshold
      long ttlThreshold = 1000L;
      GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
      rmdRecord.put("timestamp", ttlThreshold + 100);
      rmdRecord.put("replication_checkpoint_vector", java.util.Collections.emptyList());

      RecordSerializer<org.apache.avro.generic.GenericRecord> rmdSerializer =
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(rmdSchema);
      byte[] rmdBytes = rmdSerializer.serialize(rmdRecord);

      // Create value
      org.apache.avro.generic.GenericRecord valueRecord = new org.apache.avro.generic.GenericData.Record(valueSchema);
      valueRecord.put("name", "test-name");
      RecordSerializer<GenericRecord> valueSerializer =
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
      byte[] valueBytes = valueSerializer.serialize(valueRecord);

      // Create properties
      java.util.Properties props = new java.util.Properties();
      props.put("repush.ttl.policy", "0");
      props.put("repush.ttl.start.timestamp", String.valueOf(ttlThreshold));
      props.put("value.schema.dir", valueSchemaTempDir.getAbsolutePath());
      props.put("rmd.schema.dir", rmdSchemaTempDir.getAbsolutePath());
      props.put("kafka.input.topic", "test_store_v1");
      props.put("kafka.input.broker.url", "localhost:9092");
      props.put("kafka.input.source.compression.strategy", "NO_OP");

      VeniceProperties veniceProps = new com.linkedin.venice.utils.VeniceProperties(props);

      // Create assembler with TTL filtering
      SparkChunkAssembler assembler = new SparkChunkAssembler(false, true, veniceProps);

      byte[] key = "test-key".getBytes();
      Row row = createRow(key, valueBytes, rmdBytes, 1, 1, 100L, 0);

      // Assemble with TTL filtering - should keep the record (fresh timestamp)
      Row assembled = assembler.assembleChunks(key, Arrays.asList(row).iterator());

      // Fresh record should NOT be filtered
      assertNotNull(assembled, "Fresh record should not be filtered by TTL");
      assertEquals(assembled.getAs(KEY_COLUMN_NAME), key, "Key should match");
    } finally {
      deleteDirectory(valueSchemaTempDir);
      deleteDirectory(rmdSchemaTempDir);
    }
  }

  private void writeSchemaToFile(File dir, int schemaId, Schema schema) throws java.io.IOException {
    // Value schemas are named with just the schema ID (no extension)
    File schemaFile = new File(dir, String.valueOf(schemaId));
    Files.write(schemaFile.toPath(), (schema.toString() + "\n").getBytes(StandardCharsets.UTF_8));
  }

  private void writeRmdSchemaToFile(File dir, int valueSchemaId, int rmdVersionId, Schema schema)
      throws java.io.IOException {
    // RMD schemas must be named valueSchemaId_rmdVersionId (e.g., "1_1")
    // This is required by HDFSSchemaSource.parseRmdSchemaIdsFromPath()
    File schemaFile = new File(dir, valueSchemaId + "_" + rmdVersionId);
    Files.write(schemaFile.toPath(), (schema.toString() + "\n").getBytes(StandardCharsets.UTF_8));
  }

  private void deleteDirectory(java.io.File dir) {
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
