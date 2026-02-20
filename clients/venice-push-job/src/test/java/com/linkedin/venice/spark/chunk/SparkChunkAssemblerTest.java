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
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedKeySuffixSerializer;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

  /**
   * Test that regular records with null chunked_key_suffix are handled correctly.
   * Non-chunked records should have null suffix and pass through assembly.
   */
  @Test
  public void testRegularRecordWithNullChunkedKeySuffix() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "regular-key".getBytes();
    byte[] value = "regular-value".getBytes();
    byte[] rmd = "regular-rmd".getBytes();

    // Create row with null chunked_key_suffix (non-chunked record)
    Row row = createRowWithSuffix(key, value, rmd, REGULAR_SCHEMA_ID, 1, 100L, 0, null);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    assertNotNull(assembled, "Regular record with null suffix should be assembled");
    assertEquals(assembled.getAs(KEY_COLUMN_NAME), key, "Key should match");
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), value, "Value should match");
  }

  /**
   * Test that the chunked_key_suffix field is properly passed through to ChunkAssembler.
   * This verifies that when a suffix is provided, it's correctly serialized into KafkaInputMapperValue
   * and used during assembly.
   */
  @Test
  public void testChunkedKeySuffixPassthrough() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "chunk-key".getBytes();
    byte[] value = "chunk-value".getBytes();
    byte[] rmd = "chunk-rmd".getBytes();
    byte[] chunkedSuffix = "test-suffix-bytes".getBytes();

    // Create row with an actual chunked_key_suffix for a regular PUT (not a chunk)
    // Regular PUTs should work fine even with a suffix present
    Row row = createRowWithSuffix(key, value, rmd, REGULAR_SCHEMA_ID, 1, 100L, 0, chunkedSuffix);

    // Verify the row has the suffix before assembly
    assertEquals(row.getAs("__chunked_key_suffix__"), chunkedSuffix, "Chunked suffix should be stored in row");

    // Assemble - regular PUT should pass through
    Iterator<Row> rows = Arrays.asList(row).iterator();
    Row assembled = assembler.assembleChunks(key, rows);

    // Verify assembly succeeded
    assertNotNull(assembled, "Record with suffix should be assembled");
    assertEquals(assembled.getAs(KEY_COLUMN_NAME), key, "Key should match");
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), value, "Value should match");
    assertEquals(assembled.getAs(RMD_COLUMN_NAME), rmd, "RMD should match");
  }

  /**
   * Test that chunk records with suffixes are properly serialized.
   * When ChunkAssembler receives chunk records (schema_id = CHUNK protocol version),
   * it needs the chunked_key_suffix to match chunks with their manifest.
   */
  @Test
  public void testChunkRecordsWithSuffixesDontCauseErrors() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "shared-key".getBytes();
    byte[] chunkData1 = "chunk-data-1".getBytes();
    byte[] chunkData2 = "chunk-data-2".getBytes();
    byte[] suffix1 = "suffix-for-chunk-0".getBytes();
    byte[] suffix2 = "suffix-for-chunk-1".getBytes();

    // Create chunk rows with different suffixes (offset DESC order)
    Row row1 = createRowWithSuffix(key, chunkData1, new byte[0], CHUNK_SCHEMA_ID, 1, 200L, 0, suffix1);
    Row row2 = createRowWithSuffix(key, chunkData2, new byte[0], CHUNK_SCHEMA_ID, 1, 100L, 0, suffix2);

    // Verify rows are created with different suffixes
    assertEquals(row1.getAs("__chunked_key_suffix__"), suffix1, "First row should have first suffix");
    assertEquals(row2.getAs("__chunked_key_suffix__"), suffix2, "Second row should have second suffix");

    // Try to assemble - this will return null because there's no manifest, but shouldn't throw
    // The important thing is that the suffixes are properly serialized into KafkaInputMapperValue
    Iterator<Row> rows = Arrays.asList(row1, row2).iterator();
    Row assembled = assembler.assembleChunks(key, rows);

    // Without a manifest, ChunkAssembler returns null (orphan chunks)
    // But it shouldn't throw an exception - the suffix serialization should work
    assertNull(assembled, "Chunks without manifest should return null (expected behavior)");
  }

  /**
   * Test that mixing regular records and chunk records with different suffixes works correctly.
   * The highest offset record (regular PUT) should win and be returned.
   */
  @Test
  public void testMixedRecordsWithSuffixes() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "mixed-key".getBytes();
    byte[] regularValue = "regular-value".getBytes();
    byte[] chunkData = "chunk-data".getBytes();
    byte[] chunkSuffix = "chunk-suffix".getBytes();

    // Create regular PUT with highest offset (should win)
    Row regularRow = createRowWithSuffix(key, regularValue, new byte[0], REGULAR_SCHEMA_ID, 1, 300L, 0, null);

    // Create chunk record with lower offset (should be ignored)
    Row chunkRow = createRowWithSuffix(key, chunkData, new byte[0], CHUNK_SCHEMA_ID, 1, 100L, 0, chunkSuffix);

    // Assemble with regular PUT first (highest offset)
    Iterator<Row> rows = Arrays.asList(regularRow, chunkRow).iterator();
    Row assembled = assembler.assembleChunks(key, rows);

    // Regular PUT should win
    assertNotNull(assembled, "Regular PUT should be returned");
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), regularValue, "Regular value should be returned");
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
          Collections.singletonList(
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

      RecordSerializer<GenericRecord> rmdSerializer =
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(rmdSchema);
      byte[] rmdBytes = rmdSerializer.serialize(rmdRecord);

      // Create value
      GenericRecord valueRecord = new GenericData.Record(valueSchema);
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

      VeniceProperties veniceProps = new VeniceProperties(props);

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

  /**
   * Test that empty chunked_key_suffix (empty byte array) is handled correctly.
   * Some records might have empty suffix vs null, both should work.
   */
  @Test
  public void testEmptyChunkedKeySuffix() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    byte[] key = "key-with-empty-suffix".getBytes();
    byte[] value = "value".getBytes();
    byte[] emptySuffix = new byte[0];

    Row row = createRowWithSuffix(key, value, new byte[0], REGULAR_SCHEMA_ID, 1, 100L, 0, emptySuffix);
    Iterator<Row> rows = Arrays.asList(row).iterator();

    Row assembled = assembler.assembleChunks(key, rows);

    assertNotNull(assembled, "Record with empty suffix should be assembled");
    assertEquals(assembled.getAs(VALUE_COLUMN_NAME), value, "Value should match");
  }

  /**
   * Test real chunk assembly with both chunked and regular records.
   * This test verifies that:
   * 1. Chunked records (chunks + manifest) are properly assembled into the original value
   * 2. Regular non-chunked records pass through unchanged
   * 3. The assembler correctly handles a mix of both types
   */
  @Test
  public void testChunkAssemblyWithMixedRecords() {
    SparkChunkAssembler assembler = new SparkChunkAssembler(false);

    // ===== Test 1: Regular non-chunked record =====
    byte[] regularKey = "regular-key".getBytes();
    byte[] regularValue = "regular-value-data".getBytes();
    byte[] regularRmd = "regular-rmd".getBytes();

    Row regularRow = createRow(regularKey, regularValue, regularRmd, REGULAR_SCHEMA_ID, 1, 100L, 0);
    Row assembledRegular = assembler.assembleChunks(regularKey, Arrays.asList(regularRow).iterator());

    assertNotNull(assembledRegular, "Regular record should be returned as-is");
    assertEquals(assembledRegular.getAs(KEY_COLUMN_NAME), regularKey, "Regular key should match");
    assertEquals(assembledRegular.getAs(VALUE_COLUMN_NAME), regularValue, "Regular value should match");
    assertEquals(assembledRegular.getAs(RMD_COLUMN_NAME), regularRmd, "Regular RMD should match");
    assertEquals(
        (int) assembledRegular.getAs(SCHEMA_ID_COLUMN_NAME),
        REGULAR_SCHEMA_ID,
        "Regular schema ID should match");

    // ===== Test 2: Chunked record (3 chunks + 1 manifest) =====
    byte[] chunkedKey = "chunked-key".getBytes();
    int numChunks = 3;
    int chunkSize = 10;
    int segmentNumber = 1;
    int sequenceNumber = 100;

    // Create the original value that will be chunked
    byte[] originalValue = new byte[numChunks * chunkSize];
    for (int i = 0; i < originalValue.length; i++) {
      originalValue[i] = (byte) i;
    }

    // Create chunk rows and manifest
    // NOTE: Rows must be in DESCENDING order by offset (manifest first, then chunks)
    List<Row> chunkRows = new ArrayList<>();
    KeyWithChunkingSuffixSerializer keySerializer = new KeyWithChunkingSuffixSerializer();
    List<ByteBuffer> chunkKeysWithSuffix = new ArrayList<>();

    // Create each chunk and collect their keys with suffixes for the manifest
    List<Row> chunkRowsTemp = new ArrayList<>();
    for (int i = 0; i < numChunks; i++) {
      // Create chunk data
      byte[] chunkData = new byte[chunkSize];
      System.arraycopy(originalValue, i * chunkSize, chunkData, 0, chunkSize);

      // Create chunked key suffix
      ChunkedKeySuffix suffix = createChunkedKeySuffix(segmentNumber, sequenceNumber, i);
      byte[] suffixBytes = serializeChunkedKeySuffix(suffix);

      // Create composite key (raw key + suffix) for the manifest
      ByteBuffer compositeKey = keySerializer.serializeChunkedKey(chunkedKey, suffix);
      chunkKeysWithSuffix.add(compositeKey);

      // Create chunk row with CHUNK schema ID
      // The chunked_key_suffix field should contain just the suffix bytes (not the full composite key)
      Row chunkRow = createRowWithSuffix(
          chunkedKey, // raw user key (without suffix)
          chunkData,
          new byte[0], // no RMD for chunks
          CHUNK_SCHEMA_ID,
          -1,
          200L + i, // offset
          0, // PUT
          suffixBytes); // just the suffix bytes
      chunkRowsTemp.add(chunkRow);
    }

    // Create manifest
    ChunkedValueManifest manifest = new ChunkedValueManifest();
    manifest.keysWithChunkIdSuffix = chunkKeysWithSuffix;
    manifest.schemaId = REGULAR_SCHEMA_ID; // The schema ID of the assembled value
    manifest.size = originalValue.length;

    ChunkedValueManifestSerializer manifestSerializer = new ChunkedValueManifestSerializer(true);
    byte[] manifestBytes = ByteUtils.extractByteArray(manifestSerializer.serialize(manifest));

    // Create manifest row with CHUNKED_VALUE_MANIFEST schema ID
    // Manifest has the highest offset
    Row manifestRow = createRowWithSuffix(
        chunkedKey,
        manifestBytes,
        new byte[0], // no RMD in manifest
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
        -1,
        200L + numChunks, // highest offset
        0, // PUT
        null); // manifest has no suffix

    // Add rows in DESCENDING order by offset (manifest first, then chunks in reverse order)
    chunkRows.add(manifestRow);
    for (int i = chunkRowsTemp.size() - 1; i >= 0; i--) {
      chunkRows.add(chunkRowsTemp.get(i));
    }

    // Assemble chunks
    Row assembledChunked = assembler.assembleChunks(chunkedKey, chunkRows.iterator());

    assertNotNull(assembledChunked, "Chunked record should be assembled");
    assertEquals(assembledChunked.getAs(KEY_COLUMN_NAME), chunkedKey, "Chunked key should match");

    // Verify assembled value matches original
    byte[] assembledValue = assembledChunked.getAs(VALUE_COLUMN_NAME);
    assertNotNull(assembledValue, "Assembled value should not be null");
    assertEquals(assembledValue.length, originalValue.length, "Assembled value length should match original");
    assertTrue(Arrays.equals(assembledValue, originalValue), "Assembled value should match original byte-by-byte");

    // Verify schema ID is restored from manifest
    assertEquals(
        (int) assembledChunked.getAs(SCHEMA_ID_COLUMN_NAME),
        REGULAR_SCHEMA_ID,
        "Assembled record should have original schema ID from manifest");
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
    // SCHEMA_FOR_CHUNK_ASSEMBLY: key, value, rmd, schema_id, rmd_version_id, offset, message_type, chunked_key_suffix
    return new GenericRowWithSchema(
        new Object[] { key, value, rmd, schemaId, rmdVersionId, offset, messageType, null },
        SCHEMA_FOR_CHUNK_ASSEMBLY);
  }

  private Row createRowWithSuffix(
      byte[] key,
      byte[] value,
      byte[] rmd,
      int schemaId,
      int rmdVersionId,
      long offset,
      int messageType,
      byte[] chunkedKeySuffix) {
    // SCHEMA_FOR_CHUNK_ASSEMBLY: key, value, rmd, schema_id, rmd_version_id, offset, message_type, chunked_key_suffix
    return new GenericRowWithSchema(
        new Object[] { key, value, rmd, schemaId, rmdVersionId, offset, messageType, chunkedKeySuffix },
        SCHEMA_FOR_CHUNK_ASSEMBLY);
  }

  /**
   * Helper method to create a ChunkedKeySuffix for testing.
   */
  private ChunkedKeySuffix createChunkedKeySuffix(int segmentNumber, int sequenceNumber, int chunkIndex) {
    ChunkId chunkId = new ChunkId();
    chunkId.segmentNumber = segmentNumber;
    chunkId.messageSequenceNumber = sequenceNumber;
    chunkId.chunkIndex = chunkIndex;
    chunkId.producerGUID = new GUID();
    ChunkedKeySuffix suffix = new ChunkedKeySuffix();
    suffix.chunkId = chunkId;
    suffix.isChunk = true;
    return suffix;
  }

  /**
   * Helper method to serialize ChunkedKeySuffix.
   */
  private byte[] serializeChunkedKeySuffix(ChunkedKeySuffix suffix) {
    ChunkedKeySuffixSerializer serializer = new ChunkedKeySuffixSerializer();
    return serializer.serialize("ignored", suffix);
  }

  private void writeSchemaToFile(File dir, int schemaId, Schema schema) throws java.io.IOException {
    // Value schemas are named with just the schema ID (no extension)
    File schemaFile = new File(dir, String.valueOf(schemaId));
    Files.write(schemaFile.toPath(), (schema.toString() + "\n").getBytes(StandardCharsets.UTF_8));
  }

  private void writeRmdSchemaToFile(File dir, int valueSchemaId, int rmdVersionId, Schema schema) throws IOException {
    // RMD schemas must be named valueSchemaId_rmdVersionId (e.g., "1_1")
    // This is required by HDFSSchemaSource.parseRmdSchemaIdsFromPath()
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
