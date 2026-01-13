package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.spark.SparkConstants.RAW_PUBSUB_INPUT_TABLE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_POLICY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_DIR;
import static org.testng.Assert.*;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.hadoop.input.kafka.ttl.TTLResolutionPolicy;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.spark.input.kafka.ttl.SparkKafkaInputTTLFilter;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Properties;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.Test;


public class SparkRepushJobTTLTest {
  /**
   * Test that records with empty RMD payload are handled appropriately.
   * According to the implementation, records without RMD should throw an exception
   * when TTL policy is RT_WRITE_ONLY.
   */
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*doesn't contain required RMD field.*")
  public void testTTLFilterWithEmptyRmd() throws IOException {
    // Setup
    Properties props = createBasicTTLProperties();
    SparkKafkaInputTTLFilter filter = new SparkKafkaInputTTLFilter(new VeniceProperties(props));

    try {
      // Create a PUT message with empty RMD
      Row rowWithEmptyRmd = createPutRow("key1", "value1", 1, new byte[0]);

      // Execute - should throw IllegalStateException
      assertTrue(filter.shouldFilter(rowWithEmptyRmd));
    } finally {
      filter.close();
    }
  }

  /**
   * Test that chunked records (negative schema ID) are skipped by TTL filter.
   */
  @Test
  public void testTTLFilterSkipsChunkedRecords() throws IOException {
    // Setup
    Properties props = createBasicTTLProperties();
    SparkKafkaInputTTLFilter filter = new SparkKafkaInputTTLFilter(new VeniceProperties(props));

    try {
      // Create a chunked record (negative schema ID)
      Row chunkedRow = createChunkedRow("key1", "chunk-data", -1);

      // Execute - chunked records should not be filtered (skip RMD check)
      boolean shouldFilter = filter.shouldFilter(chunkedRow);

      // Verify - should NOT filter chunked records
      assertFalse(shouldFilter, "Chunked records should not be filtered");
    } finally {
      filter.close();
    }
  }

  /**
   * Test that control messages (negative schema IDs like -10) are not filtered.
   */
  @Test
  public void testControlMessageNotFiltered() throws IOException {
    // Setup
    Properties props = createBasicTTLProperties();
    SparkKafkaInputTTLFilter filter = new SparkKafkaInputTTLFilter(new VeniceProperties(props));

    try {
      Row controlRow = createControlRow();

      boolean shouldFilter = filter.shouldFilter(controlRow);

      assertFalse(shouldFilter, "Control messages should not be filtered by TTL");
    } finally {
      filter.close();
    }
  }

  /**
   * Test edge case: null RMD payload throws exception (required for TTL filtering).
   */
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*doesn't contain required RMD field.*")
  public void testNullRmdPayload() throws IOException {
    // Setup
    Properties props = createBasicTTLProperties();
    SparkKafkaInputTTLFilter filter = new SparkKafkaInputTTLFilter(new VeniceProperties(props));

    try {
      // Create a PUT message with null RMD
      Row rowWithNullRmd = new GenericRowWithSchema(
          new Object[] { "region1", 0, 100L, MessageType.PUT.getValue(), 1, "key1".getBytes(), "value1".getBytes(), 1,
              null, null },
          RAW_PUBSUB_INPUT_TABLE_SCHEMA);

      // Execute - null RMD should throw exception
      filter.shouldFilter(rowWithNullRmd);
    } finally {
      filter.close();
    }
  }

  /**
   * Test that DELETE messages with empty RMD throw exception (same as PUT with empty RMD).
   * This is expected behavior - RMD is required for TTL filtering.
   */
  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*doesn't contain required RMD field.*")
  public void testDeleteMessageWithEmptyRmd() throws IOException {
    // Setup
    Properties props = createBasicTTLProperties();
    SparkKafkaInputTTLFilter filter = new SparkKafkaInputTTLFilter(new VeniceProperties(props));

    try {
      Row deleteRow = createDeleteRow("key1", 1, 1, new byte[0]);

      filter.shouldFilter(deleteRow);
    } finally {
      filter.close();
    }
  }

  /**
   * Test that chunked records with different negative schema IDs are not filtered.
   */
  @Test
  public void testMultipleChunkedSchemaIds() throws IOException {
    // Setup
    Properties props = createBasicTTLProperties();
    SparkKafkaInputTTLFilter filter = new SparkKafkaInputTTLFilter(new VeniceProperties(props));

    try {
      // Test different chunk-related schema IDs
      Row chunkRow1 = createChunkedRow("key1", "chunk1", -1);
      Row chunkRow2 = createChunkedRow("key2", "chunk2", -2);
      Row manifestRow = createChunkedRow("key3", "manifest", -20);

      // Execute
      boolean shouldFilter1 = filter.shouldFilter(chunkRow1);
      boolean shouldFilter2 = filter.shouldFilter(chunkRow2);
      boolean shouldFilter3 = filter.shouldFilter(manifestRow);

      // Verify - all chunked records should NOT be filtered
      assertFalse(shouldFilter1, "Chunk record should not be filtered");
      assertFalse(shouldFilter2, "Chunk record should not be filtered");
      assertFalse(shouldFilter3, "Manifest record should not be filtered");
    } finally {
      filter.close();
    }
  }

  private Properties createBasicTTLProperties() {
    Properties props = new Properties();
    props.setProperty(REPUSH_TTL_POLICY, String.valueOf(TTLResolutionPolicy.RT_WRITE_ONLY.getValue()));
    props.setProperty(REPUSH_TTL_START_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
    props.setProperty(VALUE_SCHEMA_DIR, "/tmp/value-schemas"); // Would need real path in integration test
    props.setProperty(RMD_SCHEMA_DIR, "/tmp/rmd-schemas"); // Would need real path in integration test
    props.setProperty(KAFKA_INPUT_TOPIC, "test_store_v1");
    props.setProperty(KAFKA_INPUT_BROKER_URL, "localhost:9092");
    props.setProperty(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.name());
    return props;
  }

  private Row createPutRow(String key, String value, int schemaId, byte[] rmdPayload) {
    return new GenericRowWithSchema(
        new Object[] { "region1", 0, 100L, MessageType.PUT.getValue(), schemaId, key.getBytes(), value.getBytes(), 1,
            rmdPayload, null },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  private Row createChunkedRow(String key, String chunkData, int negativeSchemaId) {
    return new GenericRowWithSchema(
        new Object[] { "region1", 0, 100L, MessageType.PUT.getValue(), negativeSchemaId, key.getBytes(),
            chunkData.getBytes(), -1, new byte[0], null },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  private Row createDeleteRow(String key, int valueSchemaId, int rmdVersionId, byte[] rmdPayload) {
    return new GenericRowWithSchema(
        new Object[] { "region1", 0, 100L, MessageType.DELETE.getValue(), valueSchemaId, key.getBytes(), new byte[0],
            rmdVersionId, rmdPayload, null },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  private Row createControlRow() {
    return new GenericRowWithSchema(
        new Object[] { "region1", 0, 100L, MessageType.CONTROL_MESSAGE.getValue(), -10, new byte[0], new byte[0], -1,
            new byte[0], null },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }
}
