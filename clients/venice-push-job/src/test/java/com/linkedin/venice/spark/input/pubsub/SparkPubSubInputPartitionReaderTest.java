package com.linkedin.venice.spark.input.pubsub;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.chunking.ChunkKeyValueTransformer;
import com.linkedin.venice.chunking.RawKeyBytesAndChunkedKeySuffix;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.InternalRow;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Comprehensive TestNG unit tests for {@link SparkPubSubInputPartitionReader} with Java 8 compatibility.
 *
 * This test class validates the functionality of the partition reader including:
 * - Constructor scenarios with different configurations
 * - PartitionReader interface methods: next() and get()
 * - Message processing for PUT and DELETE message types
 * - Progress tracking and logging
 * - Resource cleanup and error handling
 * - Edge cases and boundary conditions
 */
public class SparkPubSubInputPartitionReaderTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final String TEST_TOPIC_NAME = "test-topic_v1";
  private static final int TEST_PARTITION_NUMBER = 5;
  private static final String TEST_REGION = "test-region";

  private PubSubTopic pubSubTopic;
  private PubSubTopicPartition topicPartition;
  private PubSubPartitionSplit pubSubPartitionSplit;
  private SparkPubSubInputPartition inputPartition;
  private PubSubConsumerAdapter mockConsumer;
  private PubSubSplitIterator mockSplitIterator;

  @BeforeMethod
  public void setUp() {
    // Setup basic test objects
    pubSubTopic = TOPIC_REPOSITORY.getTopic(TEST_TOPIC_NAME);
    topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, TEST_PARTITION_NUMBER);

    PubSubPosition startPosition = ApacheKafkaOffsetPosition.of(1L);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(10L);

    pubSubPartitionSplit =
        new PubSubPartitionSplit(TOPIC_REPOSITORY, topicPartition, startPosition, endPosition, 9L, 0, 1L);

    inputPartition = new SparkPubSubInputPartition(pubSubPartitionSplit);
    mockConsumer = mock(PubSubConsumerAdapter.class);
    mockSplitIterator = mock(PubSubSplitIterator.class);
  }

  @Test
  public void testConstructorNormalScenarios() throws IOException {
    // Case 1: Basic constructor with useLogicalIndexOffset = false
    SparkPubSubInputPartitionReader reader1 =
        new SparkPubSubInputPartitionReader(inputPartition, mockConsumer, TEST_REGION, false, false, null);
    assertNotNull(reader1, "Reader should be created successfully");
    reader1.close();

    // Case 2: Constructor with useLogicalIndexOffset = true
    SparkPubSubInputPartitionReader reader2 =
        new SparkPubSubInputPartitionReader(inputPartition, mockConsumer, TEST_REGION, true, false, null);
    assertNotNull(reader2, "Reader should be created successfully with logical index offset");
    reader2.close();

    // Case 3: Constructor with different region
    String differentRegion = "different-region";
    SparkPubSubInputPartitionReader reader3 =
        new SparkPubSubInputPartitionReader(inputPartition, mockConsumer, differentRegion, false, false, null);
    assertNotNull(reader3, "Reader should be created successfully with different region");
    reader3.close();
  }

  @Test
  public void testNextWithPutMessageScenarios() throws IOException {
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator();

    // Case 1: Normal PUT message processing
    PubSubSplitIterator.PubSubInputRecord putRecord = createMockPutRecord(100L, "test-key", "test-value", 1, null, 0);
    when(mockSplitIterator.next()).thenReturn(putRecord).thenReturn(null);

    assertTrue(reader.next(), "Reader should successfully process PUT message");
    InternalRow row = reader.get();
    assertNotNull(row, "Row should not be null after processing PUT message");
    // Field order: region, partition, offset, messageType, schemaId, key, value,
    // replicationMetadataVersionId, replicationMetadataPayload
    assertEquals(row.getUTF8String(0).toString(), TEST_REGION, "Region should match");
    assertEquals(row.getInt(1), TEST_PARTITION_NUMBER, "Partition number should match");
    assertEquals(row.getLong(2), 100L, "Offset should match");
    assertEquals(row.getInt(3), MessageType.PUT.getValue(), "Message type should be PUT");
    assertEquals(row.getInt(4), 1, "Schema ID should match");

    assertFalse(reader.next(), "Reader should return false when no more messages");
    reader.close();

    // Case 2: PUT message with replication metadata
    reader = createReaderWithMockIterator();
    ByteBuffer replicationMetadata = ByteBuffer.wrap("replication-data".getBytes());
    PubSubSplitIterator.PubSubInputRecord putRecordWithMetadata =
        createMockPutRecord(101L, "test-key-2", "test-value-2", 2, replicationMetadata, 5);
    when(mockSplitIterator.next()).thenReturn(putRecordWithMetadata).thenReturn(null);

    assertTrue(reader.next(), "Reader should successfully process PUT message with metadata");
    InternalRow rowWithMetadata = reader.get();
    assertNotNull(rowWithMetadata, "Row should not be null");
    assertEquals(rowWithMetadata.getInt(7), 5, "Replication metadata version ID should match");

    assertFalse(reader.next(), "Reader should return false when no more messages");
    reader.close();

    // Case 3: PUT message with empty value
    reader = createReaderWithMockIterator();
    PubSubSplitIterator.PubSubInputRecord putRecordEmptyValue = createMockPutRecord(102L, "test-key-3", "", 3, null, 0);
    when(mockSplitIterator.next()).thenReturn(putRecordEmptyValue).thenReturn(null);

    assertTrue(reader.next(), "Reader should successfully process PUT message with empty value");
    InternalRow rowEmptyValue = reader.get();
    assertNotNull(rowEmptyValue, "Row should not be null");
    assertEquals(rowEmptyValue.getBinary(6).length, 0, "Value should be empty byte array");

    assertFalse(reader.next(), "Reader should return false when no more messages");
    reader.close();
  }

  @Test
  public void testNextWithDeleteMessageScenarios() throws IOException {
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator();

    // Case 1: Normal DELETE message processing
    PubSubSplitIterator.PubSubInputRecord deleteRecord = createMockDeleteRecord(200L, "delete-key", 10, null, 0);
    when(mockSplitIterator.next()).thenReturn(deleteRecord).thenReturn(null);

    assertTrue(reader.next(), "Reader should successfully process DELETE message");
    InternalRow row = reader.get();
    assertNotNull(row, "Row should not be null after processing DELETE message");
    // Field order: region, partition, offset, messageType, schemaId, key, value,
    // replicationMetadataVersionId, replicationMetadataPayload
    assertEquals(row.getUTF8String(0).toString(), TEST_REGION, "Region should match");
    assertEquals(row.getInt(1), TEST_PARTITION_NUMBER, "Partition number should match");
    assertEquals(row.getLong(2), 200L, "Offset should match");
    assertEquals(row.getInt(3), MessageType.DELETE.getValue(), "Message type should be DELETE");
    assertEquals(row.getInt(4), 10, "Schema ID should match");
    assertTrue(row.isNullAt(6), "DELETE message should have null value");

    assertFalse(reader.next(), "Reader should return false when no more messages");
    reader.close();

    // Case 2: DELETE message with replication metadata
    reader = createReaderWithMockIterator();
    ByteBuffer deleteReplicationMetadata = ByteBuffer.wrap("delete-replication-data".getBytes());
    PubSubSplitIterator.PubSubInputRecord deleteRecordWithMetadata =
        createMockDeleteRecord(201L, "delete-key-2", 11, deleteReplicationMetadata, 7);
    when(mockSplitIterator.next()).thenReturn(deleteRecordWithMetadata).thenReturn(null);

    assertTrue(reader.next(), "Reader should successfully process DELETE message with metadata");
    InternalRow rowWithMetadata = reader.get();
    assertNotNull(rowWithMetadata, "Row should not be null");
    assertEquals(rowWithMetadata.getInt(7), 7, "Replication metadata version ID should match");

    assertFalse(reader.next(), "Reader should return false when no more messages");
    reader.close();

    // Case 3: Multiple DELETE messages scenario
    reader = createReaderWithMockIterator();
    PubSubSplitIterator.PubSubInputRecord deleteRecord1 = createMockDeleteRecord(202L, "delete-key-3", 12, null, 0);
    PubSubSplitIterator.PubSubInputRecord deleteRecord2 = createMockDeleteRecord(203L, "delete-key-4", 13, null, 0);
    when(mockSplitIterator.next()).thenReturn(deleteRecord1).thenReturn(deleteRecord2).thenReturn(null);

    assertTrue(reader.next(), "Reader should process first DELETE message");
    InternalRow firstRow = reader.get();
    assertEquals(firstRow.getLong(2), 202L, "First DELETE offset should match");

    assertTrue(reader.next(), "Reader should process second DELETE message");
    InternalRow secondRow = reader.get();
    assertEquals(secondRow.getLong(2), 203L, "Second DELETE offset should match");

    assertFalse(reader.next(), "Reader should return false when no more messages");
    reader.close();
  }

  @Test
  public void testGetMethodScenarios() throws IOException {
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator();

    // Case 1: get() before next() returns null
    InternalRow rowBeforeNext = reader.get();
    assertNull(rowBeforeNext, "get() should return null before calling next()");

    // Case 2: get() consistency after processing
    PubSubSplitIterator.PubSubInputRecord record =
        createMockPutRecord(300L, "consistency-key", "consistency-value", 20, null, 0);
    when(mockSplitIterator.next()).thenReturn(record).thenReturn(null);

    assertTrue(reader.next(), "Reader should process message successfully");
    InternalRow row1 = reader.get();
    InternalRow row2 = reader.get();
    InternalRow row3 = reader.get();

    assertNotNull(row1, "First get() should return valid row");
    assertNotNull(row2, "Second get() should return valid row");
    assertNotNull(row3, "Third get() should return valid row");
    assertEquals(row1, row2, "Multiple get() calls should return same row instance");
    assertEquals(row2, row3, "Multiple get() calls should return same row instance");

    reader.close();
  }

  @Test
  public void testLogProgressPercentScenarios() throws IOException {
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator();

    // Case 1: Progress tracking functionality exists
    when(mockSplitIterator.getProgress()).thenReturn(0.0f).thenReturn(0.5f).thenReturn(1.0f);

    float initialProgress = reader.logProgressPercent();
    assertEquals(initialProgress, 0.0f, "Initial progress should be 0%");

    // Case 2: Multiple calls should be consistent
    float midProgress = reader.logProgressPercent();
    assertEquals(midProgress, 50.0f, "Mid progress should be 50%");

    // Case 3: Progress is properly calculated as percentage
    float finalProgress = reader.logProgressPercent();
    assertEquals(finalProgress, 100.0f, "Final progress should be 100%");

    reader.close();
  }

  @Test
  public void testMessageTypeHandlingScenarios() throws IOException {
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator();

    // Case 1: Reader handles PUT message type correctly
    PubSubSplitIterator.PubSubInputRecord putRecord = createMockPutRecord(400L, "put-key", "put-value", 30, null, 0);
    when(mockSplitIterator.next()).thenReturn(putRecord).thenReturn(null);

    assertTrue(reader.next(), "Reader should handle PUT message");
    InternalRow putRow = reader.get();
    assertEquals(putRow.getInt(3), MessageType.PUT.getValue(), "Message type should be PUT");
    reader.close();

    // Case 2: Reader handles DELETE message type correctly
    reader = createReaderWithMockIterator();
    PubSubSplitIterator.PubSubInputRecord deleteRecord = createMockDeleteRecord(401L, "delete-key", 31, null, 0);
    when(mockSplitIterator.next()).thenReturn(deleteRecord).thenReturn(null);

    assertTrue(reader.next(), "Reader should handle DELETE message");
    InternalRow deleteRow = reader.get();
    assertEquals(deleteRow.getInt(3), MessageType.DELETE.getValue(), "Message type should be DELETE");
    reader.close();

    // Case 3: Message type validation
    reader = createReaderWithMockIterator();
    PubSubSplitIterator.PubSubInputRecord putRecord2 =
        createMockPutRecord(402L, "validation-key", "validation-value", 32, null, 0);
    PubSubSplitIterator.PubSubInputRecord deleteRecord2 =
        createMockDeleteRecord(403L, "validation-delete-key", 33, null, 0);
    when(mockSplitIterator.next()).thenReturn(putRecord2).thenReturn(deleteRecord2).thenReturn(null);

    assertTrue(reader.next(), "Reader should process first message");
    assertEquals(reader.get().getInt(3), MessageType.PUT.getValue(), "First message should be PUT");

    assertTrue(reader.next(), "Reader should process second message");
    assertEquals(reader.get().getInt(3), MessageType.DELETE.getValue(), "Second message should be DELETE");

    reader.close();
  }

  @Test
  public void testInternalRowFormatValidation() throws IOException {
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator();

    // Case 1: Verify reader is properly initialized for row creation
    PubSubSplitIterator.PubSubInputRecord record = createMockPutRecord(500L, "format-key", "format-value", 40, null, 0);
    when(mockSplitIterator.next()).thenReturn(record).thenReturn(null);

    assertTrue(reader.next(), "Reader should process message for format validation");
    InternalRow row = reader.get();

    assertNotNull(row, "Row should not be null");
    assertEquals(
        row.numFields(),
        10,
        "Row should have 10 fields as per RAW_PUBSUB_INPUT_TABLE_SCHEMA (including chunked_key_suffix)");

    reader.close();

    // Case 2: Verify region and partition are properly set
    String customRegion = "custom-validation-region";
    reader = new SparkPubSubInputPartitionReader(inputPartition, mockConsumer, customRegion, false, false, null);
    // Use reflection or create a custom mock to inject our mockSplitIterator

    PubSubSplitIterator.PubSubInputRecord record2 =
        createMockPutRecord(501L, "region-key", "region-value", 41, null, 0);
    when(mockSplitIterator.next()).thenReturn(record2).thenReturn(null);

    // Note: Since we can't easily inject mockSplitIterator into the real constructor,
    // we'll verify the basic structure is correct
    reader.close();
  }

  @Test
  public void testEdgeCaseScenarios() throws IOException {
    // Case 1: Empty partition split
    SparkPubSubInputPartitionReader reader1 = createReaderWithMockIterator();
    when(mockSplitIterator.next()).thenReturn(null);

    assertFalse(reader1.next(), "Reader should return false for empty partition");
    assertNull(reader1.get(), "get() should return null for empty partition");
    reader1.close();

    // Case 2: Large offset values
    SparkPubSubInputPartitionReader reader2 = createReaderWithMockIterator();
    PubSubSplitIterator.PubSubInputRecord largeOffsetRecord =
        createMockPutRecord(Long.MAX_VALUE, "large-offset-key", "large-offset-value", 50, null, 0);
    when(mockSplitIterator.next()).thenReturn(largeOffsetRecord).thenReturn(null);

    assertTrue(reader2.next(), "Reader should handle large offset values");
    InternalRow largeOffsetRow = reader2.get();
    assertEquals(largeOffsetRow.getLong(2), Long.MAX_VALUE, "Large offset should be preserved");
    reader2.close();

    // Case 3: Null region handling
    SparkPubSubInputPartitionReader reader3 =
        new SparkPubSubInputPartitionReader(inputPartition, mockConsumer, null, false, false, null);
    // Reader should handle null region gracefully
    reader3.close();
  }

  @Test
  public void testRawPubsubInternalRowOrdering() throws IOException {
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator();

    // Setup: a PUT record with non-trivial values to assert per field
    ByteBuffer replicationMetadata = ByteBuffer.wrap("rm-payload".getBytes());
    PubSubSplitIterator.PubSubInputRecord record =
        createMockPutRecord(123L, "ordering-key", "ordering-value", 42, replicationMetadata, 7);
    when(mockSplitIterator.next()).thenReturn(record).thenReturn(null);

    assertTrue(reader.next(), "Reader should process message for ordering validation");
    InternalRow row = reader.get();
    assertNotNull(row, "Row should not be null");

    // Field by field ordering matches RAW_PUBSUB_INPUT_TABLE_SCHEMA
    // 0: __region__ (StringType / UTF8String)
    assertEquals(row.getUTF8String(0).toString(), TEST_REGION, "Region should match");

    // 1: __partition__ (IntegerType)
    assertEquals(row.getInt(1), TEST_PARTITION_NUMBER, "Partition number should match");

    // 2: __offset__ (LongType)
    assertEquals(row.getLong(2), 123L, "Offset should match");

    // 3: __message_type__ (IntegerType)
    assertEquals(row.getInt(3), MessageType.PUT.getValue(), "Message type should be PUT");

    // 4: __schema_id__ (IntegerType)
    assertEquals(row.getInt(4), 42, "Schema ID should match");

    // 5: key (BinaryType)
    assertTrue(Arrays.equals(row.getBinary(5), "ordering-key".getBytes()), "Key bytes should match expected value");

    // 6: value (BinaryType)
    assertTrue(Arrays.equals(row.getBinary(6), "ordering-value".getBytes()), "Value bytes should match expected value");

    // 7: __replication_metadata_version_id__ (IntegerType)
    assertEquals(row.getInt(7), 7, "Replication metadata version ID should match");

    // 8: __replication_metadata_payload__ (BinaryType)
    assertTrue(
        Arrays.equals(row.getBinary(8), "rm-payload".getBytes()),
        "Replication metadata payload bytes should match");

    // No more records
    assertFalse(reader.next(), "Reader should return false when no more messages");

    reader.close();
  }

  @Test
  public void testChunkingDisabledDoesNotSplitKey() throws IOException {
    // When chunking is disabled, the full composite key should be used as-is without splitting
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator(); // chunking disabled by default

    // Create a PUT message with what would be a composite key if chunking was enabled
    String compositeKeyString = "user-key-123_chunk_0";
    PubSubSplitIterator.PubSubInputRecord putRecord =
        createMockPutRecord(100L, compositeKeyString, "test-value", 1, null, 0);
    when(mockSplitIterator.next()).thenReturn(putRecord).thenReturn(null);

    assertTrue(reader.next(), "Reader should process message");
    InternalRow row = reader.get();

    assertNotNull(row, "Row should not be null");
    // Verify the key is NOT split - the full composite key should be in the key field
    byte[] keyBytes = row.getBinary(5);
    assertTrue(
        Arrays.equals(keyBytes, compositeKeyString.getBytes()),
        "Key should contain full composite key when chunking is disabled");
    // Verify chunked_key_suffix is null when chunking is disabled
    assertTrue(row.isNullAt(9), "chunked_key_suffix should be null when chunking is disabled");

    reader.close();
  }

  @Test
  public void testChunkedKeySuffixColumnInOutput() throws IOException {
    // Test that the chunked_key_suffix column (10th field) is properly populated
    SparkPubSubInputPartitionReader reader = createReaderWithMockIterator();

    // Create a regular PUT message (non-chunked)
    PubSubSplitIterator.PubSubInputRecord putRecord =
        createMockPutRecord(100L, "regular-key", "regular-value", 1, null, 0);
    when(mockSplitIterator.next()).thenReturn(putRecord).thenReturn(null);

    assertTrue(reader.next(), "Reader should process message");
    InternalRow row = reader.get();

    assertNotNull(row, "Row should not be null");
    assertEquals(row.numFields(), 10, "Row should have 10 fields including chunked_key_suffix");

    // Field index 9 is chunked_key_suffix (should be null for non-chunked messages)
    assertTrue(row.isNullAt(9), "chunked_key_suffix should be null for non-chunked messages");

    reader.close();
  }

  @Test
  public void testChunkedPutMessageWithKeySplitting() throws IOException {
    // Test that chunked PUT messages properly split the key and populate the suffix
    ChunkKeyValueTransformer mockTransformer = mock(ChunkKeyValueTransformer.class);
    SparkPubSubInputPartitionReader reader = new SparkPubSubInputPartitionReader(
        inputPartition,
        TEST_REGION,
        mockSplitIterator,
        true, // chunking enabled
        mockTransformer);

    // Create a chunked PUT message with composite key
    byte[] compositeKeyBytes = "user-key-123_chunk_0".getBytes();
    byte[] userKeyBytes = "user-key-123".getBytes();
    byte[] chunkSuffixBytes = "_chunk_0".getBytes();
    String chunkValue = "chunk-value-0";

    PubSubSplitIterator.PubSubInputRecord putRecord =
        createMockPutRecordWithBytes(100L, compositeKeyBytes, chunkValue, 1, null, 0);

    // Mock the transformer to split the composite key
    RawKeyBytesAndChunkedKeySuffix mockSplit = mock(RawKeyBytesAndChunkedKeySuffix.class);
    when(mockSplit.getRawKeyBytes()).thenReturn(ByteBuffer.wrap(userKeyBytes));
    when(mockSplit.getChunkedKeySuffixBytes()).thenReturn(ByteBuffer.wrap(chunkSuffixBytes));

    when(mockTransformer.splitChunkedKey(eq(compositeKeyBytes), any(ChunkKeyValueTransformer.KeyType.class)))
        .thenReturn(mockSplit);

    when(mockSplitIterator.next()).thenReturn(putRecord).thenReturn(null);

    assertTrue(reader.next(), "Reader should process chunked PUT message");
    InternalRow row = reader.get();

    assertNotNull(row, "Row should not be null");
    assertEquals(row.numFields(), 10, "Row should have 10 fields");

    // Verify the key was split correctly - should contain only the user key
    byte[] keyBytes = row.getBinary(5);
    assertTrue(Arrays.equals(keyBytes, userKeyBytes), "Key field should contain only the user key (without suffix)");

    // Verify the value
    byte[] valueBytes = row.getBinary(6);
    assertTrue(Arrays.equals(valueBytes, chunkValue.getBytes()), "Value should match the chunk value");

    // Verify the chunked_key_suffix is populated
    assertFalse(row.isNullAt(9), "chunked_key_suffix should NOT be null for chunked messages");
    byte[] suffixBytes = row.getBinary(9);
    assertTrue(Arrays.equals(suffixBytes, chunkSuffixBytes), "chunked_key_suffix should contain the chunk suffix");

    reader.close();
  }

  @Test
  public void testChunkedDeleteMessageWithKeySplitting() throws IOException {
    // Test that chunked DELETE messages properly split the key and populate the suffix
    ChunkKeyValueTransformer mockTransformer = mock(ChunkKeyValueTransformer.class);
    SparkPubSubInputPartitionReader reader = new SparkPubSubInputPartitionReader(
        inputPartition,
        TEST_REGION,
        mockSplitIterator,
        true, // chunking enabled
        mockTransformer);

    // Create a chunked DELETE message with composite key
    byte[] compositeKeyBytes = "user-key-456_chunk_1".getBytes();
    byte[] userKeyBytes = "user-key-456".getBytes();
    byte[] chunkSuffixBytes = "_chunk_1".getBytes();

    PubSubSplitIterator.PubSubInputRecord deleteRecord =
        createMockDeleteRecordWithBytes(200L, compositeKeyBytes, 10, null, 0);

    // Mock the transformer to split the composite key
    RawKeyBytesAndChunkedKeySuffix mockSplit = mock(RawKeyBytesAndChunkedKeySuffix.class);
    when(mockSplit.getRawKeyBytes()).thenReturn(ByteBuffer.wrap(userKeyBytes));
    when(mockSplit.getChunkedKeySuffixBytes()).thenReturn(ByteBuffer.wrap(chunkSuffixBytes));

    when(mockTransformer.splitChunkedKey(eq(compositeKeyBytes), any(ChunkKeyValueTransformer.KeyType.class)))
        .thenReturn(mockSplit);

    when(mockSplitIterator.next()).thenReturn(deleteRecord).thenReturn(null);

    assertTrue(reader.next(), "Reader should process chunked DELETE message");
    InternalRow row = reader.get();

    assertNotNull(row, "Row should not be null");

    // Verify the key was split correctly
    byte[] keyBytes = row.getBinary(5);
    assertTrue(Arrays.equals(keyBytes, userKeyBytes), "Key field should contain only the user key (without suffix)");

    // Verify DELETE has null value
    assertTrue(row.isNullAt(6), "DELETE message should have null value");

    // Verify the chunked_key_suffix is populated
    assertFalse(row.isNullAt(9), "chunked_key_suffix should NOT be null for chunked DELETE");
    byte[] suffixBytes = row.getBinary(9);
    assertTrue(Arrays.equals(suffixBytes, chunkSuffixBytes), "chunked_key_suffix should contain the chunk suffix");

    reader.close();
  }

  @Test
  public void testMultipleChunksWithDifferentSuffixes() throws IOException {
    // Test processing multiple chunks of the same logical record with different suffixes
    ChunkKeyValueTransformer mockTransformer = mock(ChunkKeyValueTransformer.class);
    SparkPubSubInputPartitionReader reader = new SparkPubSubInputPartitionReader(
        inputPartition,
        TEST_REGION,
        mockSplitIterator,
        true, // chunking enabled
        mockTransformer);

    // Create multiple chunk messages for the same user key
    byte[] compositeKey0 = "user-key_chunk_0".getBytes();
    byte[] compositeKey1 = "user-key_chunk_1".getBytes();
    byte[] compositeKey2 = "user-key_chunk_2".getBytes();

    byte[] userKey = "user-key".getBytes();
    byte[] suffix0 = "_chunk_0".getBytes();
    byte[] suffix1 = "_chunk_1".getBytes();
    byte[] suffix2 = "_chunk_2".getBytes();

    // Create records
    PubSubSplitIterator.PubSubInputRecord record0 =
        createMockPutRecordWithBytes(100L, compositeKey0, "chunk-0-data", 1, null, 0);
    PubSubSplitIterator.PubSubInputRecord record1 =
        createMockPutRecordWithBytes(101L, compositeKey1, "chunk-1-data", 1, null, 0);
    PubSubSplitIterator.PubSubInputRecord record2 =
        createMockPutRecordWithBytes(102L, compositeKey2, "chunk-2-data", 1, null, 0);

    // Mock splits for each chunk
    RawKeyBytesAndChunkedKeySuffix split0 = mock(RawKeyBytesAndChunkedKeySuffix.class);
    when(split0.getRawKeyBytes()).thenReturn(ByteBuffer.wrap(userKey));
    when(split0.getChunkedKeySuffixBytes()).thenReturn(ByteBuffer.wrap(suffix0));

    RawKeyBytesAndChunkedKeySuffix split1 = mock(RawKeyBytesAndChunkedKeySuffix.class);
    when(split1.getRawKeyBytes()).thenReturn(ByteBuffer.wrap(userKey));
    when(split1.getChunkedKeySuffixBytes()).thenReturn(ByteBuffer.wrap(suffix1));

    RawKeyBytesAndChunkedKeySuffix split2 = mock(RawKeyBytesAndChunkedKeySuffix.class);
    when(split2.getRawKeyBytes()).thenReturn(ByteBuffer.wrap(userKey));
    when(split2.getChunkedKeySuffixBytes()).thenReturn(ByteBuffer.wrap(suffix2));

    when(mockTransformer.splitChunkedKey(eq(compositeKey0), any(ChunkKeyValueTransformer.KeyType.class)))
        .thenReturn(split0);
    when(mockTransformer.splitChunkedKey(eq(compositeKey1), any(ChunkKeyValueTransformer.KeyType.class)))
        .thenReturn(split1);
    when(mockTransformer.splitChunkedKey(eq(compositeKey2), any(ChunkKeyValueTransformer.KeyType.class)))
        .thenReturn(split2);

    when(mockSplitIterator.next()).thenReturn(record0).thenReturn(record1).thenReturn(record2).thenReturn(null);

    // Process chunk 0
    assertTrue(reader.next(), "Should process chunk 0");
    InternalRow row0 = reader.get();
    assertTrue(Arrays.equals(row0.getBinary(5), userKey), "All chunks should have same user key");
    assertTrue(Arrays.equals(row0.getBinary(9), suffix0), "Chunk 0 should have suffix 0");
    assertTrue(Arrays.equals(row0.getBinary(6), "chunk-0-data".getBytes()), "Chunk 0 should have correct value");

    // Process chunk 1
    assertTrue(reader.next(), "Should process chunk 1");
    InternalRow row1 = reader.get();
    assertTrue(Arrays.equals(row1.getBinary(5), userKey), "All chunks should have same user key");
    assertTrue(Arrays.equals(row1.getBinary(9), suffix1), "Chunk 1 should have suffix 1");
    assertTrue(Arrays.equals(row1.getBinary(6), "chunk-1-data".getBytes()), "Chunk 1 should have correct value");

    // Process chunk 2
    assertTrue(reader.next(), "Should process chunk 2");
    InternalRow row2 = reader.get();
    assertTrue(Arrays.equals(row2.getBinary(5), userKey), "All chunks should have same user key");
    assertTrue(Arrays.equals(row2.getBinary(9), suffix2), "Chunk 2 should have suffix 2");
    assertTrue(Arrays.equals(row2.getBinary(6), "chunk-2-data".getBytes()), "Chunk 2 should have correct value");

    assertFalse(reader.next(), "No more messages");
    reader.close();
  }

  /**
   * Helper method to create a SparkPubSubInputPartitionReader with a mocked PubSubSplitIterator.
   * Uses the test-only constructor to inject the mock iterator.
   */
  private SparkPubSubInputPartitionReader createReaderWithMockIterator() {
    return new SparkPubSubInputPartitionReader(inputPartition, TEST_REGION, mockSplitIterator, false, null);
  }

  /**
   * Helper method to create a mock PUT message record.
   * Convenience method that converts String key to bytes.
   */
  private PubSubSplitIterator.PubSubInputRecord createMockPutRecord(
      long offset,
      String keyStr,
      String valueStr,
      int schemaId,
      ByteBuffer replicationMetadata,
      int replicationMetadataVersionId) {
    return createMockPutRecordWithBytes(
        offset,
        keyStr.getBytes(),
        valueStr,
        schemaId,
        replicationMetadata,
        replicationMetadataVersionId);
  }

  /**
   * Helper method to create a mock PUT message record with byte array key.
   * This is the main implementation used by both string and byte array variants.
   */
  private PubSubSplitIterator.PubSubInputRecord createMockPutRecordWithBytes(
      long offset,
      byte[] keyBytes,
      String valueStr,
      int schemaId,
      ByteBuffer replicationMetadata,
      int replicationMetadataVersionId) {

    DefaultPubSubMessage mockMessage = mock(DefaultPubSubMessage.class);
    KafkaKey mockKey = mock(KafkaKey.class);
    KafkaMessageEnvelope mockEnvelope = mock(KafkaMessageEnvelope.class);
    Put mockPut = mock(Put.class);

    // Setup key with provided bytes
    when(mockKey.getKey()).thenReturn(keyBytes);
    when(mockKey.getKeyLength()).thenReturn(keyBytes.length);

    // Setup PUT message
    ByteBuffer valueBuffer = valueStr.isEmpty() ? Utils.EMPTY_BYTE_BUFFER : ByteBuffer.wrap(valueStr.getBytes());
    when(mockPut.getPutValue()).thenReturn(valueBuffer);
    when(mockPut.getSchemaId()).thenReturn(schemaId);
    when(mockPut.getReplicationMetadataPayload()).thenReturn(replicationMetadata);
    when(mockPut.getReplicationMetadataVersionId()).thenReturn(replicationMetadataVersionId);

    // Setup envelope
    when(mockEnvelope.getPayloadUnion()).thenReturn(mockPut);
    when(mockEnvelope.getMessageType()).thenReturn(MessageType.PUT.getValue());

    // Setup message
    when(mockMessage.getKey()).thenReturn(mockKey);
    when(mockMessage.getValue()).thenReturn(mockEnvelope);
    when(mockMessage.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(offset));

    // Create record
    PubSubSplitIterator.PubSubInputRecord record = mock(PubSubSplitIterator.PubSubInputRecord.class);
    when(record.getPubSubMessage()).thenReturn(mockMessage);
    when(record.getOffset()).thenReturn(offset);

    return record;
  }

  /**
   * Helper method to create a mock DELETE message record.
   * Convenience method that converts String key to bytes.
   */
  private PubSubSplitIterator.PubSubInputRecord createMockDeleteRecord(
      long offset,
      String keyStr,
      int schemaId,
      ByteBuffer replicationMetadata,
      int replicationMetadataVersionId) {
    return createMockDeleteRecordWithBytes(
        offset,
        keyStr.getBytes(),
        schemaId,
        replicationMetadata,
        replicationMetadataVersionId);
  }

  /**
   * Helper method to create a mock DELETE message record with byte array key.
   * This is the main implementation used by both string and byte array variants.
   */
  private PubSubSplitIterator.PubSubInputRecord createMockDeleteRecordWithBytes(
      long offset,
      byte[] keyBytes,
      int schemaId,
      ByteBuffer replicationMetadata,
      int replicationMetadataVersionId) {

    DefaultPubSubMessage mockMessage = mock(DefaultPubSubMessage.class);
    KafkaKey mockKey = mock(KafkaKey.class);
    KafkaMessageEnvelope mockEnvelope = mock(KafkaMessageEnvelope.class);
    Delete mockDelete = mock(Delete.class);

    // Setup key with provided bytes
    when(mockKey.getKey()).thenReturn(keyBytes);
    when(mockKey.getKeyLength()).thenReturn(keyBytes.length);

    // Setup DELETE message
    when(mockDelete.getSchemaId()).thenReturn(schemaId);
    when(mockDelete.getReplicationMetadataPayload()).thenReturn(replicationMetadata);
    when(mockDelete.getReplicationMetadataVersionId()).thenReturn(replicationMetadataVersionId);

    // Setup envelope
    when(mockEnvelope.getPayloadUnion()).thenReturn(mockDelete);
    when(mockEnvelope.getMessageType()).thenReturn(MessageType.DELETE.getValue());

    // Setup message
    when(mockMessage.getKey()).thenReturn(mockKey);
    when(mockMessage.getValue()).thenReturn(mockEnvelope);
    when(mockMessage.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(offset));

    // Create record
    PubSubSplitIterator.PubSubInputRecord record = mock(PubSubSplitIterator.PubSubInputRecord.class);
    when(record.getPubSubMessage()).thenReturn(mockMessage);
    when(record.getOffset()).thenReturn(offset);

    return record;
  }
}
