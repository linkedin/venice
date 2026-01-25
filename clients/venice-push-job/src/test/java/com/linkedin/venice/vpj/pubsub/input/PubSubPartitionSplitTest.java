package com.linkedin.venice.vpj.pubsub.input;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PubSubPartitionSplitTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private PubSubTopic testTopic;
  private PubSubTopicPartition testPartition;

  @BeforeMethod
  public void setUp() {
    testTopic = TOPIC_REPOSITORY.getTopic("test-topic_v1");
    testPartition = new PubSubTopicPartitionImpl(testTopic, 0);
  }

  @Test
  public void testBasicConstructorAndGetters() {
    // Case 1: Standard range split construction - splitIndex = 0
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(200);
    long recordCount = 100;
    int splitIndex = 0;
    long startOffset = recordCount * splitIndex;

    PubSubPartitionSplit split = createSplit(testPartition, startPos, endPos, recordCount, splitIndex, startOffset);

    assertEquals(split.getPubSubTopicPartition(), testPartition);
    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getNumberOfRecords(), recordCount);
    assertEquals(split.getSplitIndex(), splitIndex);
    assertEquals(split.getStartIndex(), startOffset);
    assertEquals(split.getStartIndex(), recordCount * splitIndex);

    // Case 2: Single record range - splitIndex = 5
    startPos = ApacheKafkaOffsetPosition.of(50);
    endPos = ApacheKafkaOffsetPosition.of(51);
    recordCount = 1;
    splitIndex = 5;
    startOffset = recordCount * splitIndex; // 1000 * 5 = 5000

    split = createSplit(testPartition, startPos, endPos, recordCount, splitIndex, startOffset);

    assertEquals(split.getPubSubTopicPartition(), testPartition);
    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getNumberOfRecords(), recordCount);
    assertEquals(split.getSplitIndex(), splitIndex);
    assertEquals(split.getStartIndex(), startOffset);
    assertEquals(split.getStartIndex(), recordCount * splitIndex);

    // Case 3: Large record count with high range index - splitIndex = 99
    startPos = ApacheKafkaOffsetPosition.of(1000000);
    endPos = ApacheKafkaOffsetPosition.of(2000000);
    recordCount = 1000000L;
    splitIndex = 99;
    startOffset = recordCount * splitIndex; // 1000 * 99 = 99000

    split = createSplit(testPartition, startPos, endPos, recordCount, splitIndex, startOffset);

    assertEquals(split.getPubSubTopicPartition(), testPartition);
    assertEquals(split.getStartPubSubPosition(), startPos);
    assertEquals(split.getEndPubSubPosition(), endPos);
    assertEquals(split.getNumberOfRecords(), recordCount);
    assertEquals(split.getSplitIndex(), splitIndex);
    assertEquals(split.getStartIndex(), startOffset);
    assertEquals(split.getStartIndex(), recordCount * splitIndex);
  }

  @Test
  public void testStartOffsetFormula() {
    // Test the formula: startOffset = maxRecordPerSplit * splitIndex
    long maxRecordPerSplit = 500L;

    // Case 1: splitIndex = 0
    int splitIndex = 0;
    long expectedStartOffset = maxRecordPerSplit * splitIndex; // 500 * 0 = 0
    PubSubPartitionSplit split = createSplit(
        testPartition,
        ApacheKafkaOffsetPosition.of(0),
        ApacheKafkaOffsetPosition.of(500),
        500,
        splitIndex,
        expectedStartOffset);
    assertEquals(split.getStartIndex(), expectedStartOffset);
    assertEquals(split.getStartIndex(), maxRecordPerSplit * splitIndex);

    // Case 2: splitIndex = 1
    splitIndex = 1;
    expectedStartOffset = maxRecordPerSplit * splitIndex; // 500 * 1 = 500
    split = createSplit(
        testPartition,
        ApacheKafkaOffsetPosition.of(500),
        ApacheKafkaOffsetPosition.of(1000),
        500,
        splitIndex,
        expectedStartOffset);
    assertEquals(split.getStartIndex(), expectedStartOffset);
    assertEquals(split.getStartIndex(), maxRecordPerSplit * splitIndex);

    // Case 3: splitIndex = 10
    splitIndex = 10;
    expectedStartOffset = maxRecordPerSplit * splitIndex; // 500 * 10 = 5000
    split = createSplit(
        testPartition,
        ApacheKafkaOffsetPosition.of(5000),
        ApacheKafkaOffsetPosition.of(5500),
        500,
        splitIndex,
        expectedStartOffset);
    assertEquals(split.getStartIndex(), expectedStartOffset);
    assertEquals(split.getStartIndex(), maxRecordPerSplit * splitIndex);

    // Case 4: Different maxRecordPerSplit value
    maxRecordPerSplit = 2000L;
    splitIndex = 3;
    expectedStartOffset = maxRecordPerSplit * splitIndex; // 2000 * 3 = 6000
    split = createSplit(
        testPartition,
        ApacheKafkaOffsetPosition.of(6000),
        ApacheKafkaOffsetPosition.of(8000),
        2000,
        splitIndex,
        expectedStartOffset);
    assertEquals(split.getStartIndex(), expectedStartOffset);
    assertEquals(split.getStartIndex(), maxRecordPerSplit * splitIndex);
  }

  @Test
  public void testStartOffsetScenarios() {
    long maxRecordPerSplit = 1000L;

    // Case 1: Zero start offset (splitIndex = 0)
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(100);
    int splitIndex = 0;
    long startOffset = maxRecordPerSplit * splitIndex; // 1000 * 0 = 0

    PubSubPartitionSplit split = createSplit(testPartition, startPos, endPos, 100, splitIndex, startOffset);
    assertEquals(split.getStartIndex(), 0L);
    assertEquals(split.getStartIndex(), maxRecordPerSplit * splitIndex);

    // Case 2: Large start offset with high split index
    startPos = ApacheKafkaOffsetPosition.of(9999999);
    endPos = ApacheKafkaOffsetPosition.of(10000099);
    splitIndex = 999;
    startOffset = maxRecordPerSplit * splitIndex; // 1000 * 999 = 999000
    split = createSplit(testPartition, startPos, endPos, 100, splitIndex, startOffset);
    assertEquals(split.getStartIndex(), 999000L);
    assertEquals(split.getStartIndex(), maxRecordPerSplit * splitIndex);

    // Case 3: Mid-range split index
    startPos = ApacheKafkaOffsetPosition.of(5000);
    endPos = ApacheKafkaOffsetPosition.of(6000);
    splitIndex = 25;
    startOffset = maxRecordPerSplit * splitIndex; // 1000 * 25 = 25000
    split = createSplit(testPartition, startPos, endPos, 1000, splitIndex, startOffset);
    assertEquals(split.getStartIndex(), 25000L);
    assertEquals(split.getStartIndex(), maxRecordPerSplit * splitIndex);
  }

  @Test
  public void testEmptyConstructorInitialization() {
    // Test that the nullary constructor properly initializes topicRepository
    PubSubPartitionSplit split = new PubSubPartitionSplit();
    assertNotNull(split.getTopicRepository());

    // Verify that calling getTopicRepository multiple times returns the same instance
    PubSubTopicRepository repo1 = split.getTopicRepository();
    PubSubTopicRepository repo2 = split.getTopicRepository();
    assertEquals(repo1, repo2);
  }

  @Test
  public void testSerializationWithEdgeCaseValues() throws IOException {
    // Case 1: Minimum possible values
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(0);
    PubSubPartitionSplit originalSplit = createSplit(testPartition, startPos, endPos, 0, 0, 0);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    originalSplit.writeTo(dos);
    dos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    PubSubPartitionSplit deserializedSplit = PubSubPartitionSplit.readFrom(dis);
    dis.close();

    assertEquals(deserializedSplit, originalSplit);
    assertEquals(deserializedSplit.getNumberOfRecords(), 0L);
    assertEquals(deserializedSplit.getSplitIndex(), 0);
    assertEquals(deserializedSplit.getStartIndex(), 0L);

    // Case 2: Maximum reasonable values
    startPos = ApacheKafkaOffsetPosition.of(Long.MAX_VALUE - 1);
    endPos = ApacheKafkaOffsetPosition.of(Long.MAX_VALUE);
    originalSplit = createSplit(testPartition, startPos, endPos, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);

    baos = new ByteArrayOutputStream();
    dos = new DataOutputStream(baos);
    originalSplit.writeTo(dos);
    dos.close();

    bais = new ByteArrayInputStream(baos.toByteArray());
    dis = new DataInputStream(bais);
    deserializedSplit = PubSubPartitionSplit.readFrom(dis);
    dis.close();

    assertEquals(deserializedSplit, originalSplit);
    assertEquals(deserializedSplit.getNumberOfRecords(), Long.MAX_VALUE);
    assertEquals(deserializedSplit.getSplitIndex(), Integer.MAX_VALUE);
    assertEquals(deserializedSplit.getStartIndex(), Long.MAX_VALUE);

    // Case 3: Topic with special characters and high partition number
    PubSubTopic specialTopic = TOPIC_REPOSITORY.getTopic("test-topic_with_special-chars_and.dots_v999");
    PubSubTopicPartition specialPartition = new PubSubTopicPartitionImpl(specialTopic, 999);
    originalSplit = createSplit(specialPartition, startPos, endPos, 12345, 50, 123450);

    baos = new ByteArrayOutputStream();
    dos = new DataOutputStream(baos);
    originalSplit.writeTo(dos);
    dos.close();

    bais = new ByteArrayInputStream(baos.toByteArray());
    dis = new DataInputStream(bais);
    deserializedSplit = PubSubPartitionSplit.readFrom(dis);
    dis.close();

    assertEquals(deserializedSplit, originalSplit);
    assertEquals(deserializedSplit.getTopicName(), "test-topic_with_special-chars_and.dots_v999");
    assertEquals(deserializedSplit.getPartitionNumber(), 999);
  }

  @Test
  public void testDeserializationWithMalformedData() throws IOException {
    // Case 1: Test with truncated data (missing bytes)
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(200);
    PubSubPartitionSplit originalSplit = createSplit(testPartition, startPos, endPos, 100, 5, 500);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    originalSplit.writeTo(dos);
    dos.close();

    byte[] completeData = baos.toByteArray();

    // Try to read from truncated data (only first half)
    byte[] truncatedData = new byte[completeData.length / 2];
    System.arraycopy(completeData, 0, truncatedData, 0, truncatedData.length);

    ByteArrayInputStream bais = new ByteArrayInputStream(truncatedData);
    DataInputStream dis = new DataInputStream(bais);
    PubSubPartitionSplit split = new PubSubPartitionSplit();

    try {
      split.readFields(dis);
      fail("Expected IOException for truncated data");
    } catch (IOException e) {
      // Expected exception due to truncated data
      assertTrue(e.getMessage() != null || e instanceof java.io.EOFException);
    } finally {
      dis.close();
    }

    // Case 2: Test with invalid UTF-8 string length (negative length)
    baos = new ByteArrayOutputStream();
    dos = new DataOutputStream(baos);
    dos.writeInt(5); // splitIndex
    dos.writeLong(500L); // startIndex
    dos.writeInt(-1); // Invalid negative length for topic name
    dos.close();

    bais = new ByteArrayInputStream(baos.toByteArray());
    dis = new DataInputStream(bais);
    split = new PubSubPartitionSplit();

    try {
      split.readFields(dis);
      fail("Expected IOException for negative string length");
    } catch (IOException e) {
      // Expected exception due to invalid string length
      assertNotNull(e);
    } finally {
      dis.close();
    }

    // Case 3: Test with invalid byte array length (negative length)
    baos = new ByteArrayOutputStream();
    dos = new DataOutputStream(baos);
    dos.writeInt(5); // splitIndex
    dos.writeLong(500L); // startIndex
    dos.writeUTF("test-topic_v1"); // topicName
    dos.writeInt(0); // partitionNumber
    dos.writeLong(100L); // numberOfRecords
    dos.writeInt(-5); // Invalid negative length for startBytes
    dos.close();

    bais = new ByteArrayInputStream(baos.toByteArray());
    dis = new DataInputStream(bais);
    split = new PubSubPartitionSplit();

    try {
      split.readFields(dis);
      fail("Expected exception for negative byte array length");
    } catch (Exception e) {
      // Expected exception due to invalid byte array length
      assertNotNull(e);
    } finally {
      dis.close();
    }
  }

  @Test
  public void testDeserializationWithLargeByteArrays() throws IOException {
    // Test behavior with very large byte arrays (but not unreasonably large)
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(1000);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(2000);
    PubSubPartitionSplit originalSplit = createSplit(testPartition, startPos, endPos, 1000, 10, 10000);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    originalSplit.writeTo(dos);
    dos.close();

    // Verify the serialized data can be properly deserialized
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    PubSubPartitionSplit deserializedSplit = PubSubPartitionSplit.readFrom(dis);
    dis.close();

    assertEquals(deserializedSplit, originalSplit);

    // Verify the byte arrays are properly sized (not empty but not unreasonably large)
    assertTrue(baos.toByteArray().length > 50); // Reasonable minimum size
    assertTrue(baos.toByteArray().length < 10000); // Reasonable maximum size for test data
  }

  @Test
  public void testSerializationConsistencyAcrossInstances() throws IOException {
    // Test that identical splits produce identical serialized data
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(300);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(400);

    PubSubPartitionSplit split1 = createSplit(testPartition, startPos, endPos, 100, 3, 300);
    PubSubPartitionSplit split2 = createSplit(testPartition, startPos, endPos, 100, 3, 300);

    // Verify splits are equal before serialization
    assertEquals(split1, split2);
    assertEquals(split1.hashCode(), split2.hashCode());

    // Serialize both splits
    ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
    DataOutputStream dos1 = new DataOutputStream(baos1);
    split1.writeTo(dos1);
    dos1.close();

    ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    DataOutputStream dos2 = new DataOutputStream(baos2);
    split2.writeTo(dos2);
    dos2.close();

    // Verify serialized data is identical
    byte[] data1 = baos1.toByteArray();
    byte[] data2 = baos2.toByteArray();
    assertEquals(data1.length, data2.length);
    for (int i = 0; i < data1.length; i++) {
      assertEquals(data1[i], data2[i], "Byte arrays should be identical at position " + i);
    }

    // Deserialize both and verify they remain equal
    ByteArrayInputStream bais1 = new ByteArrayInputStream(data1);
    DataInputStream dis1 = new DataInputStream(bais1);
    PubSubPartitionSplit deserialized1 = PubSubPartitionSplit.readFrom(dis1);
    dis1.close();

    ByteArrayInputStream bais2 = new ByteArrayInputStream(data2);
    DataInputStream dis2 = new DataInputStream(bais2);
    PubSubPartitionSplit deserialized2 = PubSubPartitionSplit.readFrom(dis2);
    dis2.close();

    assertEquals(deserialized1, deserialized2);
    assertEquals(deserialized1.hashCode(), deserialized2.hashCode());
  }

  @Test
  public void testToStringAfterSerialization() throws IOException {
    // Test that toString works correctly after serialization/deserialization
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(777);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(888);
    PubSubPartitionSplit originalSplit = createSplit(testPartition, startPos, endPos, 111, 22, 2222);

    String originalToString = originalSplit.toString();
    assertNotNull(originalToString);
    assertTrue(originalToString.contains("test-topic_v1"));
    assertTrue(originalToString.contains("22")); // splitIndex
    assertTrue(originalToString.contains("111")); // numberOfRecords

    // Serialize and deserialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    originalSplit.writeTo(dos);
    dos.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    PubSubPartitionSplit deserializedSplit = PubSubPartitionSplit.readFrom(dis);
    dis.close();

    String deserializedToString = deserializedSplit.toString();
    assertNotNull(deserializedToString);
    assertEquals(deserializedToString, originalToString);

    // Verify specific components are present in toString
    assertTrue(deserializedToString.contains("test-topic_v1"));
    assertTrue(deserializedToString.contains("22")); // splitIndex
    assertTrue(deserializedToString.contains("111")); // numberOfRecords
    assertTrue(deserializedToString.contains("2222")); // startIndex
  }

  private PubSubPartitionSplit createSplit(
      PubSubTopicPartition partition,
      PubSubPosition startPos,
      PubSubPosition endPos,
      long recordCount,
      int splitIndex,
      long startOffset) {
    return new PubSubPartitionSplit(
        TOPIC_REPOSITORY,
        partition,
        startPos,
        endPos,
        recordCount,
        splitIndex,
        startOffset);
  }
}
