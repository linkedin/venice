package com.linkedin.venice.hadoop.input.kafka.chunk;

import static com.linkedin.venice.hadoop.input.kafka.chunk.TestChunkingUtils.createChunkBytes;
import static com.linkedin.venice.hadoop.input.kafka.chunk.TestChunkingUtils.createChunkedKeySuffix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedKeySuffixSerializer;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestChunkAssembler {
  private final ChunkAssembler chunkAssembler = new ChunkAssembler(false);
  private final ChunkAssembler rmdChunkingEnabledChunkAssembler = new ChunkAssembler(true);
  private static final int CHUNK_MANIFEST_SCHEMA_ID =
      AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
  private static final int CHUNK_VALUE_SCHEMA_ID = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
  private static final int VALUE_SCHEMA_ID = 1234;
  private static final int VALUE_SCHEMA_ID_2 = 2234;
  private static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER =
      new ChunkedValueManifestSerializer(true);
  private static final ChunkedKeySuffixSerializer CHUNKED_KEY_SUFFIX_SERIALIZER = new ChunkedKeySuffixSerializer();
  private static final RecordDeserializer<KafkaInputMapperValue> KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(KafkaInputMapperValue.SCHEMA$, KafkaInputMapperValue.class);
  private static final RecordSerializer<KafkaInputMapperValue> KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);

  // E.g. chunk_0, chunk_1, … chunk_N, chunk_manifest
  @Test
  public void testAssembleOneCompleteLargeValue() {
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    final byte[] serializedKey = createChunkBytes(0, 5);

    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    List<BytesWritable> values = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo,
        null,
        segmentNumber,
        messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(0, totalChunkCount * eachCountSizeInBytes));
    Assert.assertEquals(assembledValue.getReplicationMetadataVersionId(), 1);
  }

  // E.g. value_chunk_0, value_chunk_1, ..., value_chunk_N, rmd_chunk_0, rmd_chunk1, ..., rmd_chunk_M, chunk_manifest
  @Test
  public void testAssembleOneCompleteLargeValueWithRmdChunking() {
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    ChunkInfo valueChunkInfo = new ChunkInfo(10, 20);
    ChunkInfo rmdChunkInfo = new ChunkInfo(20, 10);
    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo,
        rmdChunkInfo,
        segmentNumber,
        messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        rmdChunkingEnabledChunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID);
    Assert.assertEquals(
        assembledValue.getBytes(),
        createChunkBytes(0, valueChunkInfo.totalChunkCount * valueChunkInfo.eachCountSizeInBytes));
    Assert.assertEquals(assembledValue.getReplicationMetadataVersionId(), 1);
  }

  // E.g. chunk_0, chunk_1, … chunk_N (no manifest)
  @Test
  public void testNoCompleteLargeValueWithMissingManifest() {
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo,
        null,
        segmentNumber,
        messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    values.remove(0); // Remove the first value which should be a manifest

    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());
    Assert.assertNull(assembledValue);
  }

  // E.g. chunk_0, chunk_1, … chunk_N, chunk_N + 2, ... chunk_manifest (missing one chunk "chunk_N + 1")
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Cannot assemble a large value. Missing .*")
  public void testNoCompleteLargeValueWithMissingChunk() {
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo,
        null,
        segmentNumber,
        messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    int indexOfMissingChunk = ThreadLocalRandom.current().nextInt(values.size() - 2) + 1;
    values.remove(indexOfMissingChunk); // Remove a chunk
    chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());
  }

  // E.g. chunk_A_0, chunk_A_1, … chunk_A_N, chunk_A_manifest, chunk_B_0, chunk_B_1 ... chunk_B_M (no manifest for large
  // value B)
  @Test
  public void testOneCompleteLargeValueAndOneIncompleteLargeValue() {
    final int totalChunkCount1 = 10;
    final int eachCountSizeInBytes1 = 20;

    final int totalChunkCount2 = 20;
    final int eachCountSizeInBytes2 = 15;

    final ChunkId chunkId1 = new ChunkId();
    chunkId1.segmentNumber = 12;
    chunkId1.messageSequenceNumber = 34;

    final ChunkId chunkId2 = new ChunkId();
    chunkId2.segmentNumber = 22;
    chunkId2.messageSequenceNumber = 54;
    ChunkInfo valueChunkInfo1 = new ChunkInfo(totalChunkCount1, eachCountSizeInBytes1);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values1 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo1,
        null,
        chunkId1.segmentNumber,
        chunkId1.messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    ChunkInfo valueChunkInfo2 = new ChunkInfo(totalChunkCount2, eachCountSizeInBytes2);
    List<BytesWritable> values2 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo2,
        null,
        chunkId2.segmentNumber,
        chunkId2.messageSequenceNumber,
        VALUE_SCHEMA_ID_2,
        totalChunkCount1 + 1);

    values2.remove(0); // Remove the manifest from the second sequence
    List<BytesWritable> allValues = new ArrayList<>();
    allValues.addAll(values2);
    allValues.addAll(values1);

    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, allValues.iterator());

    // Large value A still gets assembled
    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(0, totalChunkCount1 * eachCountSizeInBytes1));
    Assert.assertEquals(assembledValue.getReplicationMetadataVersionId(), 1);
  }

  // E.g. chunk_A_0, chunk_A_1, … chunk_A_N, chunk_A_manifest, chunk_B_0, chunk_B_2 ... chunk_B_manifest (missing chunk
  // in large value B)
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Cannot assemble a large value. Missing .*")
  public void testOneCompleteLargeValueAndOneIncompleteLargeValueCase2() {
    final int totalChunkCount1 = 10;
    final int eachCountSizeInBytes1 = 20;

    final int totalChunkCount2 = 20;
    final int eachCountSizeInBytes2 = 15;

    final ChunkId chunkId1 = new ChunkId();
    chunkId1.segmentNumber = 12;
    chunkId1.messageSequenceNumber = 34;

    final ChunkId chunkId2 = new ChunkId();
    chunkId2.segmentNumber = 22;
    chunkId2.messageSequenceNumber = 54;
    ChunkInfo valueChunkInfo1 = new ChunkInfo(totalChunkCount1, eachCountSizeInBytes1);
    ChunkInfo valueChunkInfo2 = new ChunkInfo(totalChunkCount2, eachCountSizeInBytes2);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values1 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo1,
        null,
        chunkId1.segmentNumber,
        chunkId1.messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);

    List<BytesWritable> values2 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo2,
        null,
        chunkId2.segmentNumber,
        chunkId2.messageSequenceNumber,
        VALUE_SCHEMA_ID_2,
        totalChunkCount1);

    int indexOfMissingChunk = ThreadLocalRandom.current().nextInt(values2.size() - 2) + 1;
    values2.remove(indexOfMissingChunk); // Remove a chunk from the second sequence
    List<BytesWritable> allValues = new ArrayList<>();
    allValues.addAll(values2);
    allValues.addAll(values1);
    chunkAssembler.assembleAndGetValue(serializedKey, allValues.iterator());
  }

  // E.g. chunk_A_0, chunk_A_1, … chunk_A_N, chunk_A_manifest, chunk_B_0, chunk_B_1 ... chunk_B_manifest (2 complete
  // large values)
  @Test
  public void testTwoCompleteLargeValues() {
    final int totalChunkCount1 = 10;
    final int eachCountSizeInBytes1 = 20;

    final int totalChunkCount2 = 20;
    final int eachCountSizeInBytes2 = 15;

    final ChunkId chunkId1 = new ChunkId();
    chunkId1.segmentNumber = 12;
    chunkId1.messageSequenceNumber = 34;

    final ChunkId chunkId2 = new ChunkId();
    chunkId2.segmentNumber = 22;
    chunkId2.messageSequenceNumber = 54; // Fresher large value
    ChunkInfo valueChunkInfo1 = new ChunkInfo(totalChunkCount1, eachCountSizeInBytes1);
    ChunkInfo valueChunkInfo2 = new ChunkInfo(totalChunkCount2, eachCountSizeInBytes2);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values1 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo1,
        null,
        chunkId1.segmentNumber,
        chunkId1.messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);

    List<BytesWritable> values2 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo2,
        null,
        chunkId2.segmentNumber,
        chunkId2.messageSequenceNumber,
        VALUE_SCHEMA_ID_2,
        totalChunkCount1);
    List<BytesWritable> allValues = new ArrayList<>();
    allValues.addAll(values2);
    allValues.addAll(values1);

    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, allValues.iterator());
    // Large value B wins since it comes after large value A
    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID_2);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(0, totalChunkCount2 * eachCountSizeInBytes2));
  }

  // E.g. chunk_A_0, chunk_A_1, … chunk_A_N, chunk_A_manifest, chunk_B_0, chunk_A_1, chunk_B_1 ... chunk_B_manifest
  @Test
  public void testTwoCompleteLargeValuesWithOneDuplicatedChunk() {
    final int totalChunkCount1 = 10;
    final int eachCountSizeInBytes1 = 20;

    final int totalChunkCount2 = 20;
    final int eachCountSizeInBytes2 = 15;

    final ChunkId chunkId1 = new ChunkId();
    chunkId1.segmentNumber = 12;
    chunkId1.messageSequenceNumber = 34;

    final ChunkId chunkId2 = new ChunkId();
    chunkId2.segmentNumber = 22;
    chunkId2.messageSequenceNumber = 54; // Fresher large value
    ChunkInfo valueChunkInfo1 = new ChunkInfo(totalChunkCount1, eachCountSizeInBytes1);
    ChunkInfo valueChunkInfo2 = new ChunkInfo(totalChunkCount2, eachCountSizeInBytes2);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values1 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo1,
        null,
        chunkId1.segmentNumber,
        chunkId1.messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);

    // Simulate a duplicated chunk
    KafkaInputMapperValue randomChunk =
        deserialize(values1.get(ThreadLocalRandom.current().nextInt(values1.size() - 1)));
    KafkaInputMapperValue duplicatedChunk = new KafkaInputMapperValue();
    duplicatedChunk.schemaId = randomChunk.schemaId;
    duplicatedChunk.offset = deserialize(values1.get(values1.size() - 1)).offset + 1;
    duplicatedChunk.valueType = randomChunk.valueType;
    duplicatedChunk.value = randomChunk.value;
    duplicatedChunk.chunkedKeySuffix = randomChunk.chunkedKeySuffix;
    duplicatedChunk.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    values1.add(serialize(duplicatedChunk));

    List<BytesWritable> values2 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo2,
        null,
        chunkId2.segmentNumber,
        chunkId2.messageSequenceNumber,
        VALUE_SCHEMA_ID_2,
        totalChunkCount1 + 1);
    List<BytesWritable> allValues = new ArrayList<>();
    allValues.addAll(values2);
    allValues.addAll(values1);

    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, allValues.iterator());
    // Large value B wins since it comes after large value A and the duplicated chunk does not matter
    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID_2);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(0, totalChunkCount2 * eachCountSizeInBytes2));
  }

  // E.g. chunk_A_0, chunk_A_1, … chunk_A_N, chunk_A_manifest, chunk_A_manifest, chunk_B_0, chunk_B_0, chunk_B_1 ...
  // chunk_B_manifest
  @Test
  public void testTwoCompleteLargeValuesWithOneDuplicatedManifest() {
    final int totalChunkCount1 = 10;
    final int eachCountSizeInBytes1 = 20;

    final int totalChunkCount2 = 20;
    final int eachCountSizeInBytes2 = 15;

    final ChunkId chunkId1 = new ChunkId();
    chunkId1.segmentNumber = 12;
    chunkId1.messageSequenceNumber = 34;

    final ChunkId chunkId2 = new ChunkId();
    chunkId2.segmentNumber = 22;
    chunkId2.messageSequenceNumber = 54; // Fresher large value
    ChunkInfo valueChunkInfo1 = new ChunkInfo(totalChunkCount1, eachCountSizeInBytes1);
    ChunkInfo valueChunkInfo2 = new ChunkInfo(totalChunkCount2, eachCountSizeInBytes2);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values1 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo1,
        null,
        chunkId1.segmentNumber,
        chunkId1.messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);

    // Simulate a duplicated manifest
    KafkaInputMapperValue manifestValue = deserialize(values1.get(values1.size() - 1));
    KafkaInputMapperValue duplicatedManifestValue = new KafkaInputMapperValue();
    duplicatedManifestValue.schemaId = manifestValue.schemaId;
    duplicatedManifestValue.offset = deserialize(values1.get(values1.size() - 1)).offset + 1;
    duplicatedManifestValue.valueType = manifestValue.valueType;
    duplicatedManifestValue.value = manifestValue.value;
    duplicatedManifestValue.chunkedKeySuffix = manifestValue.chunkedKeySuffix;
    duplicatedManifestValue.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    values1.add(serialize(duplicatedManifestValue));

    List<BytesWritable> values2 = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo2,
        null,
        chunkId2.segmentNumber,
        chunkId2.messageSequenceNumber,
        VALUE_SCHEMA_ID_2,
        totalChunkCount1 + 1);
    List<BytesWritable> allValues = new ArrayList<>();
    allValues.addAll(values2);
    allValues.addAll(values1);

    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, allValues.iterator());
    // Large value B wins since it comes after large value A and the duplicated manifest does not matter
    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID_2);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(0, totalChunkCount2 * eachCountSizeInBytes2));
  }

  @Test
  public void testRegularValueAtTheEndWithCompleteLargeValue() {
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo,
        null,
        segmentNumber,
        messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    byte[] regularValueBytes = createChunkBytes(100, 23);
    values.add(0, createRegularValue(regularValueBytes, VALUE_SCHEMA_ID_2, totalChunkCount + 1, MapperValueType.PUT));
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID_2);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(100, 23));
  }

  @Test
  public void testRegularValueAtTheEndWithIncompleteLargeValue() {
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo,
        null,
        segmentNumber,
        messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    // Randomly remove a value to simulate the incomplete large value
    values.remove(ThreadLocalRandom.current().nextInt(values.size() - 2) + 1);

    byte[] regularValueBytes = createChunkBytes(100, 23);
    values.add(0, createRegularValue(regularValueBytes, VALUE_SCHEMA_ID_2, totalChunkCount + 1, MapperValueType.PUT));
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID_2);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(100, 23));
  }

  @Test
  public void testMultipleRegularValues() {
    final int value1Offset = 1;
    byte[] value1Bytes = createChunkBytes(10, 10);

    final int value2Offset = 2;
    byte[] value2Bytes = createChunkBytes(20, 20);

    final int value3Offset = 3;
    byte[] value3Bytes = createChunkBytes(30, 30);

    List<BytesWritable> values = new ArrayList<>(3);
    values.add(createRegularValue(value1Bytes, VALUE_SCHEMA_ID_2, value1Offset, MapperValueType.PUT));
    values.add(createRegularValue(value2Bytes, VALUE_SCHEMA_ID_2, value2Offset, MapperValueType.PUT));
    values.add(createRegularValue(value3Bytes, VALUE_SCHEMA_ID_2, value3Offset, MapperValueType.PUT)); // The third

    Collections.reverse(values); // value wins

    final byte[] serializedKey = createChunkBytes(0, 5);
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID_2);
    Assert.assertEquals(assembledValue.getBytes(), value3Bytes);
  }

  @Test
  public void testRegularAndDeleteValues() { // Regular value wins
    final int value1Offset = 1;
    byte[] value1Bytes = createChunkBytes(10, 10);

    final int value2Offset = 2; // Delete value offset

    final int value3Offset = 3;
    byte[] value3Bytes = createChunkBytes(30, 30);

    List<BytesWritable> values = new ArrayList<>(3);
    values.add(createRegularValue(value1Bytes, VALUE_SCHEMA_ID_2, value1Offset, MapperValueType.PUT));
    values.add(createRegularValue(new byte[0], -1, value2Offset, MapperValueType.DELETE));
    values.add(createRegularValue(value3Bytes, VALUE_SCHEMA_ID_2, value3Offset, MapperValueType.PUT)); // The third

    // value wins
    Collections.reverse(values);

    final byte[] serializedKey = createChunkBytes(0, 5);
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID_2);
    Assert.assertEquals(assembledValue.getBytes(), value3Bytes);
  }

  @Test
  public void testDeleteValueAtTheEndWithCompleteLargeValue() {
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo,
        null,
        segmentNumber,
        messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    // "Delete value" at the end
    values.add(0, createRegularValue(new byte[0], -1, totalChunkCount + 1, MapperValueType.DELETE));
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());
    Assert.assertNull(assembledValue);
  }

  @Test
  public void testDeleteValueAtTheEndWithIncompleteLargeValue() {
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = createKafkaInputMapperValues(
        serializedKey,
        valueChunkInfo,
        null,
        segmentNumber,
        messageSequenceNumber,
        VALUE_SCHEMA_ID,
        0);
    // Randomly remove a value to simulate the incomplete large value
    values.remove(ThreadLocalRandom.current().nextInt(values.size() - 2) + 1);

    // "Delete value" at the end
    values.add(0, createRegularValue(new byte[0], -1, totalChunkCount + 1, MapperValueType.DELETE));
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNull(assembledValue);
  }

  @Test
  public void testDeleteValueAndRegularValues() { // Delete value wins
    final int value1Offset = 1;
    byte[] value1Bytes = createChunkBytes(10, 10);

    final int value2Offset = 2;
    byte[] value2Bytes = createChunkBytes(20, 20);

    final int value3Offset = 3;
    List<BytesWritable> values = new ArrayList<>(3);
    values.add(createRegularValue(value1Bytes, VALUE_SCHEMA_ID_2, value1Offset, MapperValueType.PUT));
    values.add(createRegularValue(value2Bytes, VALUE_SCHEMA_ID_2, value2Offset, MapperValueType.PUT));
    values.add(createRegularValue(new byte[0], -1, value3Offset, MapperValueType.DELETE)); // The third value wins
    Collections.reverse(values);

    final byte[] serializedKey = createChunkBytes(0, 5);
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNull(assembledValue);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Cannot assemble a large value. Missing .*")
  public void testRegularValueAndIncompleteLargeValue() { // regular value wins
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = new ArrayList<>(1 + totalChunkCount + 1);
    values.addAll(
        createKafkaInputMapperValues(
            serializedKey,
            valueChunkInfo,
            null,
            segmentNumber,
            messageSequenceNumber,
            VALUE_SCHEMA_ID,
            1));
    // Randomly remove a value chunk to simulate the incomplete large value
    values.remove(ThreadLocalRandom.current().nextInt(values.size() - 2) + 1);
    byte[] regularValueBytes = createChunkBytes(100, 23);
    values.add(createRegularValue(regularValueBytes, VALUE_SCHEMA_ID_2, 0, MapperValueType.PUT));

    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID_2);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(100, 23));
  }

  @Test
  public void testRegularValueAndCompleteLargeValue() { // Large value wins
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = new ArrayList<>();

    values.addAll(
        createKafkaInputMapperValues(
            serializedKey,
            valueChunkInfo,
            null,
            segmentNumber,
            messageSequenceNumber,
            VALUE_SCHEMA_ID,
            1));
    byte[] regularValueBytes = createChunkBytes(100, 23);
    values.add(createRegularValue(regularValueBytes, VALUE_SCHEMA_ID_2, 0, MapperValueType.PUT));
    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(0, totalChunkCount * eachCountSizeInBytes));
  }

  @Test
  public void testDeleteValueAndCompleteLargeValue() { // Large value wins
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = new ArrayList<>();

    values.addAll(
        createKafkaInputMapperValues(
            serializedKey,
            valueChunkInfo,
            null,
            segmentNumber,
            messageSequenceNumber,
            VALUE_SCHEMA_ID,
            1));
    values.add(createRegularValue(new byte[0], -1, 0, MapperValueType.DELETE));

    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNotNull(assembledValue);
    Assert.assertEquals(assembledValue.getSchemaID(), VALUE_SCHEMA_ID);
    Assert.assertEquals(assembledValue.getBytes(), createChunkBytes(0, totalChunkCount * eachCountSizeInBytes));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Cannot assemble a large value. Missing .*")
  public void testDeleteValueAndIncompleteLargeValue() {
    final int totalChunkCount = 10;
    final int eachCountSizeInBytes = 20;
    final int segmentNumber = 12;
    final int messageSequenceNumber = 34;
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(totalChunkCount);
    ChunkInfo valueChunkInfo = new ChunkInfo(totalChunkCount, eachCountSizeInBytes);

    final byte[] serializedKey = createChunkBytes(0, 5);
    List<BytesWritable> values = new ArrayList<>();
    values.addAll(
        createKafkaInputMapperValues(
            serializedKey,
            valueChunkInfo,
            null,
            segmentNumber,
            messageSequenceNumber,
            VALUE_SCHEMA_ID,
            1));
    // Randomly remove a chunk value to simulate the incomplete large value
    values.remove(ThreadLocalRandom.current().nextInt(values.size() - 2) + 1);
    values.add(createRegularValue(new byte[0], -1, 0, MapperValueType.DELETE));

    ChunkAssembler.ValueBytesAndSchemaId assembledValue =
        chunkAssembler.assembleAndGetValue(serializedKey, values.iterator());

    Assert.assertNull(assembledValue);
  }

  private BytesWritable createRegularValue(byte[] valueBytes, int schemaId, int offset, MapperValueType valueType) {
    KafkaInputMapperValue regularValue = new KafkaInputMapperValue();
    regularValue.chunkedKeySuffix = ByteBuffer
        .wrap(CHUNKED_KEY_SUFFIX_SERIALIZER.serialize("", KeyWithChunkingSuffixSerializer.NON_CHUNK_KEY_SUFFIX));
    regularValue.schemaId = schemaId;
    regularValue.offset = offset;
    regularValue.value = ByteBuffer.wrap(valueBytes);
    regularValue.valueType = valueType;
    regularValue.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    regularValue.replicationMetadataVersionId = 1;
    return serialize(regularValue);
  }

  /**
   * Create a sequence of chunks and a manifest. E.g. chunk_0, chunk_1, ..., chunk_N, chunk_manifest
   */
  private List<BytesWritable> createKafkaInputMapperValues(
      byte[] serializedKey,
      ChunkInfo valueChunkInfo,
      ChunkInfo rmdChunkInfo,
      int segmentNumber,
      int sequenceNumber,
      int valueSchemaID,
      int startOffset) {

    List<BytesWritable> values = rmdChunkInfo == null
        ? new ArrayList<>(valueChunkInfo.totalChunkCount + 1)
        : new ArrayList<>(valueChunkInfo.totalChunkCount + rmdChunkInfo.totalChunkCount + 1);
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(valueChunkInfo.totalChunkCount);
    ChunkedValueManifest chunkedRmdManifest = null;
    int currOffset = startOffset;
    int currStartingByteValue = 0;
    for (int i = 0; i < valueChunkInfo.totalChunkCount; i++) {
      byte[] chunkBytes = createChunkBytes(currStartingByteValue, valueChunkInfo.eachCountSizeInBytes);
      KafkaInputMapperValue mapperValue = new KafkaInputMapperValue();
      mapperValue.valueType = MapperValueType.PUT;
      mapperValue.offset = currOffset;
      currOffset++;
      mapperValue.schemaId = CHUNK_VALUE_SCHEMA_ID;
      mapperValue.value = ByteBuffer.wrap(chunkBytes);
      ChunkedKeySuffix chunkedKeySuffix = createChunkedKeySuffix(segmentNumber, sequenceNumber, i);
      mapperValue.chunkedKeySuffix = ByteBuffer.wrap(CHUNKED_KEY_SUFFIX_SERIALIZER.serialize("", chunkedKeySuffix));
      mapperValue.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);

      values.add(serialize(mapperValue));
      currStartingByteValue += valueChunkInfo.eachCountSizeInBytes;

      ByteBuffer keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
      chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    }

    if (rmdChunkInfo != null) {
      chunkedRmdManifest = new ChunkedValueManifest();
      chunkedRmdManifest.keysWithChunkIdSuffix = new ArrayList<>(rmdChunkInfo.totalChunkCount);
      currStartingByteValue = 0;
      for (int i = 0; i < rmdChunkInfo.totalChunkCount; i++) {
        byte[] chunkBytes = createChunkBytes(currStartingByteValue, rmdChunkInfo.eachCountSizeInBytes);
        KafkaInputMapperValue mapperValue = new KafkaInputMapperValue();
        mapperValue.valueType = MapperValueType.PUT;
        mapperValue.offset = currOffset;
        currOffset++;
        mapperValue.schemaId = CHUNK_VALUE_SCHEMA_ID;
        mapperValue.value = ByteBuffer.wrap(new byte[0]);
        ChunkedKeySuffix chunkedKeySuffix = createChunkedKeySuffix(segmentNumber, sequenceNumber, i);
        mapperValue.chunkedKeySuffix = ByteBuffer.wrap(CHUNKED_KEY_SUFFIX_SERIALIZER.serialize("", chunkedKeySuffix));
        mapperValue.replicationMetadataPayload = ByteBuffer.wrap(chunkBytes);

        values.add(serialize(mapperValue));
        currStartingByteValue += rmdChunkInfo.eachCountSizeInBytes;

        ByteBuffer keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
        chunkedRmdManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
      }
    }

    // Add the manifest
    chunkedValueManifest.schemaId = valueSchemaID;
    chunkedValueManifest.size = valueChunkInfo.totalChunkCount * valueChunkInfo.eachCountSizeInBytes;
    if (rmdChunkInfo != null) {
      chunkedRmdManifest.schemaId = valueSchemaID;
      chunkedRmdManifest.size = rmdChunkInfo.totalChunkCount * rmdChunkInfo.eachCountSizeInBytes;
    }
    KafkaInputMapperValue lastMapperValue = new KafkaInputMapperValue();
    lastMapperValue.valueType = MapperValueType.PUT;
    lastMapperValue.offset = currOffset;
    lastMapperValue.schemaId = CHUNK_MANIFEST_SCHEMA_ID;
    lastMapperValue.value = ByteBuffer.wrap(CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize("", chunkedValueManifest));
    lastMapperValue.chunkedKeySuffix = ByteBuffer
        .wrap(CHUNKED_KEY_SUFFIX_SERIALIZER.serialize("", KeyWithChunkingSuffixSerializer.NON_CHUNK_KEY_SUFFIX));
    lastMapperValue.replicationMetadataPayload = (rmdChunkInfo == null)
        ? ByteBuffer.wrap(new byte[0])
        : ByteBuffer.wrap(CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize("", chunkedRmdManifest));
    lastMapperValue.replicationMetadataVersionId = 1;

    values.add(serialize(lastMapperValue));
    // The offset of the messages will be in descending order.
    Collections.reverse(values);
    return values;
  }

  private KafkaInputMapperValue deserialize(BytesWritable bytesWritable) {
    return KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(bytesWritable.copyBytes());
  }

  private BytesWritable serialize(KafkaInputMapperValue value) {
    return new BytesWritable(KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_SERIALIZER.serialize(value));
  }

  static class ChunkInfo {
    private final int totalChunkCount;
    private final int eachCountSizeInBytes;

    public ChunkInfo(int totalChunkCount, int eachCountSizeInBytes) {
      this.totalChunkCount = totalChunkCount;
      this.eachCountSizeInBytes = eachCountSizeInBytes;
    }
  }
}
