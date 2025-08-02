package com.linkedin.venice.hadoop.input.kafka;

import static org.testng.Assert.assertTrue;

import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.testng.annotations.Test;


public class TestKafkaInputKeyComparator {
  private static RecordSerializer<KafkaInputMapperKey> KAFKA_INPUT_MAPPER_KEY_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperKey.SCHEMA$);
  private static KafkaInputKeyComparator KAFKA_INPUT_KEY_COMPARATOR = new KafkaInputKeyComparator();

  private static final ByteBuffer SERIALIZED_EMPTY_BYTES_WRITABLE;

  static {
    try {
      SERIALIZED_EMPTY_BYTES_WRITABLE = getSerializedEmptyBytesWritable();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final BytesWritable EMPTY_BYTES_WRITABLE = new BytesWritable();

  public static BytesWritable getBytesWritable(byte[] key, PubSubPosition offset) {
    KafkaInputMapperKey mapperKey = new KafkaInputMapperKey();
    mapperKey.key = ByteBuffer.wrap(key);
    mapperKey.positionWireBytes = offset.toWireFormatBuffer();
    mapperKey.positionFactoryClass = offset.getFactoryClassName();

    byte[] serializedKey = KAFKA_INPUT_MAPPER_KEY_SERIALIZER.serialize(mapperKey);
    BytesWritable bytesWritable = new BytesWritable();
    bytesWritable.set(serializedKey, 0, serializedKey.length);

    return bytesWritable;
  }

  public static ByteBuffer getSerializedBytesWritable(byte[] key, PubSubPosition offset) throws IOException {
    BytesWritable bytesWritable = getBytesWritable(key, offset);
    DataOutputBuffer outputBuffer = new DataOutputBuffer();
    bytesWritable.write(outputBuffer);
    return ByteBuffer.wrap(outputBuffer.getData(), 0, outputBuffer.getLength());
  }

  private static ByteBuffer getSerializedEmptyBytesWritable() throws IOException {
    BytesWritable bytesWritable = new BytesWritable();
    DataOutputBuffer outputBuffer = new DataOutputBuffer();
    bytesWritable.write(outputBuffer);
    return ByteBuffer.wrap(outputBuffer.getData(), 0, outputBuffer.getLength());
  }

  @Test
  public void testCompareWithDifferentKey() throws IOException {
    byte[] key1 = "123".getBytes();
    byte[] key2 = "223".getBytes();
    PubSubPosition offsetForKey1 = ApacheKafkaOffsetPosition.of(1);
    PubSubPosition offsetForKey2 = ApacheKafkaOffsetPosition.of(2);
    BytesWritable bwForKey1 = getBytesWritable(key1, offsetForKey1);
    BytesWritable bwForKey2 = getBytesWritable(key2, offsetForKey2);
    ByteBuffer bbForKey1 = getSerializedBytesWritable(key1, offsetForKey1);
    ByteBuffer bbForKey2 = getSerializedBytesWritable(key2, offsetForKey2);

    assertTrue(KAFKA_INPUT_KEY_COMPARATOR.compare(bwForKey1, bwForKey2) < 0);
    assertTrue(KAFKA_INPUT_KEY_COMPARATOR.compare(bwForKey2, bwForKey1) > 0);

    assertTrue(
        KAFKA_INPUT_KEY_COMPARATOR.compare(
            bbForKey1.array(),
            bbForKey1.position(),
            bbForKey1.remaining(),
            bbForKey2.array(),
            bbForKey2.position(),
            bbForKey2.remaining()) < 0);
    assertTrue(
        KAFKA_INPUT_KEY_COMPARATOR.compare(
            bbForKey2.array(),
            bbForKey2.position(),
            bbForKey2.remaining(),
            bbForKey1.array(),
            bbForKey1.position(),
            bbForKey1.remaining()) > 0);
  }

  @Test
  public void testCompareWithSameKeyWithDifferentOffset() throws IOException {
    byte[] key = "123".getBytes();
    PubSubPosition keyOffset1 = ApacheKafkaOffsetPosition.of(1);
    PubSubPosition keyOffset2 = ApacheKafkaOffsetPosition.of(2);
    BytesWritable bwForKey1 = getBytesWritable(key, keyOffset1);
    BytesWritable bwForKey2 = getBytesWritable(key, keyOffset2);
    ByteBuffer bbForKey1 = getSerializedBytesWritable(key, keyOffset1);
    ByteBuffer bbForKey2 = getSerializedBytesWritable(key, keyOffset2);

    assertTrue(KAFKA_INPUT_KEY_COMPARATOR.compare(bwForKey1, bwForKey2) > 0);
    assertTrue(KAFKA_INPUT_KEY_COMPARATOR.compare(bwForKey2, bwForKey1) < 0);

    assertTrue(
        KAFKA_INPUT_KEY_COMPARATOR.compare(
            bbForKey1.array(),
            bbForKey1.position(),
            bbForKey1.remaining(),
            bbForKey2.array(),
            bbForKey2.position(),
            bbForKey2.remaining()) > 0);
    assertTrue(
        KAFKA_INPUT_KEY_COMPARATOR.compare(
            bbForKey2.array(),
            bbForKey2.position(),
            bbForKey2.remaining(),
            bbForKey1.array(),
            bbForKey1.position(),
            bbForKey1.remaining()) < 0);
  }

  @Test
  public void testSprayKeySorting() throws IOException {
    byte[] key = "123".getBytes();
    PubSubPosition keyOffset1 = ApacheKafkaOffsetPosition.of(1);
    BytesWritable bwForKey1 = getBytesWritable(key, keyOffset1);
    ByteBuffer bbForKey1 = getSerializedBytesWritable(key, keyOffset1);

    assertTrue(KAFKA_INPUT_KEY_COMPARATOR.compare(EMPTY_BYTES_WRITABLE, bwForKey1) < 0);
    assertTrue(KAFKA_INPUT_KEY_COMPARATOR.compare(bwForKey1, EMPTY_BYTES_WRITABLE) > 0);

    assertTrue(
        KAFKA_INPUT_KEY_COMPARATOR.compare(
            bbForKey1.array(),
            bbForKey1.position(),
            bbForKey1.remaining(),
            SERIALIZED_EMPTY_BYTES_WRITABLE.array(),
            SERIALIZED_EMPTY_BYTES_WRITABLE.position(),
            SERIALIZED_EMPTY_BYTES_WRITABLE.remaining()) > 0);
    assertTrue(
        KAFKA_INPUT_KEY_COMPARATOR.compare(
            SERIALIZED_EMPTY_BYTES_WRITABLE.array(),
            SERIALIZED_EMPTY_BYTES_WRITABLE.position(),
            SERIALIZED_EMPTY_BYTES_WRITABLE.remaining(),
            bbForKey1.array(),
            bbForKey1.position(),
            bbForKey1.remaining()) < 0);
  }
}
