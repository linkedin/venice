package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.io.IOException;
import java.io.Serializable;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;


/**
 * This class is used to support secondary sorting for KafkaInput Repush.
 * The key is composed by the raw key + offset, and this class will compare key part first to make it in ascending order
 * and then compare the offset part when keys are equal to maintain an offset-descending order for the same key.
 */
public class KafkaInputKeyComparator implements RawComparator<BytesWritable>, Serializable {
  private static final long serialVersionUID = 1L;

  private static final OptimizedBinaryDecoderFactory OPTIMIZED_BINARY_DECODER_FACTORY =
      OptimizedBinaryDecoderFactory.defaultFactory();
  private static final RecordDeserializer<KafkaInputMapperKey> KAFKA_INPUT_MAPPER_KEY_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(KafkaInputMapperKey.SCHEMA$, KafkaInputMapperKey.class);

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    DataInputBuffer inputBuffer = new DataInputBuffer();
    inputBuffer.reset(b1, s1, l1);

    BytesWritable bw1 = new BytesWritable();
    try {
      bw1.readFields(inputBuffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    inputBuffer.reset(b2, s2, l2);
    BytesWritable bw2 = new BytesWritable();

    try {
      bw2.readFields(inputBuffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return compare(bw1, bw2);
  }

  @Override
  public int compare(BytesWritable o1, BytesWritable o2) {
    if (o1.getLength() == 0) {
      return -1;
    }
    if (o2.getLength() == 0) {
      return 1;
    }
    KafkaInputMapperKey k1 = KAFKA_INPUT_MAPPER_KEY_AVRO_SPECIFIC_DESERIALIZER
        .deserialize(OPTIMIZED_BINARY_DECODER_FACTORY.createOptimizedBinaryDecoder(o1.getBytes(), 0, o1.getLength()));
    KafkaInputMapperKey k2 = KAFKA_INPUT_MAPPER_KEY_AVRO_SPECIFIC_DESERIALIZER
        .deserialize(OPTIMIZED_BINARY_DECODER_FACTORY.createOptimizedBinaryDecoder(o2.getBytes(), 0, o2.getLength()));

    return compare(k1, k2);
  }

  protected int compare(KafkaInputMapperKey k1, KafkaInputMapperKey k2) {
    int compareResult = WritableComparator.compareBytes(
        k1.key.array(),
        k1.key.position(),
        k1.key.remaining(),
        k2.key.array(),
        k2.key.position(),
        k2.key.remaining());

    if (compareResult != 0) {
      return compareResult;
    }

    return Long.compare(k2.offset, k1.offset);
  }
}
