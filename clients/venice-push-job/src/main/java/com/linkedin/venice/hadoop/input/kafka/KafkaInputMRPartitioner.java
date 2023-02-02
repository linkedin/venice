package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.hadoop.VeniceMRPartitioner;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.hadoop.io.BytesWritable;


/**
 * This class is used for KafkaInput Repush, and it only considers the key part of the composed key (ignoring the offset).
 */
public class KafkaInputMRPartitioner extends VeniceMRPartitioner {
  private static final OptimizedBinaryDecoderFactory OPTIMIZED_BINARY_DECODER_FACTORY =
      OptimizedBinaryDecoderFactory.defaultFactory();
  private static final RecordDeserializer<KafkaInputMapperKey> KAFKA_INPUT_MAPPER_KEY_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(KafkaInputMapperKey.SCHEMA$, KafkaInputMapperKey.class);

  @Override
  protected int getPartition(BytesWritable key, int numPartitions) {
    KafkaInputMapperKey mapperKey = KAFKA_INPUT_MAPPER_KEY_AVRO_SPECIFIC_DESERIALIZER
        .deserialize(OPTIMIZED_BINARY_DECODER_FACTORY.createOptimizedBinaryDecoder(key.getBytes(), 0, key.getLength()));
    return venicePartitioner
        .getPartitionId(mapperKey.key.array(), mapperKey.key.position(), mapperKey.key.remaining(), numPartitions);
  }

}
