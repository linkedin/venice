package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import org.apache.hadoop.io.WritableComparator;


/**
 * This class together with {@link KafkaInputKeyComparator} supports secondary sorting of KafkaInput Repush.
 * This class is used to group the entries with same key by ignoring the offset part.
 */
public class KafkaInputValueGroupingComparator extends KafkaInputKeyComparator {
  private static final long serialVersionUID = 1L;

  @Override
  protected int compare(KafkaInputMapperKey k1, KafkaInputMapperKey k2) {
    return WritableComparator.compareBytes(
        k1.key.array(),
        k1.key.position(),
        k1.key.remaining(),
        k2.key.array(),
        k2.key.position(),
        k2.key.remaining());
  }
}
