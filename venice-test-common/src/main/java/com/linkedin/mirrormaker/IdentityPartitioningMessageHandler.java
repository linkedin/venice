package com.linkedin.mirrormaker;

import java.util.Collections;
import java.util.List;
import kafka.consumer.BaseConsumerRecord;
import kafka.message.MessageAndMetadata;
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * N.B.: This class comes from LiKafka. It ought to be part of Kafka proper but it is not yet open-sourced.
 * This class is necessary in order to spin-up MirrorMaker in tests.
 *
 * A message handler that performs identity partitioning. It assumes that the source cluster topic and the target
 * cluster topic have an identical partition count; or at the very least that the target cluster topic partition count
 * is greater than or equal to the source cluster's topic partition count.
 */
public class IdentityPartitioningMessageHandler implements MirrorMaker.MirrorMakerMessageHandler {
  /**
   * This is the old API.
   */
  // @Override
  public List<ProducerRecord<byte[], byte[]>> handle(MessageAndMetadata<byte[], byte[]> record) {
    return Collections.singletonList(new ProducerRecord<>(record.topic(), record.partition(), record.key(), record.message()));
  }

  /**
   * This is the contemporary API.
   */
  // @Override
  public List<ProducerRecord<byte[], byte[]>> handle(BaseConsumerRecord record) {
    return Collections.singletonList(new ProducerRecord<>(record.topic(), record.partition(), record.key(), record.value()));
  }
}