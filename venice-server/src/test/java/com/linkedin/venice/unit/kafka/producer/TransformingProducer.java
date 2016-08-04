package com.linkedin.venice.unit.kafka.producer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * This {@link KafkaProducerWrapper} implementation allows tests to perform
 * arbitrary transformations on the messages that are about to be written to
 * Kafka.
 *
 * This can be used in unit tests to inject corrupt data.
 */
public class TransformingProducer implements KafkaProducerWrapper {
  private final KafkaProducerWrapper baseProducer;
  private final SendMessageParametersTransformer transformer;

  public TransformingProducer(KafkaProducerWrapper baseProducer, SendMessageParametersTransformer transformer) {
    this.baseProducer = baseProducer;
    this.transformer = transformer;
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return baseProducer.getNumberOfPartitions(topic);
  }

  @Override
  public Future<RecordMetadata> sendMessage(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition) {
    SendMessageParameters parameters = transformer.transform(topic, key, value, partition);
    return baseProducer.sendMessage(parameters.topic, parameters.key, parameters.value, parameters.partition);
  }

  @Override
  public void close(int closeTimeOutMs) {
    baseProducer.close(closeTimeOutMs);
  }

  public static class SendMessageParameters {
    public final String topic;
    public final KafkaKey key;
    public final KafkaMessageEnvelope value;
    public final int partition;
    public SendMessageParameters(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition) {
      this.topic = topic;
      this.key = key;
      this.value = value;
      this.partition = partition;
    }
  }

  public interface SendMessageParametersTransformer {
    SendMessageParameters transform(String topicName, KafkaKey key, KafkaMessageEnvelope value, int partition);
  }
}
