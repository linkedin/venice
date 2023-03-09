package com.linkedin.venice.unit.kafka.producer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import java.util.concurrent.Future;


/**
 * This {@link PubSubProducerAdapter} implementation allows tests to perform
 * arbitrary transformations on the messages that are about to be written to
 * Kafka.
 *
 * This can be used in unit tests to inject corrupt data.
 */
public class TransformingProducerAdapter implements PubSubProducerAdapter {
  private final PubSubProducerAdapter baseProducer;
  private final SendMessageParametersTransformer transformer;

  public TransformingProducerAdapter(PubSubProducerAdapter baseProducer, SendMessageParametersTransformer transformer) {
    this.baseProducer = baseProducer;
    this.transformer = transformer;
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return baseProducer.getNumberOfPartitions(topic);
  }

  @Override
  public Future<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders headers,
      PubSubProducerCallback callback) {
    SendMessageParameters parameters = transformer.transform(topic, key, value, partition);
    return baseProducer
        .sendMessage(parameters.topic, parameters.partition, parameters.key, parameters.value, headers, callback);
  }

  @Override
  public void flush() {
    baseProducer.flush();
  }

  @Override
  public void close(int closeTimeOutMs, boolean flush) {
    baseProducer.close(closeTimeOutMs, flush);
  }

  @Override
  public Object2DoubleMap<String> getMeasurableProducerMetrics() {
    return baseProducer.getMeasurableProducerMetrics();
  }

  @Override
  public String getBrokerAddress() {
    return baseProducer.getBrokerAddress();
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
