package com.linkedin.venice.unit.kafka.producer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.InMemoryKafkaMessage;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;


/**
 * A {@link KafkaProducerWrapper} implementation which interacts with the
 * {@link InMemoryKafkaBroker} in order to make unit tests more lightweight.
 */
public class MockInMemoryProducer implements KafkaProducerWrapper {
  private final InMemoryKafkaBroker broker;

  public MockInMemoryProducer(InMemoryKafkaBroker broker) {
    this.broker = broker;
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return broker.getPartitionCount(topic);
  }

  @Override
  public Future<RecordMetadata> sendMessage(
      String topic,
      KafkaKey key,
      KafkaMessageEnvelope value,
      int partition,
      Callback callback) {
    long offset = broker.produce(topic, partition, new InMemoryKafkaMessage(key, value));
    RecordMetadata recordMetadata =
        new RecordMetadata(new TopicPartition(topic, partition), 0, offset, -1, -1L, -1, -1);
    callback.onCompletion(recordMetadata, null);
    return new Future<RecordMetadata>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public RecordMetadata get() throws InterruptedException, ExecutionException {
        return recordMetadata;
      }

      @Override
      public RecordMetadata get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return recordMetadata;
      }
    };
  }

  @Override
  public Future<RecordMetadata> sendMessage(ProducerRecord<KafkaKey, KafkaMessageEnvelope> record, Callback callback) {
    return sendMessage(record.topic(), record.key(), record.value(), record.partition(), callback);
  }

  @Override
  public void flush() {
    // no-op
  }

  @Override
  public void close(int closeTimeOutMs) {
    // no-op
  }

  @Override
  public Map<String, Double> getMeasurableProducerMetrics() {
    return Collections.emptyMap();
  }
}
