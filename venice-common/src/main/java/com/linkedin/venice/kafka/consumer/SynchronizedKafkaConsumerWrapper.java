package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * A thread-safe wrapper of implementation of {@link KafkaConsumerWrapper}.
 */
public class SynchronizedKafkaConsumerWrapper implements KafkaConsumerWrapper {
  private final KafkaConsumerWrapper delegate;

  public SynchronizedKafkaConsumerWrapper(KafkaConsumerWrapper delegate) {
    this.delegate = delegate;
  }
  @Override
  public synchronized void subscribe(String topic, int partition, OffsetRecord offset) {
    this.delegate.subscribe(topic, partition, offset);
  }

  @Override
  public synchronized void unSubscribe(String topic, int partition) {
    this.delegate.unSubscribe(topic, partition);
  }

  @Override
  public synchronized void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException {
    this.delegate.resetOffset(topic, partition);
  }

  @Override
  public synchronized void close() {
    this.delegate.close();
  }

  @Override
  public synchronized ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeout) {
    return this.delegate.poll(timeout);
  }

  /**
   * Only used in unit test: StoreConsumptionTaskTest.java
   * @return
   */
  public KafkaConsumerWrapper getDelegate() {
    return this.delegate;
  }
}
