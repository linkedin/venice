package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;


public class PubSubProducerAdapterDelegator implements PubSubProducerAdapter {
  private final List<PubSubProducerAdapter> producers;
  private final Map<Integer, PubSubProducerAdapter> partitionProducerAssignment;
  private final AtomicInteger selectedProducerId = new AtomicInteger(0);

  public PubSubProducerAdapterDelegator(List<PubSubProducerAdapter> producers) {
    if (producers.isEmpty()) {
      throw new VeniceException("Param 'producers' can't be empty");
    }
    this.producers = producers;
    this.partitionProducerAssignment = new VeniceConcurrentHashMap<>();
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return producers.get(0).getNumberOfPartitions(topic);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders pubSubMessageHeaders,
      PubSubProducerCallback pubSubProducerCallback) {
    PubSubProducerAdapter selectedProducer = partitionProducerAssignment
        .computeIfAbsent(partition, p -> producers.get(selectedProducerId.getAndIncrement() % producers.size()));

    return selectedProducer.sendMessage(topic, partition, key, value, pubSubMessageHeaders, pubSubProducerCallback);
  }

  @Override
  public void flush() {
    producers.forEach(producer -> producer.flush());
  }

  @Override
  public void close(long closeTimeOutMs) {
    producers.forEach(producer -> producer.close(closeTimeOutMs));
  }

  @Override
  public Object2DoubleMap<String> getMeasurableProducerMetrics() {
    return Object2DoubleMaps.emptyMap();
  }

  @Override
  public String getBrokerAddress() {
    return producers.get(0).getBrokerAddress();
  }
}
