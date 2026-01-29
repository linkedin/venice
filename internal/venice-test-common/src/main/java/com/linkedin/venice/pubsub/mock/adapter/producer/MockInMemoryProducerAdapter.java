package com.linkedin.venice.pubsub.mock.adapter.producer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.SimplePubSubProduceResultImpl;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubBroker;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubMessage;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * A {@link PubSubProducerAdapter} implementation which interacts with the
 * {@link InMemoryPubSubBroker} in order to make unit tests more lightweight.
 */
public class MockInMemoryProducerAdapter implements PubSubProducerAdapter {
  private final InMemoryPubSubBroker broker;

  public MockInMemoryProducerAdapter(InMemoryPubSubBroker broker) {
    this.broker = broker;
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return broker.getPartitionCount(topic);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders headers,
      PubSubProducerCallback callback) {
    InMemoryPubSubPosition inMemoryPubSubPosition =
        broker.produce(topic, partition, new InMemoryPubSubMessage(key, value, headers));
    PubSubProduceResult produceResult = new SimplePubSubProduceResultImpl(topic, partition, inMemoryPubSubPosition, -1);
    if (callback != null) {
      callback.onCompletion(produceResult, null);
    }
    return CompletableFuture.completedFuture(produceResult);
  }

  @Override
  public void flush() {
    // no-op
  }

  @Override
  public void close(long closeTimeOutMs) {
    // no-op
  }

  @Override
  public Object2DoubleMap<String> getMeasurableProducerMetrics() {
    return Object2DoubleMaps.emptyMap();
  }

  @Override
  public String getBrokerAddress() {
    return broker.getPubSubBrokerAddress();
  }

  public static InMemoryPubSubPosition getPosition(Future<PubSubProduceResult> produceResultFuture)
      throws ExecutionException, InterruptedException {
    if (produceResultFuture == null || produceResultFuture.get() == null) {
      throw new VeniceException("Produce result future is null or empty.");
    }
    if (!(produceResultFuture.get().getPubSubPosition() instanceof InMemoryPubSubPosition)) {
      throw new VeniceException(
          "Produce result future does not contain InMemoryPubSubPosition: "
              + produceResultFuture.get().getPubSubPosition());
    }
    return (InMemoryPubSubPosition) produceResultFuture.get().getPubSubPosition();
  }
}
