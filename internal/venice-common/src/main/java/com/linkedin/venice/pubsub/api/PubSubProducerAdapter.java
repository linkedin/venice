package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * The pub-sub producer interface with which venice writer's interact to send messages to pub-sub topic.
 *
 * An implementation of this interface are required to provide the following guarantees:
 *  1. At-least once delivery (ALOD): messages should not be dropped.
 *  2. In order delivery (IOD): messages in the same partition should follow the order in which they were sent.
 */
public interface PubSubProducerAdapter {
  ExecutorService timeOutExecutor = Executors.newSingleThreadExecutor();

  /**
   * The support for the following two getNumberOfPartitions APIs will be removed.
   */
  @Deprecated
  int getNumberOfPartitions(String topic);

  @Deprecated
  default int getNumberOfPartitions(String topic, int timeout, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    Callable<Integer> task = () -> getNumberOfPartitions(topic);
    Future<Integer> future = timeOutExecutor.submit(task);
    return future.get(timeout, timeUnit);
  }

  Future<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders headers,
      PubSubProducerCallback callback);

  void flush();

  void close(int closeTimeOutMs, boolean doFlush);

  default void close(String topic, int closeTimeOutMs, boolean doFlush) {
    close(closeTimeOutMs, doFlush);
  }

  default void close(String topic, int closeTimeOutMs) {
    close(closeTimeOutMs, true);
  }

  Object2DoubleMap<String> getMeasurableProducerMetrics();

  String getBrokerAddress();
}
