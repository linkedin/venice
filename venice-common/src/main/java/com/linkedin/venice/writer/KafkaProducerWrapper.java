package com.linkedin.venice.writer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public interface KafkaProducerWrapper {
  ExecutorService timeOutExecutor = Executors.newSingleThreadExecutor();
  int getNumberOfPartitions(String topic);

  default int getNumberOfPartitions(String topic, int timeout, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    Callable<Integer> task = new Callable<Integer>() {
      public Integer call() {
        return getNumberOfPartitions(topic);
      }
    };
    Future<Integer> future = timeOutExecutor.submit(task);
    return future.get(timeout, timeUnit);
  }

  Future<RecordMetadata> sendMessage(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition, Callback callback);
  Future<RecordMetadata> sendMessage(ProducerRecord<KafkaKey, KafkaMessageEnvelope> record, Callback callback);
  void flush();
  void close(int closeTimeOutMs);
  default void close(int closeTimeOutMs, boolean doFlush) {close(closeTimeOutMs);}
  default void close(String topic, int closeTimeOutMs) { close(closeTimeOutMs); }
  Map<String, Double> getMeasurableProducerMetrics();
  String getBrokerLeaderHostname(String topic, int partition);
}
