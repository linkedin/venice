package com.linkedin.venice.writer;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.log4j.Logger;


/**
 * Implementation of the shared Kafka Producer for sending messages to Kafka.
 */
public class SharedKafkaProducer implements KafkaProducerWrapper {
  private static final Logger LOGGER = Logger.getLogger(SharedKafkaProducer.class);

  private final SharedKafkaProducerService sharedKafkaProducerService;
  private final int id;
  private final Set<String> producerTasks;
  private final KafkaProducerWrapper kafkaProducerWrapper;

  public SharedKafkaProducer(SharedKafkaProducerService sharedKafkaProducerService, int id, KafkaProducerWrapper kafkaProducerWrapper) {
    this.sharedKafkaProducerService = sharedKafkaProducerService;
    this.id = id;
    producerTasks = new HashSet<>();
    this.kafkaProducerWrapper = kafkaProducerWrapper;
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return kafkaProducerWrapper.getNumberOfPartitions(topic);
  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param topic - The topic to be sent to.
   * @param key - The key of the message to be sent.
   * @param value - The {@link KafkaMessageEnvelope}, which acts as the Kafka value.
   * @param callback - The callback function, which will be triggered when Kafka client sends out the message.
   * */
  @Override
  public Future<RecordMetadata> sendMessage(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition, Callback callback) {
    return kafkaProducerWrapper.sendMessage(topic, key, value, partition, callback);
  }

  @Override
  public Future<RecordMetadata> sendMessage(ProducerRecord<KafkaKey, KafkaMessageEnvelope> record, Callback callback) {
    return kafkaProducerWrapper.sendMessage(record, callback);
  }

  @Override
  public void flush() {
    kafkaProducerWrapper.flush();
  }

  @Override
  public void close(int closeTimeOutMs) {
    kafkaProducerWrapper.close(closeTimeOutMs);
  }

  @Override
  public void close(String topic, int closeTimeoutMs) {
    sharedKafkaProducerService.releaseKafkaProducer(topic);
  }

  @Override
  public Map<String, Double> getMeasurableProducerMetrics() {
    return kafkaProducerWrapper.getMeasurableProducerMetrics();
  }

  @Override
  public String getBrokerLeaderHostname(String topic, int partition) {
    return kafkaProducerWrapper.getBrokerLeaderHostname(topic, partition);
  }

  public int getId() {
    return id;
  }

  public synchronized void addProducerTask(String producerTaskName) {
    producerTasks.add(producerTaskName);
  }

  public synchronized void removeProducerTask(String producerTaskName) {
    producerTasks.remove(producerTaskName);
  }

  public int getProducerTaskCount() {
    return producerTasks.size();
  }

  public String toString() {
    return "{Id: " + id + ", Task Count: " + getProducerTaskCount() + "}";
  }

}
