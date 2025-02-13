package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageSerializer;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper over Apache Kafka producer which implements {@link PubSubProducerAdapter}
 */
public class ApacheKafkaProducerAdapter implements PubSubProducerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerAdapter.class);

  private KafkaProducer<byte[], byte[]> producer;
  private final ApacheKafkaProducerConfig producerConfig;
  private final PubSubMessageSerializer messageSerializer;

  /**
   * @param producerConfig contains producer configs
   */
  public ApacheKafkaProducerAdapter(ApacheKafkaProducerConfig producerConfig) {
    this(producerConfig, new KafkaProducer<>(producerConfig.getProducerProperties()));
  }

  @VisibleForTesting
  ApacheKafkaProducerAdapter(ApacheKafkaProducerConfig cfg, KafkaProducer<byte[], byte[]> producer) {
    this.producerConfig = cfg;
    this.producer = producer;
    this.messageSerializer = cfg.getPubSubMessageSerializer();
  }

  /**
   * N.B.: This is an expensive call, the result of which should be cached.
   *
   * @param topic for which we want to request the number of partitions.
   * @return the number of partitions for this topic.
   */
  @Deprecated
  public int getNumberOfPartitions(String topic) {
    ensureProducerIsNotClosed();
    // TODO: This blocks forever. Using getNumberOfPartitions with timeout parameter adds a timeout to this call but
    // other usages need to be refactored to handle the timeout exception correctly
    return producer.partitionsFor(topic).size();
  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param topic - The topic to be sent to.
   * @param key - The key of the message to be sent.
   * @param value - The {@link KafkaMessageEnvelope}, which acts as the Kafka value.
   * @param pubsubProducerCallback - The callback function, which will be triggered when Kafka client sends out the message.
   * @return - A {@link Future} of {@link PubSubProduceResult}, which will be completed when the message is sent to pub-sub server successfully.
   * @throws PubSubOpTimeoutException - If the operation times out.
   * @throws PubSubTopicAuthorizationException - If the producer is not authorized to send to the topic.
   * @throws PubSubTopicDoesNotExistException - If the topic does not exist.
   * @throws PubSubClientRetriableException - If the operation fails due to transient reasons.
   * @throws PubSubClientException - If the operation fails due to other reasons.
   */
  @Override
  public CompletableFuture<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders pubsubMessageHeaders,
      PubSubProducerCallback pubsubProducerCallback) {
    ensureProducerIsNotClosed();
    byte[] keyBytes = messageSerializer.serializeKey(topic, key);
    byte[] valueBytes = messageSerializer.serializeValue(topic, value);
    return sendMessage(topic, partition, keyBytes, valueBytes, pubsubMessageHeaders, pubsubProducerCallback);
  }

  public CompletableFuture<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      byte[] keyBytes,
      byte[] valueBytes,
      PubSubMessageHeaders pubsubMessageHeaders,
      PubSubProducerCallback pubsubProducerCallback) {
    ensureProducerIsNotClosed();
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        topic,
        partition,
        keyBytes,
        valueBytes,
        ApacheKafkaUtils.convertToKafkaSpecificHeaders(pubsubMessageHeaders));
    ApacheKafkaProducerCallback kafkaCallback = new ApacheKafkaProducerCallback(pubsubProducerCallback);
    try {
      producer.send(record, kafkaCallback);
      return kafkaCallback.getProduceResultFuture();
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException(
          "Timed out while trying to produce message into Kafka. Topic: " + record.topic() + ", partition: "
              + record.partition(),
          e);
    } catch (AuthenticationException | AuthorizationException e) {
      throw new PubSubTopicAuthorizationException(
          "Failed to produce message into Kafka due to authorization error. Topic: " + record.topic() + ", partition: "
              + record.partition(),
          e);
    } catch (UnknownTopicOrPartitionException | InvalidTopicException e) {
      throw new PubSubTopicDoesNotExistException(
          "Failed to produce message into Kafka due to topic not found. Topic: " + record.topic() + ", partition: "
              + record.partition(),
          e);
    } catch (Exception e) {
      if (e instanceof RetriableException) {
        throw new PubSubClientRetriableException(
            "Got a retriable error while trying to produce message into Kafka. Topic: '" + record.topic()
                + "', partition: " + record.partition(),
            e);
      }
      throw new PubSubClientException(
          "Got an error while trying to produce message into Kafka. Topic: '" + record.topic() + "', partition: "
              + record.partition(),
          e);
    }
  }

  private void ensureProducerIsNotClosed() {
    if (producer == null) {
      throw new PubSubClientException("The internal KafkaProducer has been closed");
    }
  }

  @Override
  public void flush() {
    if (producer != null) {
      producer.flush();
    }
  }

  @Override
  public void close(long closeTimeOutMs) {
    if (producer == null) {
      return; // producer has been closed already
    }
    producer.close(Duration.ofMillis(closeTimeOutMs));
    // Recycle the internal buffer allocated by KafkaProducer ASAP.
    producer = null;
  }

  @Override
  public Object2DoubleMap<String> getMeasurableProducerMetrics() {
    if (producer == null) {
      return Object2DoubleMaps.emptyMap();
    }
    Set<? extends Map.Entry<MetricName, ? extends Metric>> metrics = producer.metrics().entrySet();
    Object2DoubleMap<String> extractedMetrics = new Object2DoubleOpenHashMap<>(metrics.size());
    for (Map.Entry<MetricName, ? extends Metric> entry: metrics) {
      try {
        Object value = entry.getValue().metricValue();
        if (value instanceof Double) {
          extractedMetrics.put(entry.getKey().name(), (double) value);
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Caught exception: {} when attempting to get producer metrics. Incomplete metrics might be returned.",
            e.getMessage());
      }
    }
    return extractedMetrics;
  }

  @Override
  public String getBrokerAddress() {
    return producerConfig.getBrokerAddress();
  }
}
