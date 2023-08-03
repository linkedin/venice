package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper over Apache Kafka producer which implements {@link PubSubProducerAdapter}
 */
public class ApacheKafkaProducerAdapter implements PubSubProducerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerAdapter.class);

  private KafkaProducer<KafkaKey, KafkaMessageEnvelope> producer;
  private final ApacheKafkaProducerConfig producerConfig;
  private boolean forceClosed = false;

  /**
   * @param producerConfig contains producer configs
   */
  public ApacheKafkaProducerAdapter(ApacheKafkaProducerConfig producerConfig) {
    this(producerConfig, new KafkaProducer<>(producerConfig.getProducerProperties()));
  }

  ApacheKafkaProducerAdapter(ApacheKafkaProducerConfig cfg, KafkaProducer<KafkaKey, KafkaMessageEnvelope> producer) {
    this.producerConfig = cfg;
    this.producer = producer;
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
   * */
  @Override
  public Future<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders pubsubMessageHeaders,
      PubSubProducerCallback pubsubProducerCallback) {
    ensureProducerIsNotClosed();
    ProducerRecord<KafkaKey, KafkaMessageEnvelope> record = new ProducerRecord<>(
        topic,
        partition,
        key,
        value,
        ApacheKafkaUtils.convertToKafkaSpecificHeaders(pubsubMessageHeaders));
    ApacheKafkaProducerCallback kafkaCallback = new ApacheKafkaProducerCallback(pubsubProducerCallback, this);
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
  public void close(int closeTimeOutMs, boolean doFlush) {
    if (producer == null) {
      return; // producer has been closed already
    }
    if (doFlush) {
      // Flush out all the messages in the producer buffer
      producer.flush(closeTimeOutMs, TimeUnit.MILLISECONDS);
      LOGGER.info("Flushed all the messages in producer before closing");
    }
    if (closeTimeOutMs == 0) {
      forceClosed = true;
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

  boolean isForceClosed() {
    return forceClosed;
  }
}
