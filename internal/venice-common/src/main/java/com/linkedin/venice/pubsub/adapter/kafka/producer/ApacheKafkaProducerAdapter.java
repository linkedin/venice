package com.linkedin.venice.pubsub.adapter.kafka.producer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper over Apache Kafka producer which implements {@link PubSubProducerAdapter}
 */
public class ApacheKafkaProducerAdapter implements PubSubProducerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerAdapter.class);

  private KafkaProducer<KafkaKey, KafkaMessageEnvelope> producer;
  private final ApacheKafkaProducerConfig producerConfig;

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
    ApacheKafkaProducerCallback kafkaCallback = new ApacheKafkaProducerCallback(pubsubProducerCallback);
    try {
      producer.send(record, kafkaCallback);
      return kafkaCallback.getProduceResultFuture();
    } catch (Exception e) {
      throw new VeniceException(
          "Got an error while trying to produce message into Kafka. Topic: '" + record.topic() + "', partition: "
              + record.partition(),
          e);
    }
  }

  private void ensureProducerIsNotClosed() {
    if (producer == null) {
      throw new VeniceException("The internal KafkaProducer has been closed");
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
