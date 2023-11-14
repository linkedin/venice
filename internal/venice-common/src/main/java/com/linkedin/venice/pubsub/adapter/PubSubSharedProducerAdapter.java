package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper with a unique identifier around a regular producer. This producer may be
 * used to send data to multiple different topics.
 */
public class PubSubSharedProducerAdapter implements PubSubProducerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(PubSubSharedProducerAdapter.class);

  private final PubSubSharedProducerFactory sharedProducerFactory;
  private final Set<String> producerTasks = new HashSet<>();
  private final Map<String, Double> producerMetrics = new HashMap<>();
  private final PubSubProducerAdapter producerAdapter;
  private final int id;
  private long lastStatUpdateTsMs = 0;
  private SharedProducerStats sharedProducerStats;

  public PubSubSharedProducerAdapter(
      PubSubSharedProducerFactory sharedProducerFactory,
      PubSubProducerAdapter producerAdapter,
      MetricsRepository metricsRepository,
      Set<String> metricsToBeReported,
      int id) {
    this.sharedProducerFactory = sharedProducerFactory;
    this.id = id;
    this.producerAdapter = producerAdapter;
    metricsToBeReported
        .forEach(metric -> producerMetrics.put(metric, (double) StatsErrorCode.KAFKA_CLIENT_METRICS_DEFAULT.code));
    if (metricsRepository != null && producerMetrics.size() > 0) {
      sharedProducerStats = new SharedProducerStats(metricsRepository);
    }
  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return producerAdapter.getNumberOfPartitions(topic);
  }

  /**
   * Sends a message to a topic using internal producer adapter.
   * @param topic - The topic to be sent to.
   * @param key - The key of the message to be sent.
   * @param value - The {@link KafkaMessageEnvelope}, which acts as the value.
   * @param callback - The callback function, which will be triggered when producer sends out the message.
   * */
  @Override
  public CompletableFuture<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders headers,
      PubSubProducerCallback callback) {
    long startNs = System.nanoTime();
    CompletableFuture<PubSubProduceResult> result =
        producerAdapter.sendMessage(topic, partition, key, value, headers, callback);
    sharedProducerStats.recordProducerSendLatency(LatencyUtils.getLatencyInMS(startNs));
    return result;
  }

  @Override
  public void flush() {
    producerAdapter.flush();
  }

  @Override
  public void close(int closeTimeOutMs, boolean doFlush) {
    producerAdapter.close(closeTimeOutMs, doFlush);
  }

  @Override
  public void close(String topic, int closeTimeoutMs) {
    if (sharedProducerFactory.isRunning()) {
      sharedProducerFactory.releaseSharedProducer(topic);
    } else {
      LOGGER.info("Producer is already closed, can't release for topic: {}", topic);
    }
  }

  @Override
  public void close(String topic, int closeTimeoutMs, boolean doFlush) {
    if (doFlush) {
      producerAdapter.flush();
    }
    close(topic, closeTimeoutMs);
  }

  @Override
  public Object2DoubleMap<String> getMeasurableProducerMetrics() {
    return producerAdapter.getMeasurableProducerMetrics();
  }

  @Override
  public String getBrokerAddress() {
    return producerAdapter.getBrokerAddress();
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

  private synchronized void mayBeCalculateAllProducerMetrics() {
    if (LatencyUtils.getElapsedTimeInMs(lastStatUpdateTsMs) < 60 * Time.MS_PER_SECOND) {
      return;
    }

    // measure
    Object2DoubleMap<String> metrics = producerAdapter.getMeasurableProducerMetrics();
    producerMetrics.replaceAll((n, v) -> metrics.getOrDefault(n, StatsErrorCode.KAFKA_CLIENT_METRICS_DEFAULT.code));
    lastStatUpdateTsMs = System.currentTimeMillis();
  }

  private class SharedProducerStats extends AbstractVeniceStats {
    private final Sensor producerSendLatencySensor;

    public SharedProducerStats(MetricsRepository metricsRepository) {
      super(metricsRepository, "PubSubSharedProducer");
      producerMetrics.keySet().forEach(metric -> {
        String metricName = "producer_" + id + "_" + metric;
        LOGGER.info("Registering metric: {}", metricName);
        registerSensorIfAbsent(new AsyncGauge((c, t) -> {
          mayBeCalculateAllProducerMetrics();
          return producerMetrics.get(metric);
        }, metricName));
      });
      producerSendLatencySensor = registerSensor("producer_" + id + "_send_latency", new Avg(), new Max());
    }

    public void recordProducerSendLatency(double value) {
      producerSendLatencySensor.record(value);
    }
  }
}
