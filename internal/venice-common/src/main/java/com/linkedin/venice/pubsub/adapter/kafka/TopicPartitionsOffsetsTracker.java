package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class tracks consumed topic partitions' offsets
 */
public class TopicPartitionsOffsetsTracker {
  private static final Logger LOGGER = LogManager.getLogger(TopicPartitionsOffsetsTracker.class);
  private static final long INVALID = -1;
  private static final long DEFAULT_OFFSETS_UPDATE_INTERVAL_MS = 30 * Time.MS_PER_SECOND;

  private final Map<TopicPartition, OffsetInfo> topicPartitionOffsetInfo;
  private volatile long lastMetricsCollectedTime;
  private final long offsetsUpdateIntervalMs;

  public TopicPartitionsOffsetsTracker() {
    this(validateInterval(DEFAULT_OFFSETS_UPDATE_INTERVAL_MS));
  }

  /**
   * N.B. Package-private, for tests only. This constructor skips the {@link #validateInterval(long)} check.
   */
  TopicPartitionsOffsetsTracker(long offsetsUpdateIntervalMs) {
    this.offsetsUpdateIntervalMs = offsetsUpdateIntervalMs;
    this.lastMetricsCollectedTime = 0;
    // N.B. This map can be accessed both by poll (one caller at a time) and by metrics (arbitrary concurrency)
    // so it needs to be threadsafe.
    this.topicPartitionOffsetInfo = new VeniceConcurrentHashMap<>();
  }

  private static long validateInterval(long interval) {
    if (interval > 0) {
      return interval;
    }
    throw new IllegalArgumentException("The interval must be above zero.");
  }

  /**
   * Update the end and current offsets of consumed partitions based on consumer metrics and given consumed records.
   * For each topic partition, end offset == consumed current offset + offset lag and current offset is determined
   * to be the largest offset amongst the records passed in.
   *
   * @param records consumed records
   * @param kafkaConsumer from which to extract metrics
   */
  public void updateEndAndCurrentOffsets(
      ConsumerRecords<byte[], byte[]> records,
      Consumer<byte[], byte[]> kafkaConsumer) {
    long lastMetricsCollectedTimeSnapshot = lastMetricsCollectedTime;
    long currentTime = System.currentTimeMillis();
    long elapsedTime = currentTime - lastMetricsCollectedTimeSnapshot;
    if (elapsedTime < offsetsUpdateIntervalMs) {
      return; // Not yet
    }
    synchronized (this) {
      /**
       * N.B.: This race condition check could be defeated if we allowed the {@link offsetsUpdateIntervalMs} to be zero.
       *       This is why the {@link #validateInterval(long)} function protects against this at construction-time.
       *       We do allow an interval of zero in tests, via the package-private constructor, because these are not
       *       multi-threaded.
       */
      if (lastMetricsCollectedTimeSnapshot == lastMetricsCollectedTime) {
        lastMetricsCollectedTime = currentTime;
      } else {
        return; // Another thread got in first.
      }
    }

    // Update current offset cache for all topics partition.
    List<ConsumerRecord<byte[], byte[]>> listOfRecordsForOnePartition;
    ConsumerRecord<byte[], byte[]> lastConsumerRecordOfPartition;
    for (TopicPartition tp: records.partitions()) {
      listOfRecordsForOnePartition = records.records(tp);
      lastConsumerRecordOfPartition = listOfRecordsForOnePartition.get(listOfRecordsForOnePartition.size() - 1);
      topicPartitionOffsetInfo.computeIfAbsent(tp, k -> new OffsetInfo()).currentOffset =
          lastConsumerRecordOfPartition.offset();
    }

    MetricName metricName;
    Metric metric;
    TopicPartition tp;
    OffsetInfo offsetInfo;
    long metricValue;
    for (Map.Entry<MetricName, ? extends Metric> entry: kafkaConsumer.metrics().entrySet()) {
      metricName = entry.getKey();
      metric = entry.getValue();
      metricValue = getMetricEntryRecordsLag(metricName, metric);

      if (metricValue != INVALID) {
        tp = new TopicPartition(metricName.tags().get("topic"), Integer.parseInt(metricName.tags().get("partition")));
        offsetInfo = topicPartitionOffsetInfo.computeIfAbsent(tp, k -> new OffsetInfo());
        offsetInfo.lag = metricValue;
        if (offsetInfo.currentOffset != INVALID) {
          offsetInfo.endOffset = offsetInfo.currentOffset + metricValue;
        }
      }
    }
  }

  /**
   * @return the lag value if the metric is for lag, or {@link #INVALID} otherwise.
   */
  private long getMetricEntryRecordsLag(MetricName metricName, Metric metric) {
    try {
      if (Objects.equals(metricName.name(), "records-lag")) {
        Object metricValue = metric.metricValue();
        if (metricValue instanceof Double) {
          // Double is the way all metrics are internally represented in Kafka, but since we are dealing with lag, we
          // want an integral type, so we cast it.
          return ((Double) metricValue).longValue();
        }
      }
      return INVALID;
    } catch (Exception e) {
      LOGGER.warn(
          "Caught exception: {} when attempting to get consumer metrics. Incomplete metrics might be returned.",
          e.getMessage());
      return INVALID;
    }
  }

  /**
   * Remove tracked offsets state of a topic partition.
   *
   * @param topicPartition
   */
  public void removeTrackedOffsets(TopicPartition topicPartition) {
    topicPartitionOffsetInfo.remove(topicPartition);
  }

  /**
   * Clear all tracked offsets state
   */
  public void clearAllOffsetState() {
    topicPartitionOffsetInfo.clear();
  }

  /**
   * Get the end offset of a topic partition
   * @param topic
   * @param partition
   * @return end offset of a topic partition if there is any.
   */
  public long getEndOffset(String topic, int partition) {
    OffsetInfo offsetInfo = topicPartitionOffsetInfo.get(new TopicPartition(topic, partition));
    return offsetInfo == null ? INVALID : offsetInfo.endOffset;
  }

  /**
   * Get consuming offset lag on a topic partition
   * @param topic
   * @param partition
   * @return end offset of a topic partition if there is any.
   */
  public long getOffsetLag(String topic, int partition) {
    OffsetInfo offsetInfo = topicPartitionOffsetInfo.get(new TopicPartition(topic, partition));
    return offsetInfo == null ? INVALID : offsetInfo.lag;
  }

  private static class OffsetInfo {
    long currentOffset = INVALID;
    long endOffset = INVALID;
    long lag = INVALID;
  }
}
