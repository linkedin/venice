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


/**
 * This class tracks consumed topic partitions' offsets
 */
public class TopicPartitionsOffsetsTracker {
  static final long INVALID = -1;
  private static final long DEFAULT_OFFSETS_UPDATE_INTERVAL_MS = 30 * Time.MS_PER_SECOND;

  private final Map<TopicPartition, KafkaPartitionTopicOffsetMetrics> topicPartitionOffsetInfo;
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

    boolean lagMetricMissing = false;

    // Update current offset cache for all topics partition.
    List<ConsumerRecord<byte[], byte[]>> listOfRecordsForOnePartition;
    ConsumerRecord<byte[], byte[]> lastConsumerRecordOfPartition;
    KafkaPartitionTopicOffsetMetrics offsetInfo;
    for (TopicPartition tp: records.partitions()) {
      listOfRecordsForOnePartition = records.records(tp);
      lastConsumerRecordOfPartition = listOfRecordsForOnePartition.get(listOfRecordsForOnePartition.size() - 1);
      offsetInfo = topicPartitionOffsetInfo.computeIfAbsent(tp, k -> new KafkaPartitionTopicOffsetMetrics());
      offsetInfo.setCurrentOffset(lastConsumerRecordOfPartition.offset());
      if (offsetInfo.isLagMetricMissing()) {
        lagMetricMissing = true;
      }
    }

    long currentTime = System.currentTimeMillis();
    long elapsedTime = currentTime - lastMetricsCollectedTime;
    if (elapsedTime < offsetsUpdateIntervalMs && !lagMetricMissing) {
      return; // Not yet
    }

    lastMetricsCollectedTime = currentTime;
    MetricName metricName;
    Metric metric;
    TopicPartition tp;
    for (Map.Entry<MetricName, ? extends Metric> entry: kafkaConsumer.metrics().entrySet()) {
      metricName = entry.getKey();
      metric = entry.getValue();

      if (Objects.equals(metricName.name(), "records-lag")) {
        tp = new TopicPartition(metricName.tags().get("topic"), Integer.parseInt(metricName.tags().get("partition")));
        offsetInfo = topicPartitionOffsetInfo.computeIfAbsent(tp, k -> new KafkaPartitionTopicOffsetMetrics());
        offsetInfo.setLagMetric(metric);
      }
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
    KafkaPartitionTopicOffsetMetrics offsetInfo = topicPartitionOffsetInfo.get(new TopicPartition(topic, partition));
    return offsetInfo == null ? INVALID : offsetInfo.getEndOffset();
  }

  /**
   * Get consuming offset lag on a topic partition
   * @param topic
   * @param partition
   * @return end offset of a topic partition if there is any.
   */
  public long getOffsetLag(String topic, int partition) {
    KafkaPartitionTopicOffsetMetrics offsetInfo = topicPartitionOffsetInfo.get(new TopicPartition(topic, partition));
    return offsetInfo == null ? INVALID : offsetInfo.getLag();
  }
}
