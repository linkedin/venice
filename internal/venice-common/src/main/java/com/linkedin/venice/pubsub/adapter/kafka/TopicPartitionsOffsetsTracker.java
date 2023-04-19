package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;
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
  public enum ResultType {
    VALID_OFFSET_LAG, NO_OFFSET_LAG, INVALID_OFFSET_LAG
  }

  private static final Logger LOGGER = LogManager.getLogger(TopicPartitionsOffsetsTracker.class);
  private static final Duration DEFAULT_OFFSETS_UPDATE_INTERVAL = Duration.ofSeconds(30);
  private static final Duration DEFAULT_MIN_LOG_INTERVAL = Duration.ofMinutes(3);
  private static final ResultType[] RESULT_TYPE_VALUES = ResultType.values();

  private final Map<TopicPartition, Double> topicPartitionCurrentOffset;
  private final Map<TopicPartition, Double> topicPartitionEndOffset;
  private Instant lastMetricsCollectedTime;
  private final Duration offsetsUpdateInterval;
  private final StatsAccumulator statsAccumulator;

  public TopicPartitionsOffsetsTracker() {
    this(DEFAULT_OFFSETS_UPDATE_INTERVAL);
  }

  public TopicPartitionsOffsetsTracker(Duration offsetsUpdateInterval) {
    this(offsetsUpdateInterval, DEFAULT_MIN_LOG_INTERVAL);
  }

  public TopicPartitionsOffsetsTracker(@Nonnull Duration offsetsUpdateInterval, Duration minLogInterval) {
    Validate.notNull(offsetsUpdateInterval);
    this.offsetsUpdateInterval = offsetsUpdateInterval;
    this.lastMetricsCollectedTime = null;
    // N.B. These maps can be accessed both by poll (one caller at a time) and by metrics (arbitrary concurrency)
    // so they need to be threadsafe.
    this.topicPartitionCurrentOffset = new VeniceConcurrentHashMap<>();
    this.topicPartitionEndOffset = new VeniceConcurrentHashMap<>();
    this.statsAccumulator = new StatsAccumulator(minLogInterval);
  }

  /**
   * Update the end and current offsets of consumed partitions based on consumer metrics and given consumed records.
   * For each topic partition, end offset == consumed current offset + offset lag and current offset is determined
   * to be the largest offset amongst the records passed in.
   *
   * @param records consumed records
   */
  public void updateEndAndCurrentOffsets(
      ConsumerRecords<byte[], byte[]> records,
      Map<MetricName, ? extends Metric> metrics) {
    if (lastMetricsCollectedTime != null && LatencyUtils
        .getElapsedTimeInMs(lastMetricsCollectedTime.toEpochMilli()) < offsetsUpdateInterval.toMillis()) {
      return; // Not yet
    }
    lastMetricsCollectedTime = Instant.now();

    // Update current offset cache for all topics partition.
    List<ConsumerRecord<byte[], byte[]>> listOfRecordsForOnePartition;
    ConsumerRecord<byte[], byte[]> lastConsumerRecordOfPartition;
    for (TopicPartition tp: records.partitions()) {
      listOfRecordsForOnePartition = records.records(tp);
      lastConsumerRecordOfPartition = listOfRecordsForOnePartition.get(listOfRecordsForOnePartition.size() - 1);
      topicPartitionCurrentOffset.put(tp, (double) lastConsumerRecordOfPartition.offset());
    }

    MetricName metricName;
    Metric metric;
    TopicPartition tp;
    Double currOffset;
    for (Map.Entry<MetricName, ? extends Metric> entry: metrics.entrySet()) {
      metricName = entry.getKey();
      metric = entry.getValue();

      if (isMetricEntryRecordsLag(metricName, metric)) {
        tp = new TopicPartition(metricName.tags().get("topic"), Integer.parseInt(metricName.tags().get("partition")));
        currOffset = topicPartitionCurrentOffset.get(tp);
        if (currOffset != null) {
          topicPartitionEndOffset.put(tp, currOffset + ((Double) metric.metricValue()));
        }
      }
    }
  }

  private boolean isMetricEntryRecordsLag(MetricName metricName, Metric metric) {
    try {
      return Objects.equals(metricName.name(), "records-lag") && (metric.metricValue() instanceof Double);
    } catch (Exception e) {
      LOGGER.warn(
          "Caught exception: {} when attempting to get consumer metrics. Incomplete metrics might be returned.",
          e.getMessage());
      return false;
    }
  }

  /**
   * Remove tracked offsets state of a topic partition.
   *
   * @param topicPartition
   */
  public void removeTrackedOffsets(TopicPartition topicPartition) {
    topicPartitionCurrentOffset.remove(topicPartition);
    topicPartitionEndOffset.remove(topicPartition);
  }

  /**
   * Clear all tracked offsets state
   */
  public void clearAllOffsetState() {
    topicPartitionCurrentOffset.clear();
    topicPartitionEndOffset.clear();
  }

  /**
   * Get the end offset of a topic partition
   * @param topic
   * @param partition
   * @return end offset of a topic partition if there is any.
   */
  public long getEndOffset(String topic, int partition) {
    Double endOffset = topicPartitionEndOffset.get(new TopicPartition(topic, partition));
    return endOffset == null ? -1 : endOffset.longValue();
  }

  /**
   * Get consuming offset lag on a topic partition
   * @param topic
   * @param partition
   * @return end offset of a topic partition if there is any.
   */
  public long getOffsetLag(String topic, int partition) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    final Double endOffset = topicPartitionEndOffset.get(topicPartition);
    if (endOffset == null) {
      statsAccumulator.recordResult(ResultType.NO_OFFSET_LAG);
      return -1;
    }
    final Double currOffset = topicPartitionCurrentOffset.get(topicPartition);
    if (currOffset == null) {
      statsAccumulator.recordResult(ResultType.NO_OFFSET_LAG);
      return -1;
    }
    final long offsetLag = endOffset.longValue() - currOffset.longValue();
    if (offsetLag < 0) { // Invalid offset lag
      statsAccumulator.recordResult(ResultType.INVALID_OFFSET_LAG);
      return -1;
    }
    statsAccumulator.recordResult(ResultType.VALID_OFFSET_LAG);
    statsAccumulator.maybeLogAccumulatedStats(LOGGER);
    return offsetLag;
  }

  /**
   * Package private for testing purpose
   */
  public Map<ResultType, Integer> getResultsStats() {
    return statsAccumulator.getResultsStats();
  }

  /**
   * This class keeps track of results stats and can be used to log the accumulated results stats once in a while
   */
  private static class StatsAccumulator {
    private final Map<ResultType, Integer> resultsStats;
    private final Duration minLogInterval;
    private Instant lastLoggedTime;

    private StatsAccumulator(Duration minLogInterval) {
      // N.B. This map can be accessed by many functions in the ingestion path and by metrics. It is not 100%
      // clear whether concurrent access to this map is possible, but just to be on the safe side, we are going
      // to make it a threadsafe map.
      this.resultsStats = new VeniceConcurrentHashMap<>(RESULT_TYPE_VALUES.length);
      this.minLogInterval = minLogInterval;
      this.lastLoggedTime = Instant.now();
    }

    private void recordResult(ResultType resultType) {
      resultsStats.compute(resultType, (k, v) -> v == null ? 1 : v + 1);
    }

    /**
     * @param LOGGER the {@link Logger} instance which is used to log the accumulated stats
     */
    private void maybeLogAccumulatedStats(Logger LOGGER) {
      if (resultsStats.isEmpty()) {
        return;
      }
      final Instant now = Instant.now();
      final Duration timeSinceLastTimeLogged = Duration.between(lastLoggedTime, now);
      if (timeSinceLastTimeLogged.toMillis() >= minLogInterval.toMillis()) {
        LOGGER.info(
            String.format(
                "In the last %d second(s), results states are: %s",
                timeSinceLastTimeLogged.getSeconds(),
                resultsStatsToString()));
        lastLoggedTime = now;
        resultsStats.clear();
      }
    }

    private String resultsStatsToString() {
      StringJoiner sj = new StringJoiner(", ");
      for (ResultType resultType: RESULT_TYPE_VALUES) {
        sj.add(resultType + " count: " + resultsStats.getOrDefault(resultType, 0));
      }
      return sj.toString();
    }

    Map<ResultType, Integer> getResultsStats() {
      return Collections.unmodifiableMap(resultsStats);
    }
  }
}
