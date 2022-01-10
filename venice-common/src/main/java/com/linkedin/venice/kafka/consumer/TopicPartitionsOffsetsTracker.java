package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;


/**
 * This class tracks consumed topic partitions' offsets
 */
class TopicPartitionsOffsetsTracker {
    enum RESULT_TYPE {
        VALID_OFFSET_LAG,
        NO_OFFSET_LAG,
        INVALID_OFFSET_LAG
    }

    private static final Logger logger = LogManager.getLogger(TopicPartitionsOffsetsTracker.class);
    private static final Duration DEFAULT_OFFSETS_UPDATE_INTERVAL = Duration.ofSeconds(30);
    private static final Duration DEFAULT_MIN_LOG_INTERVAL = Duration.ofMinutes(3);

    private final Map<TopicPartition, Double> topicPartitionCurrentOffset;
    private final Map<TopicPartition, Double> topicPartitionEndOffset;
    private Instant lastMetricsCollectedTime;
    private final Duration offsetsUpdateInterval;
    private final StatsAccumulator statsAccumulator;

    TopicPartitionsOffsetsTracker() {
        this(DEFAULT_OFFSETS_UPDATE_INTERVAL);
    }

    TopicPartitionsOffsetsTracker(Duration offsetsUpdateInterval) {
        this(offsetsUpdateInterval, DEFAULT_MIN_LOG_INTERVAL);
    }

    TopicPartitionsOffsetsTracker(@Nonnull Duration offsetsUpdateInterval, Duration minLogInterval) {
        Validate.notNull(offsetsUpdateInterval);
        this.offsetsUpdateInterval = offsetsUpdateInterval;
        this.lastMetricsCollectedTime = null;
        this.topicPartitionCurrentOffset = new VeniceConcurrentHashMap<>();
        this.topicPartitionEndOffset = new VeniceConcurrentHashMap<>();
        this.statsAccumulator = new StatsAccumulator(minLogInterval);
    }

    /**
     * Update the end offsets of consumed partitions based on consumer metrics and given consumed records.
     * For each topic partition, end offset == consumed current offset + offset lag
     *
     * @param records consumed records
     */
    void updateEndOffsets(ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records, Map<MetricName, ? extends Metric> metrics) {
        if (lastMetricsCollectedTime != null && LatencyUtils.getElapsedTimeInMs(lastMetricsCollectedTime.toEpochMilli()) < offsetsUpdateInterval.toMillis()) {
            return; // Not yet
        }
        lastMetricsCollectedTime = Instant.now();

        // Update current offset cache for all topics partition.
        for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            topicPartitionCurrentOffset.put(tp, (double) record.offset());
        }

        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            final MetricName metricName = entry.getKey();
            final Metric metric = entry.getValue();

            if (isMetricEntryRecordsLag(metricName, metric)) {
                TopicPartition tp = new TopicPartition(metricName.tags().get("topic"), Integer.parseInt(metricName.tags().get("partition")));
                Double currOffset = topicPartitionCurrentOffset.get(tp);
                if (currOffset != null) {
                    topicPartitionEndOffset.put(tp, currOffset + ((Double) metric.metricValue()));
                }
            }
        }
    }

    private boolean isMetricEntryRecordsLag(MetricName metricName, Metric metric) {
        try {
            Object value = metric.metricValue();
            return (value instanceof Double) && Objects.equals(metricName.name(), "records-lag");

        } catch (Exception e) {
            logger.warn("Caught exception: " + e.getMessage() + " when attempting to get consumer metrics. "
                    + "Incomplete metrics might be returned.");
            return false;
        }
    }

    /**
     * Remove tracked offsets state of a topic partition.
     *
     * @param topicPartition
     */
    void removeTrackedOffsets(TopicPartition topicPartition) {
        topicPartitionCurrentOffset.remove(topicPartition);
        topicPartitionEndOffset.remove(topicPartition);
    }

    /**
     * Clear all tracked offsets state
     */
    void clearAllOffsetState() {
        topicPartitionCurrentOffset.clear();
        topicPartitionEndOffset.clear();
    }

    /**
     * Get the end offset of a topic partition
     * @param topic
     * @param partition
     * @return end offset of a topic partition if there is any.
     */
    Optional<Long> getEndOffset(String topic, int partition) {
        Double endOffset = topicPartitionEndOffset.get(new TopicPartition(topic, partition));
        return endOffset == null ? Optional.empty() : Optional.of(endOffset.longValue());
    }

    /**
     * Get consuming offset lag on a topic partition
     * @param topic
     * @param partition
     * @return end offset of a topic partition if there is any.
     */
    Optional<Long> getOffsetLag(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        final Double endOffset = topicPartitionEndOffset.get(topicPartition);
        if (endOffset == null) {
            statsAccumulator.recordResult(RESULT_TYPE.NO_OFFSET_LAG);
            return Optional.empty();
        }
        final Double currOffset = topicPartitionCurrentOffset.get(topicPartition);
        if (currOffset == null) {
            statsAccumulator.recordResult(RESULT_TYPE.NO_OFFSET_LAG);
            return Optional.empty();
        }
        final long offsetLag = endOffset.longValue() - currOffset.longValue();
        if (offsetLag < 0) { // Invalid offset lag
            statsAccumulator.recordResult(RESULT_TYPE.INVALID_OFFSET_LAG);
            return Optional.empty();
        }
        statsAccumulator.recordResult(RESULT_TYPE.VALID_OFFSET_LAG);
        if (statsAccumulator.maybeLogAccumulatedStats(logger)) {
            statsAccumulator.clearAccumulatedStats();
        }
        return Optional.of(offsetLag);
    }

    /**
     * Package private for testing purpose
     */
    Map<RESULT_TYPE, Integer> getResultsStats() {
        return statsAccumulator.getResultsStats();
    }

    /**
     * This class keeps track of results stats and can be used to log the accumulated results stats once in a while
     */
    private static class StatsAccumulator {
        private final Map<RESULT_TYPE, Integer> resultsStats;
        private final Duration minLogInterval;
        private Instant lastLoggedTime;

        private StatsAccumulator(Duration minLogInterval) {
            this.resultsStats = new HashMap<>(RESULT_TYPE.values().length);
            this.minLogInterval = minLogInterval;
            this.lastLoggedTime = Instant.now();
        }

        private void recordResult(RESULT_TYPE resultType) {
            resultsStats.put(resultType, resultsStats.getOrDefault(resultType, 0) + 1);
        }

        /**
         * @param logger the logger instance which is used to log the accumulated stats
         * @return True if accumulated stats are logged and vice versa
         */
        private boolean maybeLogAccumulatedStats(Logger logger) {
            if (resultsStats.isEmpty()) {
                return false;
            }
            final Instant now = Instant.now();
            final Duration timeSinceLastTimeLogged = Duration.between(lastLoggedTime, now);
            if (timeSinceLastTimeLogged.toMillis() >= minLogInterval.toMillis()) {
                logger.info(String.format(
                        "In the last %d second(s), results states are: %s",
                        timeSinceLastTimeLogged.getSeconds(),
                        resultsStatsToString()
                ));
                lastLoggedTime = now;
                return true;
            }
            return false;
        }

        private void clearAccumulatedStats() {
            resultsStats.clear();
        }

        private String resultsStatsToString() {
            StringJoiner sj = new StringJoiner(", ");
            for (RESULT_TYPE resultType : RESULT_TYPE.values()) {
                sj.add(resultType + " count: " + resultsStats.getOrDefault(resultType, 0));
            }
            return sj.toString();
        }

        Map<RESULT_TYPE, Integer> getResultsStats() {
            return Collections.unmodifiableMap(resultsStats);
        }
    }
}
