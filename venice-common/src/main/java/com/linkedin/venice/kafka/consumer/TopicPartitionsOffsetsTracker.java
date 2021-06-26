package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

/**
 * This class tracks consumed topic partitions' offsets
 */
class TopicPartitionsOffsetsTracker {
    private static final Logger logger = Logger.getLogger(TopicPartitionsOffsetsTracker.class);
    private static final Duration DEFAULT_OFFSETS_UPDATE_INTERVAL = Duration.ofSeconds(30);

    private final Map<TopicPartition, Double> topicPartitionCurrentOffset;
    private final Map<TopicPartition, Double> topicPartitionEndOffset;
    private Instant lastMetricsCollectedTime;
    private Duration offsetsUpdateInterval;

    TopicPartitionsOffsetsTracker() {
        this(DEFAULT_OFFSETS_UPDATE_INTERVAL);
    }

    TopicPartitionsOffsetsTracker(Duration offsetsUpdateInterval) {
        this.offsetsUpdateInterval = Utils.notNull(offsetsUpdateInterval);
        this.lastMetricsCollectedTime = null;
        this.topicPartitionCurrentOffset = new VeniceConcurrentHashMap<>();
        this.topicPartitionEndOffset = new VeniceConcurrentHashMap<>();
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
                TopicPartition tp = new TopicPartition(metricName.tags().get("topic"), Integer.valueOf(metricName.tags().get("partition")));
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
}
