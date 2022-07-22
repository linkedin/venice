package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class TopicPartitionsOffsetsTrackerTest {

    private static final Duration OFFSETS_UPDATE_INTERVAL = Duration.ZERO; // No interval
    private static final String TOPIC_1 = "topic_1";
    private static final String TOPIC_2 = "topic_2";
    private static final int PARTITION_ID = 2;
    private TopicPartitionsOffsetsTracker topicPartitionsOffsetsTracker;

    @BeforeMethod
    public void initTopicPartitionsOffsetsTracker() {
        topicPartitionsOffsetsTracker = new TopicPartitionsOffsetsTracker(OFFSETS_UPDATE_INTERVAL);
    }

    @Test
    public void testNoUpdateWithRecords() {
        Assert.assertEquals(topicPartitionsOffsetsTracker.getEndOffset(TOPIC_1, PARTITION_ID), OptionalLong.empty());
        Assert.assertEquals(topicPartitionsOffsetsTracker.getEndOffset(TOPIC_2, PARTITION_ID), OptionalLong.empty());
        Assert.assertTrue(topicPartitionsOffsetsTracker.getResultsStats().isEmpty());

        Assert.assertEquals(topicPartitionsOffsetsTracker.getOffsetLag(TOPIC_1, PARTITION_ID), OptionalLong.empty());
        Assert.assertEquals(topicPartitionsOffsetsTracker.getOffsetLag(TOPIC_2, PARTITION_ID), OptionalLong.empty());
        Assert.assertEquals(topicPartitionsOffsetsTracker.getResultsStats().size(), 1);
        Assert.assertEquals(
                topicPartitionsOffsetsTracker.getResultsStats().get(TopicPartitionsOffsetsTracker.ResultType.NO_OFFSET_LAG).intValue(),
                2
        );
    }

    @Test
    public void testUpdateWithRecords() {
        // Setup: 2 topic partitions and each of them has one record with corresponding offset
        final long firstOffset = 5;
        final long secondOffset = 10;
        final long firstPartitionLag = 6;
        final long secondPartitionLag = 124;

        TopicPartition firstTopicPartition = new TopicPartition(TOPIC_1, PARTITION_ID);
        TopicPartition secondTopicPartition = new TopicPartition(TOPIC_2, PARTITION_ID);

        Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> recordsMap = new HashMap<>(2);
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> firsRecord = new ConsumerRecord<>(
                firstTopicPartition.topic(),
                firstTopicPartition.partition(),
                firstOffset,
                mock(KafkaKey.class),
                mock(KafkaMessageEnvelope.class)
        );
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> secondRecord = new ConsumerRecord<>(
                secondTopicPartition.topic(),
                secondTopicPartition.partition(),
                secondOffset,
                mock(KafkaKey.class),
                mock(KafkaMessageEnvelope.class)
        );

        recordsMap.put(firstTopicPartition, Collections.singletonList(firsRecord));
        recordsMap.put(secondTopicPartition, Collections.singletonList(secondRecord));
        ConsumerRecords<KafkaKey, KafkaMessageEnvelope> mockRecords = new ConsumerRecords<>(recordsMap);

        Map<MetricName, Metric> mockMetrics = new HashMap<>();
        // Set up partition record lag for the first topic partition
        Map<String, String> metricsTags = new HashMap<>(2);
        metricsTags.put("topic", TOPIC_1);
        metricsTags.put("partition", String.valueOf(PARTITION_ID));
        MetricName metricName = new MetricName("records-lag", "", "", metricsTags);
        Metric metricValue = mock(Metric.class);
        when(metricValue.metricValue()).thenReturn((double) firstPartitionLag);
        mockMetrics.put(metricName, metricValue);

        // Set up partition record lag for the second topic partition
        metricsTags = new HashMap<>(2);
        metricsTags.put("topic", TOPIC_2);
        metricsTags.put("partition", String.valueOf(PARTITION_ID));
        metricName = new MetricName("records-lag", "", "", metricsTags);
        metricValue = mock(Metric.class);
        when(metricValue.metricValue()).thenReturn((double) secondPartitionLag);
        mockMetrics.put(metricName, metricValue);

        topicPartitionsOffsetsTracker.updateEndOffsets(mockRecords, mockMetrics);

        Assert.assertEquals(topicPartitionsOffsetsTracker.getOffsetLag(TOPIC_1, PARTITION_ID), OptionalLong.of(firstPartitionLag));
        Assert.assertEquals(topicPartitionsOffsetsTracker.getResultsStats().size(), 1);
        Assert.assertEquals(
                topicPartitionsOffsetsTracker.getResultsStats().get(TopicPartitionsOffsetsTracker.ResultType.VALID_OFFSET_LAG).intValue(),
                1
        );

        Assert.assertEquals(topicPartitionsOffsetsTracker.getOffsetLag(TOPIC_2, PARTITION_ID), OptionalLong.of(secondPartitionLag));
        Assert.assertEquals(topicPartitionsOffsetsTracker.getResultsStats().size(), 1);
        Assert.assertEquals(
                topicPartitionsOffsetsTracker.getResultsStats().get(TopicPartitionsOffsetsTracker.ResultType.VALID_OFFSET_LAG).intValue(),
                2
        );

        // End offset == current offset + lag
        Assert.assertEquals(topicPartitionsOffsetsTracker.getEndOffset(TOPIC_1, PARTITION_ID), OptionalLong.of(firstOffset + firstPartitionLag));
        Assert.assertEquals(topicPartitionsOffsetsTracker.getEndOffset(TOPIC_2, PARTITION_ID), OptionalLong.of(secondOffset + secondPartitionLag));

        topicPartitionsOffsetsTracker.removeTrackedOffsets(new TopicPartition(TOPIC_1, PARTITION_ID));
        Assert.assertEquals(topicPartitionsOffsetsTracker.getEndOffset(TOPIC_1, PARTITION_ID), OptionalLong.empty());
        Assert.assertEquals(topicPartitionsOffsetsTracker.getEndOffset(TOPIC_2, PARTITION_ID), OptionalLong.of(secondOffset + secondPartitionLag));

        topicPartitionsOffsetsTracker.clearAllOffsetState();
        Assert.assertEquals(topicPartitionsOffsetsTracker.getEndOffset(TOPIC_1, PARTITION_ID), OptionalLong.empty());
        Assert.assertEquals(topicPartitionsOffsetsTracker.getEndOffset(TOPIC_2, PARTITION_ID), OptionalLong.empty());
        Assert.assertEquals(topicPartitionsOffsetsTracker.getResultsStats().size(), 1);
        Assert.assertEquals(
                topicPartitionsOffsetsTracker.getResultsStats().get(TopicPartitionsOffsetsTracker.ResultType.VALID_OFFSET_LAG).intValue(),
                2
        );
    }
}
