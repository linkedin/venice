package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


public class InstrumentedPartitionOffsetFetcher implements PartitionOffsetFetcher {

  private final PartitionOffsetFetcher partitionOffsetFetcher;
  private final PartitionOffsetFetcherStats stats;
  private final Time time;

  public InstrumentedPartitionOffsetFetcher(
      PartitionOffsetFetcher partitionOffsetFetcher,
      PartitionOffsetFetcherStats stats,
      Time time
  ) {
    this.partitionOffsetFetcher = Utils.notNull(partitionOffsetFetcher);
    this.stats = Utils.notNull(stats);
    this.time = Utils.notNull(time);
  }

  @Override
  public Map<Integer, Long> getTopicLatestOffsets(String topic) {
    final long startTimeMs = time.getMilliseconds();
    Map<Integer, Long> res = partitionOffsetFetcher.getTopicLatestOffsets(topic);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_LATEST_OFFSETS,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getPartitionLatestOffset(String topic, int partition) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getPartitionLatestOffset(topic, partition);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_LATEST_OFFSET,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getPartitionLatestOffsetAndRetry(String topic, int partition, int retries) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getPartitionLatestOffsetAndRetry(topic, partition, retries);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_LATEST_OFFSET_WITH_RETRY,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getPartitionOffsetByTimeWithRetry(String topic, int partition, long timestamp, int maxAttempt, Duration delay) {
    final long startTimeMs = time.getMilliseconds();
    final long res = partitionOffsetFetcher.getPartitionOffsetByTimeWithRetry(topic, partition, timestamp, maxAttempt, delay);
    stats.recordLatency(
            PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_OFFSETS_BY_TIME_WITH_RETRY,
            Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public Map<Integer, Long> getTopicOffsetsByTime(String topic, long timestamp) {
    final long startTimeMs = time.getMilliseconds();
    Map<Integer, Long> res = partitionOffsetFetcher.getTopicOffsetsByTime(topic, timestamp);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_OFFSETS_BY_TIME,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getPartitionOffsetByTime(String topic, int partition, long timestamp) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getPartitionOffsetByTime(topic, partition, timestamp);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_OFFSET_BY_TIME,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getProducerTimestampOfLastDataRecord(String topic, int partition, int retries) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getProducerTimestampOfLastDataRecord(topic, partition, retries);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_LATEST_PRODUCER_TIMESTAMP_ON_DATA_RECORD_WITH_RETRY,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    final long startTimeMs = time.getMilliseconds();
    List<PartitionInfo> res = partitionOffsetFetcher.partitionsFor(topic);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.PARTITIONS_FOR,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getOffsetByTimeIfOutOfRange(topicPartition, timestamp);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_OFFSET_BY_TIME_IF_OUT_OF_RANGE,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public void close() {
    partitionOffsetFetcher.close();
  }
}
