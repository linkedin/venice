package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


public class InstrumentedPartitionOffsetFetcher implements PartitionOffsetFetcher {
  private final PartitionOffsetFetcher partitionOffsetFetcher;
  private final PartitionOffsetFetcherStats stats;
  private final Time time;

  public InstrumentedPartitionOffsetFetcher(
      @Nonnull PartitionOffsetFetcher partitionOffsetFetcher,
      @Nonnull PartitionOffsetFetcherStats stats,
      @Nonnull Time time) {
    Validate.notNull(partitionOffsetFetcher);
    Validate.notNull(stats);
    Validate.notNull(time);
    this.partitionOffsetFetcher = partitionOffsetFetcher;
    this.stats = stats;
    this.time = time;
  }

  @Override
  public Int2LongMap getTopicLatestOffsets(String topic) {
    final long startTimeMs = time.getMilliseconds();
    Int2LongMap res = partitionOffsetFetcher.getTopicLatestOffsets(topic);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_LATEST_OFFSETS,
        Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }

  @Override
  public long getPartitionLatestOffsetAndRetry(String topic, int partition, int retries) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getPartitionLatestOffsetAndRetry(topic, partition, retries);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_LATEST_OFFSET_WITH_RETRY,
        Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }

  @Override
  public long getPartitionEarliestOffsetAndRetry(String topic, int partition, int retries) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getPartitionEarliestOffsetAndRetry(topic, partition, retries);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_EARLIEST_OFFSET_WITH_RETRY,
        Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }

  @Override
  public long getPartitionOffsetByTime(String topic, int partition, long timestamp) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getPartitionOffsetByTime(topic, partition, timestamp);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_OFFSET_BY_TIME,
        Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }

  @Override
  public long getProducerTimestampOfLastDataRecord(String topic, int partition, int retries) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getProducerTimestampOfLastDataRecord(topic, partition, retries);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_LATEST_PRODUCER_TIMESTAMP_ON_DATA_RECORD_WITH_RETRY,
        Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    final long startTimeMs = time.getMilliseconds();
    List<PartitionInfo> res = partitionOffsetFetcher.partitionsFor(topic);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.PARTITIONS_FOR,
        Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }

  @Override
  public long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getOffsetByTimeIfOutOfRange(topicPartition, timestamp);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_OFFSET_BY_TIME_IF_OUT_OF_RANGE,
        Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }

  @Override
  public void close() {
    partitionOffsetFetcher.close();
  }
}
