package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
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
  public Map<Integer, Long> getLatestOffsets(String topic) {
    final long startTimeMs = time.getMilliseconds();
    Map<Integer, Long> res = partitionOffsetFetcher.getLatestOffsets(topic);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_LATEST_OFFSETS,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getLatestOffset(String topic, int partition) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getLatestOffset(topic, partition);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_LATEST_OFFSET,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getLatestOffsetAndRetry(String topic, int partition, int retries) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getLatestOffsetAndRetry(topic, partition, retries);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_LATEST_OFFSET_WITH_RETRY,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public Map<Integer, Long> getOffsetsByTime(
      String topic,
      Map<TopicPartition, Long> timestampsToSearch,
      long timestamp
  ) {
    final long startTimeMs = time.getMilliseconds();
    Map<Integer, Long> res = partitionOffsetFetcher.getOffsetsByTime(topic, timestampsToSearch, timestamp);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITIONS_OFFSETS_BY_TIME,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public Map<Integer, Long> getOffsetsByTime(String topic, long timestamp) {
    final long startTimeMs = time.getMilliseconds();
    Map<Integer, Long> res = partitionOffsetFetcher.getOffsetsByTime(topic, timestamp);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_OFFSETS_BY_TIME,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getOffsetByTime(String topic, int partition, long timestamp) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getOffsetByTime(topic, partition, timestamp);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_PARTITION_OFFSET_BY_TIME,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public long getLatestProducerTimestampAndRetry(String topic, int partition, int retries) {
    final long startTimeMs = time.getMilliseconds();
    long res = partitionOffsetFetcher.getLatestProducerTimestampAndRetry(topic, partition, retries);
    stats.recordLatency(
        PartitionOffsetFetcherStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_LATEST_PRODUCER_TIMESTAMP_WITH_RETRY,
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
