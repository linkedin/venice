package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;


/**
 * Statistics for compute aggregation operations.
 */
public class ComputeAggregationResponseStats extends AbstractReadResponseStats {
  private double aggregationLatency = 0;
  private double aggregationDeserializationLatency = 0;
  private double aggregationSerializationLatency = 0;
  private int aggregationOutputSize = 0;
  private int totalValueSize = 0;
  private int countByValueCount = 0;
  private int countByBucketCount = 0;

  @Override
  public void addValueSize(int size) {
    this.totalValueSize += size;
  }

  @Override
  public void addReadComputeLatency(double latency) {
    this.aggregationLatency += latency;
  }

  @Override
  public void addReadComputeDeserializationLatency(double latency) {
    this.aggregationDeserializationLatency += latency;
  }

  @Override
  public void addReadComputeSerializationLatency(double latency) {
    this.aggregationSerializationLatency += latency;
  }

  @Override
  public void addReadComputeOutputSize(int size) {
    this.aggregationOutputSize += size;
  }

  public void incrementCountByValueCount(int count) {
    this.countByValueCount += count;
  }

  public void incrementCountByBucketCount(int count) {
    this.countByBucketCount += count;
  }

  @Override
  public int getRecordCount() {
    return 1; // Aggregation responses return a single aggregated result
  }

  @Override
  public void addKeySize(int size) {
    // Not used for aggregation stats
  }

  @Override
  public void recordMetrics(ServerHttpRequestStats stats) {
    super.recordMetrics(stats);
    // TODO: Add aggregation-specific metrics when available
  }

  @Override
  public void merge(ReadResponseStatsRecorder other) {
    super.merge(other);
    if (other instanceof ComputeAggregationResponseStats) {
      ComputeAggregationResponseStats otherStats = (ComputeAggregationResponseStats) other;
      this.aggregationLatency += otherStats.aggregationLatency;
      this.aggregationDeserializationLatency += otherStats.aggregationDeserializationLatency;
      this.aggregationSerializationLatency += otherStats.aggregationSerializationLatency;
      this.aggregationOutputSize += otherStats.aggregationOutputSize;
      this.totalValueSize += otherStats.totalValueSize;
      this.countByValueCount += otherStats.countByValueCount;
      this.countByBucketCount += otherStats.countByBucketCount;
    }
  }
}
