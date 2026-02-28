package com.linkedin.venice.listener.response.stats;

import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeDoubleAndBooleanIfAbove;
import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeDoubleIfAbove;
import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeIntIfAbove;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;


public class ComputeResponseStats extends MultiKeyResponseStats {
  private double readComputeLatency = 0;
  private double readComputeDeserializationLatency = 0;
  private double readComputeSerializationLatency = 0;
  private int readComputeOutputSize = 0;
  private int totalValueSize = 0;
  private int dotProductCount = 0;
  private int cosineSimilarityCount = 0;
  private int hadamardProductCount = 0;
  private int countOperatorCount = 0;

  @Override
  public void addValueSize(int size) {
    this.totalValueSize += size;
  }

  @Override
  public void addReadComputeLatency(double latency) {
    this.readComputeLatency += latency;
  }

  @Override
  public void addReadComputeDeserializationLatency(double latency) {
    this.readComputeDeserializationLatency += latency;
  }

  @Override
  public void addReadComputeSerializationLatency(double latency) {
    this.readComputeSerializationLatency += latency;
  }

  @Override
  public void addReadComputeOutputSize(int size) {
    this.readComputeOutputSize += size;
  }

  @Override
  public void incrementDotProductCount(int count) {
    this.dotProductCount += count;
  }

  @Override
  public void incrementCountOperatorCount(int count) {
    this.countOperatorCount += count;
  }

  @Override
  public void incrementCosineSimilarityCount(int count) {
    this.cosineSimilarityCount += count;
  }

  @Override
  public void incrementHadamardProductCount(int count) {
    this.hadamardProductCount += count;
  }

  @Override
  public void recordMetrics(
      ServerHttpRequestStats stats,
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    super.recordMetrics(stats, statusEnum, statusCategory, veniceCategory);

    consumeIntIfAbove(stats::recordCosineSimilarityCount, this.cosineSimilarityCount, 0);
    consumeIntIfAbove(stats::recordCountOperatorCount, this.countOperatorCount, 0);
    consumeIntIfAbove(stats::recordDotProductCount, this.dotProductCount, 0);
    consumeIntIfAbove(stats::recordHadamardProductCount, this.hadamardProductCount, 0);
    boolean isAssembledMultiChunkLargeValue = isAssembledMultiChunkLargeValue();
    consumeDoubleAndBooleanIfAbove(
        stats::recordReadComputeDeserializationLatency,
        this.readComputeDeserializationLatency,
        isAssembledMultiChunkLargeValue,
        0);
    consumeDoubleAndBooleanIfAbove(
        stats::recordReadComputeLatency,
        this.readComputeLatency,
        isAssembledMultiChunkLargeValue,
        0);
    consumeDoubleIfAbove(stats::recordReadComputeSerializationLatency, this.readComputeSerializationLatency, 0);
    if (this.readComputeOutputSize > 0) {
      stats.recordReadComputeEfficiency((double) this.totalValueSize / readComputeOutputSize);
    }
  }

  @VisibleForTesting
  public int getResponseValueSize() {
    return this.totalValueSize;
  }

  @Override
  public void merge(ReadResponseStatsRecorder other) {
    super.merge(other);
    // Merges only the fields this subclass introduces: compute latencies, totalValueSize,
    // and per-operation counts.
    if (other instanceof ComputeResponseStats) {
      ComputeResponseStats otherStats = (ComputeResponseStats) other;
      this.readComputeLatency += otherStats.readComputeLatency;
      this.readComputeDeserializationLatency += otherStats.readComputeDeserializationLatency;
      this.readComputeSerializationLatency += otherStats.readComputeSerializationLatency;
      this.readComputeOutputSize += otherStats.readComputeOutputSize;
      this.totalValueSize += otherStats.totalValueSize;
      this.dotProductCount += otherStats.dotProductCount;
      this.cosineSimilarityCount += otherStats.cosineSimilarityCount;
      this.hadamardProductCount += otherStats.hadamardProductCount;
      this.countOperatorCount += otherStats.countOperatorCount;
    }
  }
}
