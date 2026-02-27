package com.linkedin.venice.listener.response.stats;

import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeDoubleAndBooleanIfAbove;
import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeDoubleIfAbove;
import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeIntIfAbove;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.LatencyUtils;


/**
 * This abstract class is the container for response stats. The stats can be accumulated via the APIs provided by
 * {@link ReadResponseStats}, and then recorded using the API from {@link ReadResponseStatsRecorder}.
 *
 * The class hierarchy aims to minimize the amount of state required for any given response, based on its type and
 * relevant server configs:
 *
 * - {@link AbstractReadResponseStats}
 * +-- {@link SingleGetResponseStats}
 * +-- {@link MultiKeyResponseStats}
 *   +-- {@link MultiGetResponseStatsWithSizeProfiling}
 *   +-- {@link ComputeResponseStats}
 *     +-- {@link ComputeResponseStatsWithSizeProfiling}
 */
public abstract class AbstractReadResponseStats implements ReadResponseStats, ReadResponseStatsRecorder {
  private static final int UNINITIALIZED = -1;

  private double databaseLookupLatency = 0;
  private double storageExecutionSubmissionWaitTime = UNINITIALIZED;
  private int storageExecutionQueueLen = UNINITIALIZED;
  protected int multiChunkLargeValueCount = 0;
  private int keyNotFoundCount = 0;

  protected abstract int getRecordCount();

  @Override
  public long getCurrentTimeInNanos() {
    return System.nanoTime();
  }

  @Override
  public void addDatabaseLookupLatency(long startTimeInNanos) {
    this.databaseLookupLatency += LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNanos);
  }

  @Override
  public void setStorageExecutionSubmissionWaitTime(double storageExecutionSubmissionWaitTime) {
    this.storageExecutionSubmissionWaitTime = storageExecutionSubmissionWaitTime;
  }

  @Override
  public void setStorageExecutionQueueLen(int storageExecutionQueueLen) {
    this.storageExecutionQueueLen = storageExecutionQueueLen;
  }

  public void incrementMultiChunkLargeValueCount() {
    this.multiChunkLargeValueCount++;
  }

  @Override
  public void incrementKeyNotFoundCount() {
    this.keyNotFoundCount++;
  }

  public int getKeyNotFoundCount() {
    return this.keyNotFoundCount;
  }

  @Override
  public void recordMetrics(
      ServerHttpRequestStats stats,
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    consumeDoubleAndBooleanIfAbove(
        stats::recordDatabaseLookupLatency,
        this.databaseLookupLatency,
        isAssembledMultiChunkLargeValue(),
        0);
    consumeIntIfAbove(stats::recordMultiChunkLargeValueCount, this.multiChunkLargeValueCount, 0);
    consumeIntIfAbove(stats::recordStorageExecutionQueueLen, this.storageExecutionQueueLen, UNINITIALIZED);
    consumeIntIfAbove(stats::recordKeyNotFoundCount, this.keyNotFoundCount, 0);

    recordUnmergedMetrics(stats, statusEnum, statusCategory, veniceCategory);

    // Other metrics can be recorded by subclasses
  }

  @Override
  public void recordUnmergedMetrics(
      ServerHttpRequestStats stats,
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    consumeDoubleIfAbove(
        stats::recordStorageExecutionHandlerSubmissionWaitTime,
        this.storageExecutionSubmissionWaitTime,
        UNINITIALIZED);
  }

  @Override
  public void merge(ReadResponseStatsRecorder other) {
    if (other instanceof AbstractReadResponseStats) {
      AbstractReadResponseStats otherStats = (AbstractReadResponseStats) other;
      this.databaseLookupLatency += otherStats.databaseLookupLatency;
      this.multiChunkLargeValueCount += otherStats.multiChunkLargeValueCount;
      this.keyNotFoundCount += otherStats.keyNotFoundCount;
    }
  }

  protected boolean isAssembledMultiChunkLargeValue() {
    return this.multiChunkLargeValueCount > 0;
  }

  // Below are the read compute functions which are part of the API but should only be overridden and called when
  // appropriate

  /**
   * This defensive code should never be called. If it is, then some refactoring caused a regression.
   */
  private void throwUnsupportedMetric() {
    throw new IllegalStateException(this.getClass().getSimpleName() + " does not support compute metrics.");
  }

  @Override
  public void addReadComputeLatency(double latency) {
    throwUnsupportedMetric();
  }

  @Override
  public void addReadComputeDeserializationLatency(double latency) {
    throwUnsupportedMetric();
  }

  @Override
  public void addReadComputeSerializationLatency(double latency) {
    throwUnsupportedMetric();
  }

  @Override
  public void addReadComputeOutputSize(int size) {
    throwUnsupportedMetric();
  }

  @Override
  public void incrementDotProductCount(int count) {
    throwUnsupportedMetric();
  }

  @Override
  public void incrementCountOperatorCount(int count) {
    throwUnsupportedMetric();
  }

  @Override
  public void incrementCosineSimilarityCount(int count) {
    throwUnsupportedMetric();
  }

  @Override
  public void incrementHadamardProductCount(int count) {
    throwUnsupportedMetric();
  }
}
