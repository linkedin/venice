package com.linkedin.venice.listener.response.stats;

import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeDoubleAndBooleanIfAbove;
import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeDoubleIfAbove;
import static com.linkedin.venice.listener.response.stats.ResponseStatsUtil.consumeIntIfAbove;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
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
  /**
   * Package-private on purpose. Only intended for use in tests.
   */
  static boolean TEST_ONLY_INJECT_SLEEP_DURING_INCREMENT_TO_SIMULATE_RACE_CONDITION = false;
  private static final int UNINITIALIZED = -1;

  private double databaseLookupLatency = 0;
  private double storageExecutionSubmissionWaitTime = UNINITIALIZED;
  private int storageExecutionQueueLen = UNINITIALIZED;
  private int multiChunkLargeValueCount = 0;

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
    int currentValue = multiChunkLargeValueCount;
    if (TEST_ONLY_INJECT_SLEEP_DURING_INCREMENT_TO_SIMULATE_RACE_CONDITION) {
      /**
       * The code below is to reliably trigger a race condition in parallel batch get metrics. Unfortunately, it is not
       * easy to cleanly inject this delay, so it is left here as a global variable. The race can still happen without
       * the sleep (assuming the stats handling code regressed to a buggy state), but it is less likely.
       *
       * See: StorageReadRequestHandlerTest.testMultiGetNotUsingKeyBytes
       */
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    multiChunkLargeValueCount = currentValue + 1;
  }

  @Override
  public void recordMetrics(ServerHttpRequestStats stats) {
    consumeDoubleAndBooleanIfAbove(
        stats::recordDatabaseLookupLatency,
        this.databaseLookupLatency,
        isAssembledMultiChunkLargeValue(),
        0);
    consumeIntIfAbove(stats::recordMultiChunkLargeValueCount, this.multiChunkLargeValueCount, 0);
    consumeIntIfAbove(stats::recordSuccessRequestKeyCount, getRecordCount(), 0);
    consumeIntIfAbove(stats::recordStorageExecutionQueueLen, this.storageExecutionQueueLen, UNINITIALIZED);

    recordUnmergedMetrics(stats);

    // Other metrics can be recorded by subclasses
  }

  @Override
  public void recordUnmergedMetrics(ServerHttpRequestStats stats) {
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
