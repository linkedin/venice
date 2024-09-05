package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;


/**
 * This class is used to record stats associated with a read response. This container may hold two types of stats:
 *
 * Some stats are "mergeable", which means that we can aggregate multiple instances of {@link ReadResponseStatsRecorder}
 * into one via {@link #merge(ReadResponseStatsRecorder)}, and call {@link #recordMetrics(ServerHttpRequestStats)} onto
 * the merged instance. Doing so achieves the same outcome as calling the {@link #recordMetrics(ServerHttpRequestStats)}
 * API on all individual instances, but from a performance standpoint, it may be preferable to do fewer calls to the
 * stats object, since it needs to correctly handle high concurrency.
 *
 * Some other stats are not mergeable, and must be recorded individually. Those stats are not going to be merged when
 * calling {@link #merge(ReadResponseStatsRecorder)}, and the code path which makes use of the merge is responsible for
 * calling the {@link #recordUnmergedMetrics(ServerHttpRequestStats)} API on the remaining objects.
 */
public interface ReadResponseStatsRecorder {
  /**
   * Record all metrics, including those which can and cannot be merged.
   *
   * @param stats the {@link ServerHttpRequestStats} object to record stats into.
   */
  void recordMetrics(ServerHttpRequestStats stats);

  /**
   * Record metrics which are not mergeable by the {@link #merge(ReadResponseStatsRecorder)} function.
   *
   * @param stats the {@link ServerHttpRequestStats} object to record stats into.
   */
  void recordUnmergedMetrics(ServerHttpRequestStats stats);

  /**
   * Merge the stats contained in this instance with those contained in the {@param other} instance.
   *
   * @param other instance to merge with.
   */
  void merge(ReadResponseStatsRecorder other);
}
