package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import com.linkedin.venice.stats.routing.LatencyAdaptiveGroupSelector;
import io.tehuti.metrics.MetricsRepository;
import java.util.NavigableMap;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Latency-adaptive Helix group routing strategy for the fast client. Instead of winner-take-all least-loaded selection
 * it does a latency-driven weighted-random draw across groups, sharing the selection math with the router via
 * {@link LatencyAdaptiveGroupSelector}.
 *
 * <p>Latency is populated by {@link HelixGroupRoutingStrategy#trackRequest} (multi-key, non-retry requests). In the
 * size-agnostic mode it comes from the shared {@link HelixGroupStats#getGroupResponseWaitingTimeAvg}; in the size-aware
 * mode each request's latency is attributed to the key-count bucket it falls into and read back per bucket, so a group
 * that is fast for small batches but slow for large ones is routed accordingly. A group with no datapoint yet (or too
 * few samples in a bucket) is treated as neutral so it is neither flooded nor starved before it has been measured.
 */
public class HelixLatencyAdaptiveGroupRoutingStrategy extends HelixGroupRoutingStrategy {
  /** Key count used by the size-agnostic entry point; maps to the first (and only) bucket in non size-aware mode. */
  private static final int SINGLE_KEY_COUNT = 1;
  private final LatencyAdaptiveGroupSelector selector;
  /**
   * When {@code true} the strategy tracks and skews latency per key-count bucket, reading and recording the dedicated
   * per-(group, bucket) routing metric; when {@code false} it uses the single absolute-ms band and the shared
   * response-waiting-time metric.
   */
  private final boolean sizeAware;

  public HelixLatencyAdaptiveGroupRoutingStrategy(
      InstanceHealthMonitor instanceHealthMonitor,
      MetricsRepository metricsRepository,
      String storeName,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double skewRampExponent) {
    super(instanceHealthMonitor, metricsRepository, storeName);
    this.sizeAware = false;
    this.selector = buildSelector(evenUntilLatencyMs, fullSkewAtLatencyMs, skewRampExponent);
    this.selector.setLogContext("fast-client:" + storeName);
  }

  /** Size-aware construction: latency is tracked and skewed per key-count bucket. */
  public HelixLatencyAdaptiveGroupRoutingStrategy(
      InstanceHealthMonitor instanceHealthMonitor,
      MetricsRepository metricsRepository,
      String storeName,
      NavigableMap<Integer, Integer> fullSkewMsByLowerBound,
      double evenUntilFraction,
      double skewRampExponent,
      long minSampleCount) {
    super(instanceHealthMonitor, metricsRepository, storeName);
    this.sizeAware = true;
    this.selector = buildBucketedSelector(fullSkewMsByLowerBound, evenUntilFraction, skewRampExponent, minSampleCount);
    this.selector.setLogContext("fast-client:" + storeName);
  }

  /** Uses the default latency-adaptive knobs. */
  HelixLatencyAdaptiveGroupRoutingStrategy(InstanceHealthMonitor monitor, HelixGroupStats helixGroupStats) {
    this(
        monitor,
        helixGroupStats,
        LatencyAdaptiveGroupSelector.DEFAULT_EVEN_UNTIL_LATENCY_MS,
        LatencyAdaptiveGroupSelector.DEFAULT_FULL_SKEW_AT_LATENCY_MS,
        LatencyAdaptiveGroupSelector.DEFAULT_INTERPOLATION_EXPONENT);
  }

  HelixLatencyAdaptiveGroupRoutingStrategy(
      InstanceHealthMonitor monitor,
      HelixGroupStats helixGroupStats,
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double skewRampExponent) {
    super(monitor, helixGroupStats);
    this.sizeAware = false;
    this.selector = buildSelector(evenUntilLatencyMs, fullSkewAtLatencyMs, skewRampExponent);
  }

  HelixLatencyAdaptiveGroupRoutingStrategy(
      InstanceHealthMonitor monitor,
      HelixGroupStats helixGroupStats,
      NavigableMap<Integer, Integer> fullSkewMsByLowerBound,
      double evenUntilFraction,
      double skewRampExponent,
      long minSampleCount) {
    super(monitor, helixGroupStats);
    this.sizeAware = true;
    this.selector = buildBucketedSelector(fullSkewMsByLowerBound, evenUntilFraction, skewRampExponent, minSampleCount);
  }

  private LatencyAdaptiveGroupSelector buildSelector(
      double evenUntilLatencyMs,
      double fullSkewAtLatencyMs,
      double skewRampExponent) {
    LatencyAdaptiveGroupSelector selector = new LatencyAdaptiveGroupSelector(
        helixGroupStats::getGroupResponseWaitingTimeAvg,
        evenUntilLatencyMs,
        fullSkewAtLatencyMs,
        skewRampExponent,
        () -> ThreadLocalRandom.current().nextDouble());
    selector.setLogContext("fast-client");
    return selector;
  }

  private LatencyAdaptiveGroupSelector buildBucketedSelector(
      NavigableMap<Integer, Integer> fullSkewMsByLowerBound,
      double evenUntilFraction,
      double skewRampExponent,
      long minSampleCount) {
    LatencyAdaptiveGroupSelector selector = new LatencyAdaptiveGroupSelector(
        helixGroupStats::getRoutingLatencyAvg,
        helixGroupStats::getRoutingLatencySampleCount,
        fullSkewMsByLowerBound,
        evenUntilFraction,
        skewRampExponent,
        minSampleCount,
        () -> ThreadLocalRandom.current().nextDouble());
    selector.setLogContext("fast-client");
    return selector;
  }

  @Override
  public int getHelixGroupId(long requestId, int groupIdForOriginalRequest) {
    return getHelixGroupId(requestId, groupIdForOriginalRequest, SINGLE_KEY_COUNT);
  }

  /**
   * Pick a group by a latency-adaptive weighted-random draw, excluding {@code groupIdForOriginalRequest} so a retry
   * lands on a different group than the one already tried ({@code -1} on the original request excludes nothing). In
   * size-aware mode the draw is skewed by the bucket {@code keyCount} falls into.
   */
  @Override
  public int getHelixGroupId(long requestId, int groupIdForOriginalRequest, int keyCount) {
    int groupCnt = getGroupCount();
    if (groupCnt <= 0) {
      throw new VeniceClientException("Unexpected group count: " + groupCnt);
    }
    int startPos = (int) (requestId % groupCnt);
    return sizeAware
        ? selector.selectGroup(groupCnt, startPos, groupIdForOriginalRequest, keyCount)
        : selector.selectGroup(groupCnt, startPos, groupIdForOriginalRequest);
  }

  @Override
  protected void recordLatencySample(int groupId, int keyCount, double latency) {
    if (sizeAware) {
      helixGroupStats.recordRoutingLatency(groupId, selector.bucketKeyForKeyCount(keyCount), latency);
    } else {
      helixGroupStats.recordGroupResponseWaitingTime(groupId, latency);
    }
  }
}
