package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.utils.BatchGetConfigUtils;
import com.linkedin.venice.utils.MultiKeyLongTailRetryThresholds;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.Supplier;


/**
 * Resolves the effective batch-get long-tail retry threshold for a key count. Precedence: a fixed threshold
 * ({@code > 0}) wins; otherwise the server-provided policy, unless the local range was set explicitly; otherwise the
 * local range. Shared by retry execution and latency-adaptive routing so both always agree on the threshold.
 */
public class BatchGetRetryThresholdResolver {
  private final int fixedThresholdInMicroSeconds;
  private final boolean localRangeExplicit;
  private final String localRange;
  private final NavigableMap<Integer, Integer> localThresholdsMs;
  private final Supplier<MultiKeyLongTailRetryThresholds> serverRetryPolicy;

  public BatchGetRetryThresholdResolver(
      ClientConfig clientConfig,
      Supplier<MultiKeyLongTailRetryThresholds> serverRetryPolicy) {
    this.fixedThresholdInMicroSeconds = clientConfig.getLongTailRetryThresholdForBatchGetInMicroSeconds();
    this.localRangeExplicit = clientConfig.isBatchGetRetryRangeExplicit();
    this.localRange = clientConfig.getLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds();
    // A fixed threshold makes the range unused, so it is not parsed (matching the retry client's prior behavior).
    this.localThresholdsMs = fixedThresholdInMicroSeconds > 0
        ? Collections.emptyNavigableMap()
        : BatchGetConfigUtils.parseRetryThresholdForBatchGet(localRange);
    this.serverRetryPolicy = serverRetryPolicy;
  }

  public int getThresholdInMicroSeconds(int numKeys) {
    if (fixedThresholdInMicroSeconds > 0) {
      return fixedThresholdInMicroSeconds;
    }
    MultiKeyLongTailRetryThresholds policy = localRangeExplicit ? null : serverRetryPolicy.get();
    if (policy != null) {
      return policy.getRetryThresholdInMicroSeconds(numKeys);
    }
    // Explicit local range, or the existing default when no server policy is available.
    Map.Entry<Integer, Integer> entry = localThresholdsMs.floorEntry(numKeys);
    if (entry == null) {
      // This should never happen as the configuration will always have a continuous range starting from 1.
      throw new VeniceClientException(
          "Failed to find long tail retry threshold for batch get with " + numKeys + " keys. Please check the config: "
              + localRange);
    }
    return entry.getValue() * 1000;
  }
}
