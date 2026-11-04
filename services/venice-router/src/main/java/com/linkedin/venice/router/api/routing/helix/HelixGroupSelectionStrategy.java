package com.linkedin.venice.router.api.routing.helix;

public interface HelixGroupSelectionStrategy {
  /**
   * Select a Helix Group for the current request.
   */
  int selectGroup(long requestId, int groupCount);

  /**
   * Notify the corresponding Helix Group that the request is completed, and the implementation will decide whether
   * any cleanup is required or not.
   */
  void finishRequest(long requestId, int groupId, double latency);

  /**
   * Size-aware group selection: {@code keyCount} is the number of keys in the request, letting a strategy route by
   * request size. Defaults to the size-agnostic {@link #selectGroup(long, int)} so strategies that do not care about
   * request size (round-robin, least-loaded) need no change.
   */
  default int selectGroup(long requestId, int groupCount, int keyCount) {
    return selectGroup(requestId, groupCount);
  }

  /**
   * Size-aware request completion: records the latency under the request's {@code keyCount} so a size-aware strategy
   * can attribute it to the matching bucket. Defaults to the size-agnostic {@link #finishRequest(long, int, double)}.
   */
  default void finishRequest(long requestId, int groupId, int keyCount, double latency) {
    finishRequest(requestId, groupId, latency);
  }

  /**
   * Success-aware request completion: {@code success} is {@code false} when the request failed (error or throttled)
   * response, letting a latency-adaptive strategy keep the latency sample out of its routing signal so a fast-failing
   * group is not mistaken for a fast-serving one. Group bookkeeping still happens regardless. Defaults to the
   * success-agnostic {@link #finishRequest(long, int, int, double)}.
   */
  default void finishRequest(long requestId, int groupId, int keyCount, double latency, boolean success) {
    finishRequest(requestId, groupId, keyCount, latency);
  }

}
