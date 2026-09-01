package com.linkedin.venice.router.api.routing.helix;

public interface HelixGroupSelectionStrategy {
  /**
   * Select a Helix Group for the current request, weighting the request's load contribution to the assigned
   * group by {@code weight} (for example, its key count / estimated RCU). A larger weight makes the assigned
   * group appear more loaded to subsequent selections, so variable-size multi-key requests are balanced by
   * keys/RCU rather than by raw request count. Implementations must decrement by the same weight when the
   * request finishes (or times out).
   */
  int selectGroup(long requestId, int groupCount, int weight);

  /**
   * Select a Helix Group for the current request, weighting every request equally (weight = 1). Preserves the
   * legacy request-count-based selection behavior.
   */
  default int selectGroup(long requestId, int groupCount) {
    return selectGroup(requestId, groupCount, 1);
  }

  /**
   * Notify the corresponding Helix Group that the request is completed, and the implementation will decide whether
   * any cleanup is required or not.
   */
  void finishRequest(long requestId, int groupId, double latency);

}
