package com.linkedin.venice.router.api.routing.helix;

public interface HelixGroupSelectionStrategy {
  /** Sentinel latency budget meaning "unknown / not applicable"; latency-budget-aware strategies ignore the gate. */
  int NO_LATENCY_BUDGET = -1;

  /**
   * Select a Helix Group for the current request.
   */
  int selectGroup(long requestId, int groupCount);

  /**
   * Select a Helix Group for the current request given a per-request latency budget (ms) -- typically the
   * long-tail retry threshold for the request's key range (see
   * {@link com.linkedin.venice.router.api.path.VenicePath#getLongTailRetryThresholdMs()}). Strategies that are
   * latency-budget-aware use it to shed traffic from groups whose observed latency approaches the budget;
   * strategies that are not simply ignore it. The default delegates to {@link #selectGroup(long, int)}.
   *
   * @param latencyBudgetMs the per-request latency budget in ms, or a non-positive value if unknown/not applicable.
   */
  default int selectGroup(long requestId, int groupCount, int latencyBudgetMs) {
    return selectGroup(requestId, groupCount);
  }

  /**
   * Notify the corresponding Helix Group that the request is completed, and the implementation will decide whether
   * any cleanup is required or not.
   */
  void finishRequest(long requestId, int groupId, double latency);

}
