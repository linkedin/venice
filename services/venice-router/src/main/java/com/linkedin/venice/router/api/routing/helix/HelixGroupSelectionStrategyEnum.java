package com.linkedin.venice.router.api.routing.helix;

public enum HelixGroupSelectionStrategyEnum {
  /**
   * This strategy will try to distribute the load to each group evenly all the time.
   */
  ROUND_ROBIN(HelixGroupRoundRobinStrategy.class),
  /**
   * This strategy will try to distribute the load to each group according to the capacity of each group.
   */
  LEAST_LOADED(HelixGroupLeastLoadedStrategy.class),
  /**
   * This strategy is a best-effort latency equaliser: it routes evenly while the groups' measured latencies are
   * close and progressively sheds traffic off the slower groups onto the faster ones as their latency spread
   * widens, using measured latency as the only signal. See {@link HelixGroupWeightedLeastLoadedStrategy}.
   */
  LATENCY_EQUALIZED(HelixGroupWeightedLeastLoadedStrategy.class);

  private final Class<? extends HelixGroupSelectionStrategy> strategyClass;

  HelixGroupSelectionStrategyEnum(Class<? extends HelixGroupSelectionStrategy> strategyClass) {
    this.strategyClass = strategyClass;
  }

  public Class<? extends HelixGroupSelectionStrategy> getStrategyClass() {
    return this.strategyClass;
  }
}
