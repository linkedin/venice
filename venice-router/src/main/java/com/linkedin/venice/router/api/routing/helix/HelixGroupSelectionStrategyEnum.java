package com.linkedin.venice.router.api.routing.helix;

public enum HelixGroupSelectionStrategyEnum {
  /**
   * This strategy will try to distribute the load to each group evenly all the time.
   */
  ROUND_ROBIN(HelixGroupRoundRobinStrategy.class),
  /**
   * This strategy will try to distribute the load to each group according to the capacity of each group.
   */
  LEAST_LOADED(HelixGroupLeastLoadedStrategy.class);

  private final Class<? extends HelixGroupSelectionStrategy> strategyClass;

  HelixGroupSelectionStrategyEnum(Class<? extends HelixGroupSelectionStrategy> strategyClass) {
    this.strategyClass = strategyClass;
  }

  public Class<? extends HelixGroupSelectionStrategy> getStrategyClass() {
    return this.strategyClass;
  }
}
