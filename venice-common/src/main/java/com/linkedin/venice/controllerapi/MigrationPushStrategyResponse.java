package com.linkedin.venice.controllerapi;

import java.util.Map;


public class MigrationPushStrategyResponse extends ControllerResponse {
  private Map<String, String> strategies;

  public void setStrategies(Map<String, String> strategies) {
    this.strategies = strategies;
  }

  public Map<String, String> getStrategies() {
    return this.strategies;
  }

  @Override
  public String toString() {
    return super.toString() + ", strategies: " + strategies.toString();
  }

}
