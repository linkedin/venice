package com.linkedin.venice.meta;

/**
 * Cluster level metadata for all routers.
 */
public class RoutersClusterConfig {
  private int expectedRouterCount;

  private boolean throttlingEnabled = true;
  private boolean maxCapacityProtectionEnabled = true;

  public RoutersClusterConfig() {
  }

  public int getExpectedRouterCount() {
    return expectedRouterCount;
  }

  public void setExpectedRouterCount(int expectedRouterCount) {
    this.expectedRouterCount = expectedRouterCount;
  }

  public boolean isThrottlingEnabled() {
    return throttlingEnabled;
  }

  public void setThrottlingEnabled(boolean throttlingEnabled) {
    this.throttlingEnabled = throttlingEnabled;
  }

  public boolean isMaxCapacityProtectionEnabled() {
    return maxCapacityProtectionEnabled;
  }

  public void setMaxCapacityProtectionEnabled(boolean maxCapacityProtectionEnabled) {
    this.maxCapacityProtectionEnabled = maxCapacityProtectionEnabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RoutersClusterConfig config = (RoutersClusterConfig) o;

    if (expectedRouterCount != config.expectedRouterCount) {
      return false;
    }
    if (throttlingEnabled != config.throttlingEnabled) {
      return false;
    }
    return maxCapacityProtectionEnabled == config.maxCapacityProtectionEnabled;
  }

  @Override
  public int hashCode() {
    int result = expectedRouterCount;
    result = 31 * result + (throttlingEnabled ? 1 : 0);
    result = 31 * result + (maxCapacityProtectionEnabled ? 1 : 0);
    return result;
  }

  public RoutersClusterConfig cloneRoutesClusterConfig() {
    RoutersClusterConfig config = new RoutersClusterConfig();
    config.setThrottlingEnabled(isThrottlingEnabled());
    config.setMaxCapacityProtectionEnabled(isMaxCapacityProtectionEnabled());
    config.setExpectedRouterCount(getExpectedRouterCount());
    return config;
  }
}
