package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Cluster level metadata for all routers.
 */
public class RoutersClusterConfig {
  private final static Logger logger = Logger.getLogger(RoutersClusterConfig.class);

  private int expectedRouterCount;

  private boolean throttlingEnabled = true;
  private boolean quotaRebalanceEnabled = true;
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

  public boolean isQuotaRebalanceEnabled() {
    return quotaRebalanceEnabled;
  }

  public void setQuotaRebalanceEnabled(boolean quotaRebalanceEnabled) {
    this.quotaRebalanceEnabled = quotaRebalanceEnabled;
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
    if (quotaRebalanceEnabled != config.quotaRebalanceEnabled) {
      return false;
    }
    return maxCapacityProtectionEnabled == config.maxCapacityProtectionEnabled;
  }

  @Override
  public int hashCode() {
    int result = expectedRouterCount;
    result = 31 * result + (throttlingEnabled ? 1 : 0);
    result = 31 * result + (quotaRebalanceEnabled ? 1 : 0);
    result = 31 * result + (maxCapacityProtectionEnabled ? 1 : 0);
    return result;
  }

  public RoutersClusterConfig cloneRoutesClusterConfig(){
    RoutersClusterConfig config = new RoutersClusterConfig();
    config.setThrottlingEnabled(isThrottlingEnabled());
    config.setQuotaRebalanceEnabled(isQuotaRebalanceEnabled());
    config.setMaxCapacityProtectionEnabled(isMaxCapacityProtectionEnabled());
    config.setExpectedRouterCount(getExpectedRouterCount());
    return config;
  }
}
