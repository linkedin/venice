package com.linkedin.venice.fastclient.meta;

import com.linkedin.r2.transport.common.Client;


public class InstanceHealthMonitorConfig {
  private final long routingRequestDefaultTimeoutMS;
  private final long routingTimedOutRequestCounterResetDelayMS; // 10s
  private final int routingPendingRequestCounterInstanceBlockThreshold;

  private final long heartBeatIntervalSeconds;
  private final long heartBeatRequestTimeoutMS;

  private final int loadControllerWindowSizeInSec;
  private final int loadControllerRejectionRatioUpdateIntervalInSec;
  private final double loadControllerMaxRejectionRatio;
  private final double loadControllerAcceptMultiplier;
  private final boolean loadControllerEnabled;

  private final Client client;

  public InstanceHealthMonitorConfig(Builder builder) {
    this.routingRequestDefaultTimeoutMS = builder.routingRequestDefaultTimeoutMS;
    this.routingTimedOutRequestCounterResetDelayMS = builder.routingTimedOutRequestCounterResetDelayMS;
    this.routingPendingRequestCounterInstanceBlockThreshold =
        builder.routingPendingRequestCounterInstanceBlockThreshold;
    this.heartBeatIntervalSeconds = builder.heartBeatIntervalSeconds;
    this.heartBeatRequestTimeoutMS = builder.heartBeatRequestTimeoutMS;
    this.client = builder.client;
    this.loadControllerWindowSizeInSec = builder.loadControllerWindowSizeInSec;
    this.loadControllerRejectionRatioUpdateIntervalInSec = builder.loadControllerRejectionRatioUpdateIntervalInSec;
    this.loadControllerMaxRejectionRatio = builder.loadControllerMaxRejectionRatio;
    this.loadControllerAcceptMultiplier = builder.loadControllerAcceptMultiplier;
    this.loadControllerEnabled = builder.loadControllerEnabled;
  }

  public long getRoutingRequestDefaultTimeoutMS() {
    return routingRequestDefaultTimeoutMS;
  }

  public long getRoutingTimedOutRequestCounterResetDelayMS() {
    return routingTimedOutRequestCounterResetDelayMS;
  }

  public int getRoutingPendingRequestCounterInstanceBlockThreshold() {
    return routingPendingRequestCounterInstanceBlockThreshold;
  }

  public long getHeartBeatIntervalSeconds() {
    return heartBeatIntervalSeconds;
  }

  public long getHeartBeatRequestTimeoutMS() {
    return heartBeatRequestTimeoutMS;
  }

  public Client getClient() {
    return client;
  }

  public int getLoadControllerWindowSizeInSec() {
    return loadControllerWindowSizeInSec;
  }

  public int getLoadControllerRejectionRatioUpdateIntervalInSec() {
    return loadControllerRejectionRatioUpdateIntervalInSec;
  }

  public double getLoadControllerMaxRejectionRatio() {
    return loadControllerMaxRejectionRatio;
  }

  public double getLoadControllerAcceptMultiplier() {
    return loadControllerAcceptMultiplier;
  }

  public boolean isLoadControllerEnabled() {
    return loadControllerEnabled;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    /**
     * Default timeout is 1s and the underlying transport client doesn't support request-level timeout properly.
     */
    private long routingRequestDefaultTimeoutMS = 1000;
    private int routingPendingRequestCounterInstanceBlockThreshold = 50;

    private long routingTimedOutRequestCounterResetDelayMS = 10000; // 10s

    private long heartBeatIntervalSeconds = 30;
    private long heartBeatRequestTimeoutMS = 10000;

    private int loadControllerWindowSizeInSec = 30;
    private int loadControllerRejectionRatioUpdateIntervalInSec = 3;
    private double loadControllerMaxRejectionRatio = 0.9;
    private double loadControllerAcceptMultiplier = 2.0;
    private boolean loadControllerEnabled = false;

    private Client client;

    public Builder setRoutingRequestDefaultTimeoutMS(long routingRequestDefaultTimeoutMS) {
      this.routingRequestDefaultTimeoutMS = routingRequestDefaultTimeoutMS;
      return this;
    }

    public Builder setRoutingTimedOutRequestCounterResetDelayMS(long routingTimedOutRequestCounterResetDelayMS) {
      this.routingTimedOutRequestCounterResetDelayMS = routingTimedOutRequestCounterResetDelayMS;
      return this;
    }

    public Builder setRoutingPendingRequestCounterInstanceBlockThreshold(
        int routingPendingRequestCounterInstanceBlockThreshold) {
      this.routingPendingRequestCounterInstanceBlockThreshold = routingPendingRequestCounterInstanceBlockThreshold;
      return this;
    }

    public Builder setHeartBeatIntervalSeconds(long heartBeatIntervalSeconds) {
      this.heartBeatIntervalSeconds = heartBeatIntervalSeconds;
      return this;
    }

    public Builder setHeartBeatRequestTimeoutMS(long heartBeatRequestTimeoutMS) {
      this.heartBeatRequestTimeoutMS = heartBeatRequestTimeoutMS;
      return this;
    }

    public Builder setClient(Client client) {
      this.client = client;
      return this;
    }

    public Builder setLoadControllerWindowSizeInSec(int loadControllerWindowSizeInSec) {
      this.loadControllerWindowSizeInSec = loadControllerWindowSizeInSec;
      return this;
    }

    public Builder setLoadControllerRejectionRatioUpdateIntervalInSec(
        int loadControllerRejectionRatioUpdateIntervalInSec) {
      this.loadControllerRejectionRatioUpdateIntervalInSec = loadControllerRejectionRatioUpdateIntervalInSec;
      return this;
    }

    public Builder setLoadControllerMaxRejectionRatio(double loadControllerMaxRejectionRatio) {
      this.loadControllerMaxRejectionRatio = loadControllerMaxRejectionRatio;
      return this;
    }

    public Builder setLoadControllerAcceptMultiplier(double loadControllerAcceptMultiplier) {
      this.loadControllerAcceptMultiplier = loadControllerAcceptMultiplier;
      return this;
    }

    public Builder setLoadControllerEnabled(boolean loadControllerEnabled) {
      this.loadControllerEnabled = loadControllerEnabled;
      return this;
    }

    public InstanceHealthMonitorConfig build() {
      return new InstanceHealthMonitorConfig(this);
    }
  }
}
