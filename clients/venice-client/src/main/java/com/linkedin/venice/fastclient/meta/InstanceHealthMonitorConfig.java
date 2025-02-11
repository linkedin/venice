package com.linkedin.venice.fastclient.meta;

import com.linkedin.r2.transport.common.Client;


public class InstanceHealthMonitorConfig {
  private final long routingRequestDefaultTimeoutMS;
  private final long routingTimedOutRequestCounterResetDelayMS; // 10s
  private final int routingPendingRequestCounterInstanceBlockThreshold;

  private final long heartBeatIntervalSeconds;
  private final long heartBeatRequestTimeoutMS;

  private final Client client;

  public InstanceHealthMonitorConfig(Builder builder) {
    this.routingRequestDefaultTimeoutMS = builder.routingRequestDefaultTimeoutMS;
    this.routingTimedOutRequestCounterResetDelayMS = builder.routingTimedOutRequestCounterResetDelayMS;
    this.routingPendingRequestCounterInstanceBlockThreshold =
        builder.routingPendingRequestCounterInstanceBlockThreshold;
    this.heartBeatIntervalSeconds = builder.heartBeatIntervalSeconds;
    this.heartBeatRequestTimeoutMS = builder.heartBeatRequestTimeoutMS;
    this.client = builder.client;
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

    public InstanceHealthMonitorConfig build() {
      return new InstanceHealthMonitorConfig(this);
    }
  }
}
