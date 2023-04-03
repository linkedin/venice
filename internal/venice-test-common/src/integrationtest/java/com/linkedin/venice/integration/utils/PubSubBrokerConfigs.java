package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.TestMockTime;


public class PubSubBrokerConfigs {
  private final ZkServerWrapper zkServerWrapper;
  private final TestMockTime mockTime;

  private PubSubBrokerConfigs(Builder builder) {
    this.zkServerWrapper = builder.zkServerWrapper;
    this.mockTime = builder.mockTime;
  }

  public ZkServerWrapper getZkWrapper() {
    return zkServerWrapper;
  }

  public TestMockTime getMockTime() {
    return mockTime;
  }

  public static class Builder {
    private ZkServerWrapper zkServerWrapper;
    private TestMockTime mockTime;

    public Builder setZkWrapper(ZkServerWrapper zkServerWrapper) {
      this.zkServerWrapper = zkServerWrapper;
      return this;
    }

    public Builder setMockTime(TestMockTime mockTime) {
      this.mockTime = mockTime;
      return this;
    }

    public PubSubBrokerConfigs build() {
      return new PubSubBrokerConfigs(this);
    }
  }
}
