package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.TestMockTime;


public class PubSubBrokerConfigs {
  private String zkAddress;
  private TestMockTime mockTime;

  private PubSubBrokerConfigs(String zkAddress, TestMockTime mockTime) {
    this.zkAddress = zkAddress;
    this.mockTime = mockTime;
  }

  public String getZkAddress() {
    return zkAddress;
  }

  public TestMockTime getMockTime() {
    return mockTime;
  }

  public static class Builder {
    private String zkAddress;
    private TestMockTime mockTime;

    public Builder setZkAddress(String zkAddress) {
      this.zkAddress = zkAddress;
      return this;
    }

    public Builder setMockTime(TestMockTime mockTime) {
      this.mockTime = mockTime;
      return this;
    }

    public PubSubBrokerConfigs build() {
      return new PubSubBrokerConfigs(zkAddress, mockTime);
    }
  }
}
