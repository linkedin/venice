package com.linkedin.venice.listener.response;

/**
 * This class is used to store common fields shared by various read responses.
 */
public abstract class ReadResponse {
  private long bdbQueryLatency;

  public void setBdbQueryLatency(long latency) {
    this.bdbQueryLatency = latency;
  }

  public long getBdbQueryLatency() {
    return this.bdbQueryLatency;
  }
}
