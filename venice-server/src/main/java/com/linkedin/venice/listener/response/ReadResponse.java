package com.linkedin.venice.listener.response;

/**
 * This class is used to store common fields shared by various read responses.
 */
public abstract class ReadResponse {
  private double bdbQueryLatency;

  public void setBdbQueryLatency(double latency) {
    this.bdbQueryLatency = latency;
  }

  public double getBdbQueryLatency() {
    return this.bdbQueryLatency;
  }
}
