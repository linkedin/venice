package com.linkedin.davinci.notifier;

public class VeniceChangelogBootstrapListener {
  private int bootstrapTimestampDeltaInSeconds = 0;

  public VeniceChangelogBootstrapListener() {
  }

  public void handleBootstrapCompleted() {
  }

  public void setBootstrapTimestampDeltaInSeconds(int seconds) {
    bootstrapTimestampDeltaInSeconds = seconds;
  }

  public long getBootstrapTimestampDeltaInSeconds() {
    return bootstrapTimestampDeltaInSeconds;
  }
}
