package com.linkedin.venice.heartbeat;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;


public class NoOpPushJobHeartbeatSender implements PushJobHeartbeatSender {
  @Override
  public Duration getHeartbeatSendInterval() {
    return Duration.ZERO; // Placeholder
  }

  @Override
  public Duration getHeartbeatInitialDelay() {
    return Duration.ZERO; // Placeholder
  }

  @Override
  public void start(@Nonnull String storeName, int storeVersion) {
    // No op
  }

  @Override
  public void stop() {
    // No op
  }

  @Override
  public void run() {
    // No op
  }

  @Override
  public Optional<Exception> getFirstSendHeartbeatException() {
    return Optional.empty();
  }
}
