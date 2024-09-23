package com.linkedin.venice.heartbeat;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * This interface provides methods to send push job heartbeats
 */
public interface PushJobHeartbeatSender extends Runnable {
  Duration getHeartbeatSendInterval();

  Duration getHeartbeatInitialDelay();

  void start(@Nonnull String storeName, int storeVersion);

  void stop();

  /**
   * Get first exception encountered while it is sending heartbeats. If there are multiple exceptions encountered during
   * the process of sending heartbeats, only the first one is recorded and returned here.
   *
   * @return Empty optional if no exception
   */
  Optional<Exception> getFirstSendHeartbeatException();
}
