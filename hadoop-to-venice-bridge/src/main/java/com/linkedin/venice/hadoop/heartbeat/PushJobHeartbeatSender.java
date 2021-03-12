package com.linkedin.venice.hadoop.heartbeat;

import com.linkedin.venice.writer.VeniceWriter;
import java.time.Duration;
import javax.annotation.Nonnull;


/**
 * This interface provides methods to send push job heartbeats
 */
public interface PushJobHeartbeatSender extends Runnable {

  Duration getHeartbeatSendInterval();

  Duration getHeartbeatInitialDelay();

  void start(
      @Nonnull String storeName,
      int storeVersion
  );

  void stop();
}
