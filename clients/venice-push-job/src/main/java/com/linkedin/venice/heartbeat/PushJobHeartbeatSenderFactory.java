package com.linkedin.venice.heartbeat;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;


/**
 * This interface provides methods to let users create a heartbeat provider
 */
public interface PushJobHeartbeatSenderFactory {
  /**
   * Create a heartbeat sender
   * @param properties
   */
  PushJobHeartbeatSender createHeartbeatSender(
      String kafkaUrl,
      VeniceProperties properties,
      ControllerClient controllerClient,
      Optional<Properties> sslProperties);
}
