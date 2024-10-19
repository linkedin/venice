package com.linkedin.venice.heartbeat;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;


public class NoOpPushJobHeartbeatSenderFactory implements PushJobHeartbeatSenderFactory {
  @Override
  public PushJobHeartbeatSender createHeartbeatSender(
      String kafkaUrl,
      VeniceProperties properties,
      ControllerClient controllerClient,
      Optional<Properties> sslProperties) {
    return new NoOpPushJobHeartbeatSender();
  }
}
