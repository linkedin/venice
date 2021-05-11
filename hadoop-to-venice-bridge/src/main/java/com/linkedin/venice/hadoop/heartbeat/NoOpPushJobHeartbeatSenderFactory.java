package com.linkedin.venice.hadoop.heartbeat;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;

public class NoOpPushJobHeartbeatSenderFactory implements PushJobHeartbeatSenderFactory {
    @Override
    public PushJobHeartbeatSender createHeartbeatSender(
        VeniceProperties properties,
        ControllerClient controllerClient,
        Optional<SSLFactory> sslFactory
    ) {
        return new NoOpPushJobHeartbeatSender();
    }
}
