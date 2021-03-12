package com.linkedin.venice.hadoop.heartbeat;

import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;

/**
 * This interface provides methods to let users create a heartbeat provider
 */
public interface PushJobHeartbeatSenderFactory {

    /**
     * Create a heartbeat sender
     * @param properties
     */
    PushJobHeartbeatSender createHeartbeatSender(VeniceProperties properties, Optional<SSLFactory> sslFactory);
}
