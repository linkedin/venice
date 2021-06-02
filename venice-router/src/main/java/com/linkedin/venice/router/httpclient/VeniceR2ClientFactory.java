package com.linkedin.venice.router.httpclient;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import java.util.Optional;


public interface VeniceR2ClientFactory {
  Client buildR2Client(Optional<SSLEngineComponentFactory> sslEngineComponentFactory);
}
