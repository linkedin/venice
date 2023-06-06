package com.linkedin.venice.authentication;

import com.linkedin.venice.authentication.jwt.ClientAuthenticationProviderToken;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;


/**
 * Generic interface for client authentication providers.
 */
public interface ClientAuthenticationProvider extends AutoCloseable {
  default void initialize(VeniceProperties veniceProperties) throws Exception {
  }

  Map<String, String> getHTTPAuthenticationHeaders();

  @Override
  default void close() {
  }

  /**
   * No Authentication.
   */
  static ClientAuthenticationProvider DISABLED = new ClientAuthenticationProviderToken() {
    @Override
    public Map<String, String> getHTTPAuthenticationHeaders() {
      return Collections.emptyMap();
    }
  };
}
