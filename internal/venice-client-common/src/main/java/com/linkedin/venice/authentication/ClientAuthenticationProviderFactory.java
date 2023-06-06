package com.linkedin.venice.authentication;

import static com.linkedin.venice.CommonConfigKeys.AUTHENTICATION_TOKEN;

import com.linkedin.venice.authentication.jwt.ClientAuthenticationProviderToken;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;


public final class ClientAuthenticationProviderFactory {
  private ClientAuthenticationProviderFactory() {

  }

  public static ClientAuthenticationProvider build(VeniceProperties properties) {
    return build(properties.toProperties());
  }

  public static ClientAuthenticationProvider build(Map properties) {
    String token = properties.getOrDefault(AUTHENTICATION_TOKEN, "").toString();
    if (token.isEmpty()) {
      return ClientAuthenticationProvider.DISABLED;
    } else {
      return ClientAuthenticationProviderToken.TOKEN(token);
    }
  }
}
