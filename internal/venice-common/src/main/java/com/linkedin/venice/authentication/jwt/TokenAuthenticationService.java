package com.linkedin.venice.authentication.jwt;

import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Validates JWT tokens passed as Bearer tokens in the HTTP Authorise Request Header.
 */
public class TokenAuthenticationService implements AuthenticationService {
  private static final Logger log = LogManager.getLogger(TokenAuthenticationService.class);
  private static final String HTTP_HEADER_VALUE_PREFIX = "Bearer ";

  private AuthenticationProviderToken authenticationProvider;

  @Override
  public void initialise(VeniceProperties veniceProperties) throws Exception {
    TokenProperties tokenProperties = new TokenProperties(
        veniceProperties.getString(ConfigKeys.SECRET_KEY, ""),
        veniceProperties.getString(ConfigKeys.PUBLIC_KEY, ""),
        veniceProperties.getString(ConfigKeys.AUDIENCE_CLAIM, ""),
        veniceProperties.getString(ConfigKeys.AUTH_CLAIM, ""),
        veniceProperties.getString(ConfigKeys.AUDIENCE_CLAIM, ""),
        veniceProperties.getString(ConfigKeys.AUDIENCE, ""),
        veniceProperties.getString(ConfigKeys.JWKS_HOSTS_ALLOWLIST, ""));
    authenticationProvider = new AuthenticationProviderToken(tokenProperties);
  }

  @Override
  public void close() {

  }

  @Override
  public Principal getPrincipalFromHttpRequest(HttpRequestAccessor requestAccessor) {
    String httpHeaderValue = requestAccessor.getHeader("Authorization");
    String token;
    if (httpHeaderValue == null || httpHeaderValue.length() <= HTTP_HEADER_VALUE_PREFIX.length()) {
      log.info("No Authorization header found in request: {}", requestAccessor);
      return null;
    } else {
      token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
    }

    if (log.isDebugEnabled()) {
      log.debug("Authenticating user with token: {}", token);
    }
    try {
      String role = authenticationProvider.authenticate(token);
      if (log.isDebugEnabled()) {
        log.debug("Authenticated user: {} with role: {}", token, role);
      }
      return new Principal(role);
    } catch (AuthenticationProviderToken.AuthenticationException error) {
      log.error("Cannot authenticatate user with token: {}", token, error);
      return null;
    }
  }
}
