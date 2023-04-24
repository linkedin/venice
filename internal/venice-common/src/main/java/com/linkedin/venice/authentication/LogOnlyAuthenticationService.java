package com.linkedin.venice.authentication;

import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class LogOnlyAuthenticationService implements AuthenticationService {
  private static final Logger LOGGER = LogManager.getLogger(LogOnlyAuthenticationService.class);

  @Override
  public void initialise(VeniceProperties veniceProperties) throws Exception {
    LOGGER.info("initialise {}", veniceProperties);
  }

  @Override
  public void close() {
    LOGGER.info("close");
  }

  @Override
  public Principal getPrincipalFromHttpRequest(HttpRequestAccessor requestAccessor) {
    LOGGER.info("getPrincipalFromHttpRequest {}", requestAccessor);
    return null;
  }

}
