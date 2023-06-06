package com.linkedin.venice.authentication;

import static com.linkedin.venice.ConfigKeys.AUTHENTICATION_SERVICE_CLASS;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class AuthenticationServiceUtils {
  private static final Logger LOGGER = LogManager.getLogger(AuthenticationServiceUtils.class);

  private AuthenticationServiceUtils() {
  }

  public static Optional<AuthenticationService> buildAuthenticationService(VeniceProperties veniceProperties) {
    String className = veniceProperties.getString(AUTHENTICATION_SERVICE_CLASS, "");
    if (className.isEmpty()) {
      return Optional.empty();
    }
    LOGGER.info("Building authentication service: {}", className);
    try {
      AuthenticationService authenticationService =
          Class.forName(className, false, AuthenticationService.class.getClassLoader())
              .asSubclass(AuthenticationService.class)
              .getConstructor()
              .newInstance();
      authenticationService.initialise(veniceProperties);
      return Optional.of(authenticationService);
    } catch (Exception ex) {
      LOGGER.error("Cannot load {}", className, ex);
      throw new IllegalStateException(ex);
    }
  }

}
