package com.linkedin.venice.authorization;

import static com.linkedin.venice.ConfigKeys.AUTHORIZER_SERVICE_CLASS;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class AuthorizerServiceUtils {
  private static final Logger LOGGER = LogManager.getLogger(AuthorizerServiceUtils.class);

  private AuthorizerServiceUtils() {
  }

  public static Optional<AuthorizerService> buildAuthorizerService(VeniceProperties veniceProperties) {
    String className = veniceProperties.getString(AUTHORIZER_SERVICE_CLASS, "");
    if (className.isEmpty()) {
      return Optional.empty();
    }
    LOGGER.info("Building authorizer service: {}", className);
    try {
      AuthorizerService authorizerService = Class.forName(className, false, AuthorizerService.class.getClassLoader())
          .asSubclass(AuthorizerService.class)
          .getConstructor()
          .newInstance();
      authorizerService.initialise(veniceProperties);
      return Optional.of(authorizerService);
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

}
