package com.linkedin.venice.authentication;

import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;
import java.security.cert.X509Certificate;


/**
 * Performs authentication.
 */
public interface AuthenticationService extends Closeable {
  default void initialise(VeniceProperties veniceProperties) throws Exception {
  }

  @Override
  default void close() {
  }

  default Principal getPrincipalFromHttpRequest(HttpRequestAccessor requestAccessor) {
    return null;
  }

  interface HttpRequestAccessor {
    String getHeader(String headerName);

    X509Certificate getCertificate();
  }

}
