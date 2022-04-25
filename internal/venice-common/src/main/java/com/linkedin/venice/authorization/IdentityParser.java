package com.linkedin.venice.authorization;

import java.security.cert.X509Certificate;


/**
 * An interface that provides a method to parse identity information from a certificate. It's up to the implementation
 * to decide what exact information it is.
 */
public interface IdentityParser {
  /**
   * Returns a string representation of the identity represented by the {@link X509Certificate}.
   *
   * @param certificate The {@link X509Certificate} for the request
   * @return Returns a string representation of the X.500 distinguished name using the format defined in RFC 2253.
   */
  String parseIdentityFromCert(X509Certificate certificate);
}
