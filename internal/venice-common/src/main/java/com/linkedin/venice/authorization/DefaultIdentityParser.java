package com.linkedin.venice.authorization;

import java.security.cert.X509Certificate;


public class DefaultIdentityParser implements IdentityParser {
  /**
   * Returns a string representation of the X.500 distinguished name using the format defined in RFC 2253.
   *
   * @param certificate The {@link X509Certificate} for the request
   * @return Returns a string representation of the X.500 distinguished name using the format defined in RFC 2253.
   */
  @Override
  public String parseIdentityFromCert(X509Certificate certificate) {
    return certificate.getSubjectX500Principal().getName();
  }
}
