package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.security.datavault.common.principal.PrincipalBuilder;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import org.apache.log4j.Logger;


public class IdentityParserImpl implements IdentityParser {
  private static final Logger logger = Logger.getLogger(IdentityParserImpl.class);

  /**
   * Firstly, it tries to parse the principal from the given certificate. If it works, use the principal's toString as
   * the return identity information. Otherwise, use the certificate's toString as the return identity information.
   *
   * @param certificate
   * @return
   */
  @Override
  public String parseIdentityFromCert(X509Certificate certificate) {
    try {
      return PrincipalBuilder.builderForCertificate(certificate).build().toString();
    } catch (CertificateParsingException e) {
      logger.error("Failed to parse principal from cert. "
          + "Ignore this error. Use certificate.toString as identity", e);
      return certificate.toString();
    }
  }
}
