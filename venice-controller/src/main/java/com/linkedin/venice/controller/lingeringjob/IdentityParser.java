package com.linkedin.venice.controller.lingeringjob;

import java.security.cert.X509Certificate;


/**
 * An interface that provides a method to parse identity information from a certificate. It's up to the implementation
 * to decide what exact information it is.
 */
public interface IdentityParser {

  String parseIdentityFromCert(X509Certificate certificate);
}
