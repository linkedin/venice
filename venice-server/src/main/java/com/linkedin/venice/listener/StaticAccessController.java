package com.linkedin.venice.listener;

import java.security.cert.X509Certificate;


/**
 * An AccessController allows a request to be checked against an Access Control List (ACL).
 * A static AccessController is a AccessController with an immutable ACL.
 */
public interface StaticAccessController {

  /**
   * Check if client has permission to access a resource.
   * This method is invoked by every single request, therefore
   * minimized execution time will result the best latency and throughput.
   *
   * @param clientCert the X509Certificate submitted by client
   * @param resource the resource being requested
   * @param method the operation (GET, POST, ...) to perform against the resource
   * @return  {@code true} if client has permission to access, otherwise {@code false}.
   */
  boolean hasAccess(X509Certificate clientCert, String resource, String method);
}
