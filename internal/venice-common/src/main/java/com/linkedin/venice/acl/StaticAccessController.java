package com.linkedin.venice.acl;

import java.security.cert.X509Certificate;


/**
 * A StaticAccessController is an AccessController with an immutable resource list.
 */
public interface StaticAccessController extends AccessController {
  /**
   * Check if client has permission to access a particular resource.
   * This method is invoked by every single request, therefore
   * minimized execution time will result the best latency and throughput.
   *
   * @param clientCert the X509Certificate submitted by client
   * @param resource the resource being requested
   * @param method the operation (GET, POST, ...) to perform against the resource
   * @return {@code true} if client has permission to access, otherwise {@code false}.
   */
  @Override
  boolean hasAccess(X509Certificate clientCert, String resource, String method);
}
