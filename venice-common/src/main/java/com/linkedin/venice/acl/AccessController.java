package com.linkedin.venice.acl;

import java.security.cert.X509Certificate;


/**
 * An AccessController allows a request to be checked against an Access Control List (ACL).
 */
public interface AccessController {
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
  boolean hasAccess(X509Certificate clientCert, String resource, String method) throws AclException;

  /**
   * Check if client has permission to access a particular topic resource.
   *
   * @param clientCert the X509Certificate submitted by client
   * @param resource the resource being requested
   * @param method the operation (Read, Write, ...) to perform against the topic resource
   * @return {@code true} if client has permission to access, otherwise {@code false}.
   */
  boolean hasAccessToTopic(X509Certificate clientCert, String resource, String method) throws AclException;

  /**
   * Check if client has permission to execute a particular admin operation.
   *
   * @param clientCert the X509Certificate submitted by client
   * @param operation the operation being performed
   * @return {@code true} if client has permission to access, otherwise {@code false}.
   */
  boolean hasAccessToAdminOperation(X509Certificate clientCert, String operation) throws AclException;

  /**
   * Check whether the client is the allowlist admin users.
   *
   * @param clientCert the X509Certificate submitted by client
   * @param resource the resource being requested;
   * @param method the operation (GET, POST, ...) to perform against the resource
   * @return true if the client is admin
   */
  boolean isAllowlistUsers(X509Certificate clientCert, String resource, String method);

  /**
   * Get principal Id from client certificate.
   * @param clientCert the X509Certificate submitted by client
   * @return principal Id. (headless account name, service name, LDAP id or group id)
   */
  String getPrincipalId(X509Certificate clientCert);
}
