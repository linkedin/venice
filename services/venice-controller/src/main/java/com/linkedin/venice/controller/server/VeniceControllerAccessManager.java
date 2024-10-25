package com.linkedin.venice.controller.server;

import static java.util.Objects.requireNonNull;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.NoOpDynamicAccessController;
import com.linkedin.venice.authorization.Method;
import java.security.cert.X509Certificate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceControllerAccessManager {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerAccessManager.class);

  private static final String USER_UNKNOWN = "USER_UNKNOWN";
  private static final String STORE_UNKNOWN = "STORE_UNKNOWN";
  private final DynamicAccessController accessController;

  public VeniceControllerAccessManager(DynamicAccessController accessController) {
    this.accessController = requireNonNull(accessController, "DynamicAccessController is required to enforce ACL");
  }

  /**
   * Checks whether the user certificate in the request grants access of the specified {@link Method} type to the given resource.
   *
   * @param resourceName The name of the resource to access.
   * @param x509Certificate The user's X.509 certificate.
   * @param accessMethod The method of access (e.g., GET, POST).
   * @param requesterHostname The hostname of the requester (optional).
   * @param requesterIp The IP address of the requester (optional).
   * @return true if access is granted; false otherwise.
   */
  private boolean hasAccess(
      String resourceName,
      X509Certificate x509Certificate,
      Method accessMethod,
      String requesterHostname,
      String requesterIp) {
    try {
      if (accessController.hasAccess(x509Certificate, resourceName, accessMethod.name())) {
        return true;
      }
      // access denied; log the abusing users
      LOGGER.warn(
          "Client {} [host:{} IP:{}] doesn't have access to store {}",
          x509Certificate.getSubjectX500Principal().toString(),
          requesterHostname,
          requesterIp,
          resourceName);
    } catch (AclException e) {
      LOGGER.error(
          "Error while parsing certificate from client {} [host:{} IP:{}]",
          x509Certificate.getSubjectX500Principal().toString(),
          requesterHostname,
          requesterIp,
          e);
    }
    return false;
  }

  public boolean hasWriteAccessToPubSubTopic(
      String resourceName,
      X509Certificate x509Certificate,
      String requesterHostname,
      String requesterIp) {
    return hasAccess(resourceName, x509Certificate, Method.Write, requesterHostname, requesterIp);
  }

  public boolean hasReadAccessToPubSubTopic(
      String resourceName,
      X509Certificate x509Certificate,
      String requesterHostname,
      String requesterIp) {
    return hasAccess(resourceName, x509Certificate, Method.Read, requesterHostname, requesterIp);
  }

  public boolean hasAccessToStore(
      String resourceName,
      X509Certificate x509Certificate,
      String requesterHostname,
      String requesterIp) {
    return hasAccess(resourceName, x509Certificate, Method.GET, requesterHostname, requesterIp);
  }

  /**
   * Check whether the user is within the admin users allowlist.
   */
  public boolean isAllowListUser(String resourceName, X509Certificate x509Certificate) {
    if (resourceName == null) {
      resourceName = STORE_UNKNOWN;
    }
    return accessController.isAllowlistUsers(x509Certificate, resourceName, Method.GET.name());
  }

  public String getPrincipalId(X509Certificate x509Certificate) {
    try {
      if (x509Certificate != null) {
        return accessController.getPrincipalId(x509Certificate);
      }
      LOGGER.warn("Client certificate is null. Unable to extract principal Id. Returning USER_UNKNOWN");
    } catch (Exception e) {
      LOGGER.error("Error when retrieving principal Id from request", e);
    }
    return USER_UNKNOWN;
  }

  /**
   * @return whether ACL check is enabled.
   */
  protected boolean isAclEnabled() {
    /**
     * {@link accessController} will be of type {@link NoOpDynamicAccessController} if ACL is disabled.
     */
    return !(accessController instanceof NoOpDynamicAccessController);
  }
}
