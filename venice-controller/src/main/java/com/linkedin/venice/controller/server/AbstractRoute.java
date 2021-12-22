package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.exceptions.VeniceException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.apache.log4j.Logger;
import spark.Request;

import static com.linkedin.venice.HttpConstants.*;
import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;


public class AbstractRoute {
  private static final Logger logger = Logger.getLogger(AbstractRoute.class);

  private static final String USER_UNKNOWN = "USER_UNKNOWN";
  private static final String STORE_UNKNOWN = "STORE_UNKNOWN";

  // A singleton of acl check function against store resource
  private static final ResourceAclCheck getAccessToStore = (cert, resourceName, aclClient) -> {
    return aclClient.hasAccess(cert, resourceName, HTTP_GET);
  };
  // A singleton of acl check function against topic resource
  private static final ResourceAclCheck writeAccessToTopic = (cert, resourceName, aclClient) -> {
    return aclClient.hasAccessToTopic(cert, resourceName, "Write");
  };

  private static final ResourceAclCheck readAccessToTopic = (cert, resourceName, aclClient) -> {
    return aclClient.hasAccessToTopic(cert, resourceName, "Read");
  };

  private final Optional<DynamicAccessController> accessController;

  /**
   * Default constructor for different controller request routes.
   *
   * TODO: once Venice Admin white list proposal is approved, we can transfer the whitelist to all routes
   * through this constructor; make sure Nuage is also in the whitelist so that they can create stores
   * @param accessController the access client that check whether a certificate can access a resource
   */
  public AbstractRoute(Optional<DynamicAccessController> accessController) {
    this.accessController = accessController;
  }

  /**
   * Check whether the user certificate in request has access to the store specified in
   * the request.
   */
  private boolean hasAccess(Request request, ResourceAclCheck aclCheckFunction) {
    if (!isAclEnabled()) {
      /**
       * Grant access if it's not required to check ACL.
       */
      return true;
    }
    X509Certificate certificate = getCertificate(request);

    String storeName = request.queryParams(NAME);
    /**
     * Currently Nuage only supports adding GET/POST methods for a store resource
     * TODO: Feature request for Nuage to support other method like PUT or customized methods
     * like WRITE, UPDATE, ADMIN etc.
     */
    try {
      if (!aclCheckFunction.apply(certificate, storeName, accessController.get())) {
        // log the abused users
        logger.warn(String.format("Client %s [host:%s IP:%s] doesn't have access to store %s",
            certificate.getSubjectX500Principal().toString(), request.host(), request.ip(), storeName));
        return false;
      }
    } catch (AclException e) {
      logger.error(String.format("Error while parsing certificate from client %s [host:%s IP:%s]",
          certificate.getSubjectX500Principal().toString(), request.host(), request.ip()), e);
      return false;
    }
    return true;
  }

  /**
   * Check whether the user has "Write" method access to the related version topics.
   */
  protected boolean hasWriteAccessToTopic(Request request) {
    return hasAccess(request, writeAccessToTopic);
  }

  /**
   * Check whether the user has "Read" method access to the related version topics.
   */
  protected boolean hasReadAccessToTopic(Request request) {
    return hasAccess(request, readAccessToTopic);
  }

  /**
   * Get principal Id from request.
   */
  protected String getPrincipalId(Request request) {
    if (!isAclEnabled()) {
      logger.warn("ACL is not enabled. No certificate could be extracted from request.");
      return USER_UNKNOWN;
    }
    try {
      X509Certificate certificate = getCertificate(request);
      return accessController.get().getPrincipalId(certificate);
    } catch (Exception e) {
      logger.error("Error when retrieving principal Id from request", e);
      return USER_UNKNOWN;
    }
  }

  /**
   * Check whether the user has "GET" method access to the related store resource.
   *
   * Notice: currently we don't have any controller request that necessarily requires "GET" ACL to store;
   * ACL is not checked for requests that want to get metadata of a store/job.
   */
  protected boolean hasAccessToStore(Request request) {
    return hasAccess(request, getAccessToStore);
  }

  /**
   * Check whether the user is within the admin users whitelist.
   */
  protected boolean isWhitelistUsers(Request request) {
    if (!isAclEnabled()) {
      /**
       * Grant access if it's not required to check ACL.
       * {@link accessController} will be empty if ACL is not enabled.
       */
      return true;
    }
    X509Certificate certificate = getCertificate(request);

    String storeName = request.queryParamOrDefault(NAME, STORE_UNKNOWN);
    return accessController.get().isWhitelistUsers(certificate, storeName, HTTP_GET);
  }

  /**
   * @return whether ACL check is enabled.
   */
  protected boolean isAclEnabled() {
    /**
     * {@link accessController} will be empty if ACL is not enabled.
     */
    return accessController.isPresent();
  }

  /**
   * Helper function to get certificate out of Spark request
   */
  protected static X509Certificate getCertificate(Request request) {
    HttpServletRequest rawRequest = request.raw();
    Object certificateObject = rawRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    if (null == certificateObject) {
      throw new VeniceException("Client request doesn't contain certificate for store: " + request.queryParams(NAME));
    }
    return ((X509Certificate[])certificateObject)[0];
  }

  /**
   * A function that would check whether a principal has access to a resource.
   */
  @FunctionalInterface
  interface ResourceAclCheck {
    boolean apply(X509Certificate clientCert, String resource, DynamicAccessController accessController) throws AclException;
  }
}
