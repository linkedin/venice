package com.linkedin.venice.controller.server;

import static com.linkedin.venice.HttpConstants.HTTP_GET;
import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.exceptions.VeniceException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;


public class AbstractRoute {
  private static final Logger LOGGER = LogManager.getLogger(AbstractRoute.class);

  private static final String USER_UNKNOWN = "USER_UNKNOWN";
  private static final String STORE_UNKNOWN = "STORE_UNKNOWN";

  // A singleton of acl check function against store resource
  private static final ResourceAclCheck GET_ACCESS_TO_STORE =
      (cert, resourceName, aclClient) -> aclClient.hasAccess(cert, resourceName, HTTP_GET);
  // A singleton of acl check function against topic resource
  private static final ResourceAclCheck WRITE_ACCESS_TO_TOPIC =
      (cert, resourceName, aclClient) -> aclClient.hasAccessToTopic(cert, resourceName, "Write");

  private static final ResourceAclCheck READ_ACCESS_TO_TOPIC =
      (cert, resourceName, aclClient) -> aclClient.hasAccessToTopic(cert, resourceName, "Read");

  private static final PermissionAssertion ASSERT_ACCESS_TO_STORE =
      (principal, resource, auth) -> auth.canAccess(Method.GET, resource, principal);
  // A singleton of acl check function against topic resource
  private static final PermissionAssertion ASSERT_WRITE_ACCESS_TO_TOPIC =
      (principal, resource, auth) -> auth.canAccess(Method.Write, resource, principal);

  private static final PermissionAssertion ASSERT_READ_ACCESS_TO_TOPIC =
      (principal, resource, auth) -> auth.canAccess(Method.Read, resource, principal);

  private final boolean sslEnabled;
  private final Optional<DynamicAccessController> accessController;
  private final Optional<AuthenticationService> authenticationService;
  private final Optional<AuthorizerService> authorizerService;

  /**
   * Default constructor for different controller request routes.
   *
   * TODO: once Venice Admin allowlist proposal is approved, we can transfer the allowlist to all routes
   * through this constructor; make sure Nuage is also in the allowlist so that they can create stores
   * @param accessController the access client that check whether a certificate can access a resource
   */
  public AbstractRoute(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    this.sslEnabled = sslEnabled;
    this.authenticationService = authenticationService;
    this.accessController = accessController;
    this.authorizerService = authorizerService;
  }

  /**
   * Check whether the user certificate in request has access to the store specified in
   * the request.
   */
  private boolean hasAccess(Request request, ResourceAclCheck aclCheckFunction, PermissionAssertion assertion) {
    Principal principal = getPrincipal(request);
    LOGGER.info("hasAccess {} {} {}", request, principal, authenticationService);

    String storeName = request.queryParams(NAME);

    if (authorizerService.isPresent()) {
      boolean allowed = assertion.apply(principal, new Resource(storeName), authorizerService.get());
      if (!allowed) {
        LOGGER.warn(
            "Client {} [host:{} IP:{}] doesn't have access to store {}",
            principal,
            request.host(),
            request.ip(),
            storeName);
        return false;
      }
    }

    if (!isAclEnabled()) {
      /**
       * Grant access if it's not required to check ACL.
       */
      return true;
    }

    X509Certificate certificate = getCertificate(request);
    /**
     * Currently Nuage only supports adding GET/POST methods for a store resource
     * TODO: Feature request for Nuage to support other method like PUT or customized methods
     * like WRITE, UPDATE, ADMIN etc.
     */
    try {
      if (!aclCheckFunction.apply(certificate, storeName, accessController.get())) {
        // log the abused users
        LOGGER.warn(
            "Client {} [host:{} IP:{}] doesn't have access to store {}",
            certificate.getSubjectX500Principal().toString(),
            request.host(),
            request.ip(),
            storeName);
        return false;
      }
    } catch (AclException e) {
      LOGGER.error(
          "Error while parsing certificate from client {} [host:{} IP:{}]",
          certificate.getSubjectX500Principal().toString(),
          request.host(),
          request.ip(),
          e);
      return false;
    }
    return true;
  }

  /**
   * Check whether the user has "Write" method access to the related version topics.
   */
  protected boolean hasWriteAccessToTopic(Request request) {
    return hasAccess(request, WRITE_ACCESS_TO_TOPIC, ASSERT_WRITE_ACCESS_TO_TOPIC);
  }

  /**
   * Check whether the user has "Read" method access to the related version topics.
   */
  protected boolean hasReadAccessToTopic(Request request) {
    return hasAccess(request, READ_ACCESS_TO_TOPIC, ASSERT_READ_ACCESS_TO_TOPIC);
  }

  protected Principal getPrincipal(Request request) {
    return authenticationService.map(service -> {
      AuthenticationService.HttpRequestAccessor requestAccessor = new HttpRequestAccessor(request);
      return service.getPrincipalFromHttpRequest(requestAccessor);
    }).orElse(null);
  }

  /**
   * Get principal Id from request.
   */
  protected String getPrincipalId(Request request) {

    if (authenticationService.isPresent()) {
      Principal principal = getPrincipal(request);
      if (principal != null) {
        return principal.getName();
      }
      // fallback to legacy implementation
    }

    if (!isSslEnabled()) {
      LOGGER.warn("SSL is not enabled. No certificate could be extracted from request.");
      return USER_UNKNOWN;
    }
    X509Certificate certificate = getCertificate(request);
    if (isAclEnabled()) {
      try {
        return accessController.get().getPrincipalId(certificate);
      } catch (Exception e) {
        LOGGER.error("Error when retrieving principal Id from request", e);
        return USER_UNKNOWN;
      }
    } else {
      return certificate.getSubjectX500Principal().getName();
    }
  }

  /**
   * Check whether the user has "GET" method access to the related store resource.
   *
   * Notice: currently we don't have any controller request that necessarily requires "GET" ACL to store;
   * ACL is not checked for requests that want to get metadata of a store/job.
   */
  protected boolean hasAccessToStore(Request request) {
    return hasAccess(request, GET_ACCESS_TO_STORE, ASSERT_ACCESS_TO_STORE);
  }

  /**
   * Check whether the user is within the admin users allowlist.
   */
  protected boolean isAllowListUser(Request request) {
    String storeName = request.queryParamOrDefault(NAME, STORE_UNKNOWN);
    if (!isAclEnabled()) {
      if (authenticationService.isPresent()) {
        Principal principal = getPrincipal(request);
        if (authorizerService.isPresent()) {
          return authorizerService.get().isSuperUser(principal, storeName);
        }
      }

      /**
       * Grant access if it's not required to check ACL.
       * {@link accessController} will be empty if ACL is not enabled.
       */
      return true;
    }
    X509Certificate certificate = getCertificate(request);
    return accessController.get().isAllowlistUsers(certificate, storeName, HTTP_GET);
  }

  /**
   * @return whether SSL is enabled
   */
  protected boolean isSslEnabled() {
    return sslEnabled;
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
  protected X509Certificate getCertificate(Request request) {
    HttpServletRequest rawRequest = request.raw();
    Object certificateObject = rawRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    if (certificateObject == null) {
      if (accessController.isPresent()) {
        throw new VeniceException("Client request doesn't contain certificate for store: " + request.queryParams(NAME));
      } else {
        // it is okay to not have certificate if legacy accessController is not configured
        return null;
      }
    }
    return ((X509Certificate[]) certificateObject)[0];
  }

  /**
   * A function that would check whether a principal has access to a resource.
   */
  @FunctionalInterface
  interface ResourceAclCheck {
    boolean apply(X509Certificate clientCert, String resource, DynamicAccessController accessController)
        throws AclException;
  }

  @FunctionalInterface
  interface PermissionAssertion {
    boolean apply(Principal principal, Resource resource, AuthorizerService authorizerService);
  }

  private class HttpRequestAccessor implements AuthenticationService.HttpRequestAccessor {
    private final Request request;

    public HttpRequestAccessor(Request request) {
      this.request = request;
    }

    @Override
    public String getHeader(String headerName) {
      return request.headers(headerName);
    }

    @Override
    public X509Certificate getCertificate() {
      return AbstractRoute.this.getCertificate(request);
    }

    @Override
    public String toString() {
      return request.toString();
    }
  }
}
