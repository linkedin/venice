package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.exceptions.VeniceException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.apache.log4j.Logger;
import spark.Request;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;


public class AbstractRoute {
  private static final Logger logger = Logger.getLogger(AbstractRoute.class);
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
  protected boolean hasAccess(Request request) {
    if (!accessController.isPresent()) {
      /**
       * Grant access if it's not required to check ACL.
       * {@link accessController} will be empty if ACL is not enabled.
       */
      return true;
    }
    HttpServletRequest rawRequest = request.raw();
    Object certificateObject = rawRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    if (null == certificateObject) {
      throw new VeniceException("Client request doesn't contain certificate for store: " + request.queryParams(NAME));
    }
    X509Certificate certificate = ((X509Certificate[])certificateObject)[0];

    String storeName = request.queryParams(NAME);
    /**
     * Currently Nuage only supports adding GET/POST methods for a store resource
     * TODO: Feature request for Nuage to support other method like PUT or customized methods
     * like WRITE, UPDATE, ADMIN etc.
     */
    try {
      if (!accessController.get().hasAccess(certificate, storeName, "GET")) {
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
}
