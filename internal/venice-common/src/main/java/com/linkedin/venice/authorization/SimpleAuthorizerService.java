package com.linkedin.venice.authorization;

import java.security.cert.X509Certificate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A very simple AuthorizerService that requires that the user is authenticated.
 * In that case the user in allowed to do anything.
 */
public class SimpleAuthorizerService implements AuthorizerService {
  private static final Logger LOGGER = LogManager.getLogger(SimpleAuthorizerService.class);

  @Override
  public boolean canAccess(Method method, Resource resource, Principal principal) {
    boolean result = principal != null;
    LOGGER.info("canAccess {} {} {} -> {}", method, resource, principal, result);
    return result;
  }

  @Override
  public boolean canAccess(Method method, Resource resource, X509Certificate accessorCert) {
    boolean result = accessorCert != null;
    LOGGER.info("canAccess {} {} {} -> {}", method, resource, accessorCert, result);
    return result;
  }

  @Override
  public boolean isSuperUser(Principal principal, String storeName) {
    boolean result = principal != null;
    LOGGER.info("isSuperUser {} {} -> {}", principal, storeName, result);
    return result;
  }

  @Override
  public AclBinding describeAcls(Resource resource) {
    return new AclBinding(resource);
  }

  @Override
  public void setAcls(AclBinding aclBinding) {
  }

  @Override
  public void clearAcls(Resource resource) {
  }

  @Override
  public void addAce(Resource resource, AceEntry aceEntry) {
  }

  @Override
  public void removeAce(Resource resource, AceEntry aceEntry) {
  }
}
