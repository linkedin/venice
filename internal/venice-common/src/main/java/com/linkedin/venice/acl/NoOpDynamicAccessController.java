package com.linkedin.venice.acl;

import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Set;


public class NoOpDynamicAccessController implements DynamicAccessController {
  public static final String USER_UNKNOWN = "USER_UNKNOWN";

  @Override
  public DynamicAccessController init(List<String> resources) {
    return this;
  }

  @Override
  public boolean hasAccess(X509Certificate clientCert, String resource, String method) throws AclException {
    return true;
  }

  @Override
  public boolean hasAccessToTopic(X509Certificate clientCert, String resource, String method) throws AclException {
    return true;
  }

  @Override
  public boolean hasAccessToAdminOperation(X509Certificate clientCert, String operation) throws AclException {
    return true;
  }

  @Override
  public boolean isAllowlistUsers(X509Certificate clientCert, String resource, String method) {
    return true;
  }

  @Override
  public String getPrincipalId(X509Certificate clientCert) {
    if (clientCert != null && clientCert.getSubjectX500Principal() != null) {
      return clientCert.getSubjectX500Principal().getName();
    }
    return USER_UNKNOWN;
  }

  @Override
  public boolean hasAcl(String resource) throws AclException {
    return true;
  }

  @Override
  public void addAcl(String resource) throws AclException {

  }

  @Override
  public void removeAcl(String resource) throws AclException {

  }

  @Override
  public Set<String> getAccessControlledResources() {
    return Collections.emptySet();
  }

  @Override
  public boolean isFailOpen() {
    return false;
  }
}
