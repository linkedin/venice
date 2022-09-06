package com.linkedin.venice.controller;

import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import java.security.cert.X509Certificate;


/**
 * A mock {@link AuthorizerService} implementation to help in unit test.
 */
public class MockVeniceAuthorizer implements AuthorizerService {
  private AclBinding aclBinding = null;
  public int setAclsCounter = 0;
  public int clearAclCounter = 0;

  public boolean canAccess(Method method, Resource resource, Principal principal) {
    return true;
  }

  public boolean canAccess(Method method, Resource resource, X509Certificate accessorCert) {
    return true;
  }

  public AclBinding describeAcls(Resource resource) {
    return aclBinding != null ? aclBinding : new AclBinding(resource);
  }

  public void setAcls(AclBinding aclBinding) {
    this.aclBinding = aclBinding;
    this.setAclsCounter++;
  }

  public void clearAcls(Resource resource) {
    this.aclBinding = null;
    this.clearAclCounter++;
  }

  public void addAce(Resource resource, AceEntry aceEntry) {
    if (this.aclBinding == null) {
      this.aclBinding = new AclBinding(resource);
    }
    this.aclBinding.addAceEntry(aceEntry);
  }

  public void removeAce(Resource resource, AceEntry aceEntry) {
  }
}
