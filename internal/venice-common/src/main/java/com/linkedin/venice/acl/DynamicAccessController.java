package com.linkedin.venice.acl;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;


/**
 * A DynamicAccessController is an AccessController with a mutable resource list.
 * The resource list may be changed at runtime.
 */
public interface DynamicAccessController extends AccessController {
  /**
   * Initialize access controller.
   *
   * @param resources the initial resource list
   * @return the access controller
   */
  DynamicAccessController init(List<String> resources);

  /**
   * Check if client has permission to access a particular resource.
   * This method is invoked by every single request, therefore
   * minimized execution time will result the best latency and throughput.
   *
   * @param clientCert the X509Certificate submitted by client
   * @param resource the resource being requested, such as a Venice store
   * @param method the operation (GET, POST, ...) to perform against the resource
   * @return {@code true} if client has permission to access, otherwise {@code false}.
   *          <p>When {@link #hasAcl(String)} == false, return {@link #isFailOpen()}.</p>
   */
  @Override
  boolean hasAccess(X509Certificate clientCert, String resource, String method) throws AclException;

  /**
   * Check if ACL exists for a particular resource.
   *
   * @param resource the resource name
   * @return  whether an ACL exists for the resource
   */
  boolean hasAcl(String resource) throws AclException;

  /**
   * Add a resource to the resource list.
   * Call this method when a new resource gets created.
   *
   * @param resource the resource name
   */
  void addAcl(String resource) throws AclException;

  /**
   * Remove a resource from the resource list.
   * Call this method when a existing resource gets deleted.
   *
   * @param resource the resource name
   */
  void removeAcl(String resource) throws AclException;

  /**
   * Get a list of currently being access-controlled resources.
   * Ideally, this list should be identical the list of existing resources,
   * so that each and every resource (e.g. Venice store) is being access-controlled.
   *
   * @return the set of access-controlled resources
   */
  Set<String> getAccessControlledResources();

  /**
   * Use this method to determine if clients will be granted access
   * to a resource when the corresponding ACL of the resource is missing.
   *
   * @return whether the implementation uses a fail-open policy
   */
  boolean isFailOpen();
}
