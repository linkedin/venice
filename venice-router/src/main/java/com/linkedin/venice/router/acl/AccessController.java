package com.linkedin.venice.router.acl;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;


/**
 * An AccessController allows a request to be checked against an Access Control List (ACL).
 */
public interface AccessController {

  /**
   * Initialize access controller.
   *
   * @param stores the existing stores
   * @return the access controller
   */
  AccessController init(List<String> stores);

  /**
   * Check if client has has permission to access a particular store.
   * This method is invoked by every single request, therefore
   * minimized execution time will result the best latency and throughput.
   *
   * @param clientCert the X509Certificate submitted by client
   * @param storeName the Venice store name
   * @param method the operation (GET, POST, ...) to perform against the store
   * @return  {@code true} if client has permission to access, otherwise {@code false}.
   *          When {@link #hasAcl(String)} == false, return {@link #isFailOpen()}.
   */
  boolean hasAccess(X509Certificate clientCert, String storeName, String method) throws AclException;

  /**
   * Check if ACL exists for a particular store.
   *
   * @param storeName the Venice store name
   * @return  whether or not ACL exists for the store
   */
  boolean hasAcl(String storeName) throws AclException;

  /**
   * Add a store to the access control list.
   * Call this method when a new store gets created.
   *
   * @param storeName the store name
   */
  void addAcl(String storeName) throws AclException;

  /**
   * Remove a store from the access control list.
   * Call this method when a existing store gets deleted.
   *
   * @param storeName the store name
   */
  void removeAcl(String storeName) throws AclException;

  /**
   * Get a list of access-controlled resources.
   * Ideally, this list should be identical the list of existing resources,
   * so that each and every resource (Venice store) is being access-controlled.
   *
   * @return the set of access-controlled resources
   */
  Set<String> getAccessControlledStores();

  /**
   * Use this method to determine if clients will be granted access
   * to a resource when the corresponding ACL of the resource is missing.
   *
   * @return whether or not the implementation uses a fail-open policy
   */
  boolean isFailOpen();
}
