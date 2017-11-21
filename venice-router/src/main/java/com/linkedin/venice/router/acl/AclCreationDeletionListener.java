package com.linkedin.venice.router.acl;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * Update access controller resource list when a store is created/deleted
 */
public class AclCreationDeletionListener implements StoreDataChangedListener {
  private static final Logger logger = Logger.getLogger(AclCreationDeletionListener.class);
  private final HelixReadOnlyStoreRepository metadataRepository;
  private final DynamicAccessController accessController;

  public AclCreationDeletionListener(DynamicAccessController accessController,
      HelixReadOnlyStoreRepository metadataRepository) {
    this.accessController = accessController;
    this.metadataRepository = metadataRepository;
  }

  @Override
  public void handleStoreCreated(Store store) {
    if (logger.isDebugEnabled()) {
      // Should not access to metadata repo again, might cause dead lock issue.
      logger.debug("Added \"" + store.getName() + "\" to store list.");
      logger.debug("Previous ACL list: " + accessController.getAccessControlledResources());
    }
    try {
      accessController.addAcl(store.getName());
    } catch (AclException e) {
      logger.error("Cannot add store to resource list: " + store.getName());
    }
    if (logger.isDebugEnabled()) {
      logger.debug("*EXPECTED* current ACL list: " + accessController.getAccessControlledResources() +
          " + " + store.getName()); // Actual ACL list cannot be determined yet
    }
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    if (logger.isDebugEnabled()) {
      // Should not access to metadata repo again, might cause dead lock issue.
      logger.debug("Removed \"" + storeName + "\" from store list.");
      logger.debug("Previous ACL list: " + accessController.getAccessControlledResources());
    }
    try {
      accessController.removeAcl(storeName);
    } catch (AclException e) {
      logger.error("Cannot remove store from resource list: " + storeName);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Current ACL list: " + accessController.getAccessControlledResources());
    }
  }

  @Override
  public void handleStoreChanged(Store store) {
  }
}
