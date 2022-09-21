package com.linkedin.venice.acl;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Update access controller resource list when a store is created/deleted
 */
public class AclCreationDeletionListener implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(AclCreationDeletionListener.class);
  private final DynamicAccessController accessController;

  public AclCreationDeletionListener(DynamicAccessController accessController) {
    this.accessController = accessController;
  }

  @Override
  public void handleStoreCreated(Store store) {
    // Should not access to metadata repo again, might cause dead lock issue.
    LOGGER.debug("Added store: {} to store list.", store.getName());
    LOGGER.debug("Previous ACL list: {}", accessController.getAccessControlledResources());
    try {
      accessController.addAcl(store.getName());
    } catch (AclException e) {
      LOGGER.error("Cannot add store to resource list: {}", store.getName());
    }
    LOGGER.debug(
        "*EXPECTED* current ACL list: {} + store: {}",
        accessController.getAccessControlledResources(),
        store.getName());
    // Actual ACL list cannot be determined yet.
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    // Should not access to metadata repo again, might cause dead lock issue.
    LOGGER.debug("Removed store: {} from store list.", storeName);
    LOGGER.debug("Previous ACL list: {}", accessController.getAccessControlledResources());
    try {
      accessController.removeAcl(storeName);
    } catch (AclException e) {
      LOGGER.error("Cannot remove store from resource list: {}", storeName);
    }
    LOGGER.debug("Current ACL list: {}", accessController.getAccessControlledResources());
  }

  @Override
  public void handleStoreChanged(Store store) {
  }
}
