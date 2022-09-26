package com.linkedin.venice.controller.authorization;

import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Currently, acl creation is still managed by Nuage but Nuage is not aware about the system stores. The long term plan
 * is to let Nuage call Venice controller's acl APIs to perform user store acl operations. Venice controller's acl APIs
 * will handle corresponding system store acls properly. In the meantime this task periodically scans and synchronizes
 * any misaligned acls between the user store and their corresponding system stores (if any). The task is one per
 * controller and only in the parent controller based on the availability of the AuthorizerService.
 */
public class SystemStoreAclSynchronizationTask implements Runnable, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(SystemStoreAclSynchronizationTask.class);

  private final AuthorizerService authorizationService;
  private final VeniceParentHelixAdmin veniceParentHelixAdmin;
  private final long synchronizationCycleDelayInMs;
  private final AtomicBoolean isRunning = new AtomicBoolean();

  public SystemStoreAclSynchronizationTask(
      AuthorizerService authorizationService,
      VeniceParentHelixAdmin veniceParentHelixAdmin,
      long synchronizationCycleDelayInMs) {
    this.authorizationService = authorizationService;
    this.veniceParentHelixAdmin = veniceParentHelixAdmin;
    this.synchronizationCycleDelayInMs = synchronizationCycleDelayInMs;
  }

  @Override
  public void close() {
    isRunning.set(false);
  }

  /**
   * Since acl synchronization could be a long-running task we want to make sure we can handle stale states from local
   * variable, race conditions and early termination gracefully.
   */
  @Override
  public void run() {
    LOGGER.info("Running {}", SystemStoreAclSynchronizationTask.class.getSimpleName());
    isRunning.set(true);
    while (isRunning.get()) {
      try {
        Thread.sleep(synchronizationCycleDelayInMs);
        clusterLoop: for (String cluster: veniceParentHelixAdmin.getClustersLeaderOf()) {
          List<Store> storeList = veniceParentHelixAdmin.getAllStores(cluster);
          for (Store storeInList: storeList) {
            if (!isRunning.get()) {
              break clusterLoop;
            }
            try {
              if (!veniceParentHelixAdmin.isLeaderControllerFor(cluster)) {
                // Leadership might have changed.
                continue clusterLoop;
              }
              Store store = veniceParentHelixAdmin.getStore(cluster, storeInList.getName());
              if (store == null) {
                // The store is deleted or moved, continue.
                continue;
              }
              String storeName = store.getName();
              AclBinding storeAclBinding = null;
              for (VeniceSystemStoreType veniceSystemStoreType: VeniceSystemStoreType
                  .getEnabledSystemStoreTypes(store)) {
                if (storeAclBinding == null) {
                  storeAclBinding = authorizationService.describeAcls(new Resource(storeName));
                  if (storeAclBinding == null) {
                    throw new VeniceException("Retrieved null AclBinding for store: " + storeName);
                  }
                }
                synchronizeAclForSystemStore(cluster, veniceSystemStoreType, storeName, storeAclBinding);
              }
            } catch (Exception e) {
              LOGGER.error(
                  "Unexpected exception occurred while trying to synchronize acl for store: {}",
                  storeInList.getName(),
                  e);
            }
          }
        }
      } catch (InterruptedException e) {
        LOGGER.info("Task interrupted, closing");
        close();
      } catch (Exception e) {
        LOGGER.error("Unexpected exception encountered, isRunning = {}", isRunning, e);
      }
    }
    LOGGER.info("Stopped {}", SystemStoreAclSynchronizationTask.class.getSimpleName());
  }

  private void synchronizeAclForSystemStore(
      String clusterName,
      VeniceSystemStoreType veniceSystemStoreType,
      String storeName,
      AclBinding storeAclBinding) {
    AclBinding targetSystemStoreAclBinding = veniceSystemStoreType.generateSystemStoreAclBinding(storeAclBinding);
    Resource systemStoreResource = new Resource(veniceSystemStoreType.getSystemStoreName(storeName));
    AclBinding currentSystemStoreAclBinding = authorizationService.describeAcls(systemStoreResource);

    if (currentSystemStoreAclBinding == null || !targetSystemStoreAclBinding
        .equals(VeniceSystemStoreType.getSystemStoreAclFromTopicAcl(currentSystemStoreAclBinding))) {
      // TODO calling through the VeniceParentHelixAdmin for now in order to use the cluster level store repository lock
      // to prevent leaking acls when synchronizing system store acls for a store that's being deleted. This can be
      // optimized once global store level locks is implemented.
      veniceParentHelixAdmin.updateSystemStoreAclForStore(clusterName, storeName, targetSystemStoreAclBinding);
    }
  }
}
