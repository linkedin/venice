package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.SystemStoreAttributes;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;


public class HelixStoreGraveyard implements StoreGraveyard {
  private static final Logger LOGGER = LogManager.getLogger(HelixStoreGraveyard.class);

  public static final String STORE_GRAVEYARD_PATH = "/StoreGraveyard";

  protected ZkBaseDataAccessor<Store> dataAccessor;
  // TODO we could put the Store gravyard znode to upper level to make it non-cluster-specific.
  private final Set<String> clusterNames;

  public HelixStoreGraveyard(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      Collection<String> clusterNames) {
    this(zkClient, adapterSerializer, clusterNames, new StoreJSONSerializer());
  }

  public HelixStoreGraveyard(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      Collection<String> clusterNames,
      VeniceSerializer<Store> storeSerializer) {
    this.clusterNames = new HashSet<>(clusterNames);
    adapterSerializer.registerSerializer(getGeneralStoreGraveyardPath(), storeSerializer);
    zkClient.setZkSerializer(adapterSerializer);
    dataAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  @Override
  public int getLargestUsedVersionNumber(String storeName) {
    if (VeniceSystemStoreUtils.isSystemStore(storeName)) {
      VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
      if (systemStoreType != null && systemStoreType.isStoreZkShared()) {
        String userStoreName = systemStoreType.extractRegularStoreName(storeName);
        return getPerUserStoreSystemStoreLargestUsedVersionNumber(userStoreName, systemStoreType);
      }
    }

    List<Store> stores = getStoreFromAllClusters(storeName);
    if (stores.isEmpty()) {
      LOGGER.info(
          "Store: {} does NOT exist in the store graveyard. Will initialize the new store at version: {}.",
          storeName,
          Store.NON_EXISTING_VERSION);
      // If store does NOT existing in graveyard, it means store has never been deleted, return 0 which is the default
      // value of largestUsedVersionNumber for a new store.
      return Store.NON_EXISTING_VERSION;
    }
    int largestUsedVersionNumber = Store.NON_EXISTING_VERSION;
    for (Store deletedStore: stores) {
      if (deletedStore.getLargestUsedVersionNumber() > largestUsedVersionNumber) {
        largestUsedVersionNumber = deletedStore.getLargestUsedVersionNumber();
      }
    }

    LOGGER.info(
        "Found store: {} in the store graveyard. Will initialize the new store at version: {}.",
        storeName,
        largestUsedVersionNumber);
    return largestUsedVersionNumber;
  }

  @Override
  public void putStoreIntoGraveyard(String clusterName, Store store) {
    int largestUsedVersionNumber = getLargestUsedVersionNumber(store.getName());

    if (store.isMigrating()) {
      /**
       * Suppose I have two datacenters Parent and Child, each has two clusters C1 and C2
       * Before migration, I have a store with largest version 3:
       * P: C1:v3*, C2:null
       * C: C1:v3*, C2:null
       *
       * After migration, both clusters shoud have the same store with same largest version and cluster discovery points to C2
       * P: C1:v3, C2:v3*
       * C: C1:v3, C2:v3*
       *
       * Then before I send --end-migration command, another push job started
       * P: C1:v3, C2:v4*
       * C: C1:v4, C2:v4*
       *
       * Suppose I accidentally delete the store in the wrong cluster C2 using the wrong command --delete-store
       * P: C1:v3, C2:null*
       * C: C1:v4, C2:null*
       *
       * Then I realized the error and want to delete the other store as well, but now I can't delete it because the largest
       * version number (3) doesn't match with the one retrived from graveyard (4).
       * This check will address to this situation, and keep the largest version number in both graveyards the same.
       */
      if (largestUsedVersionNumber > store.getLargestUsedVersionNumber()) {
        LOGGER.info(
            "Increased largestUsedVersionNumber for migrating store {} from {} to {}.",
            store.getName(),
            store.getLargestUsedVersionNumber(),
            largestUsedVersionNumber);
        store.setLargestUsedVersionNumber(largestUsedVersionNumber);
      }
    } else if (store.getLargestUsedVersionNumber() < largestUsedVersionNumber) {
      // largestUsedVersion number in re-created store is smaller than the deleted store. It's should be a issue.
      String errorMsg = "Invalid largestUsedVersionNumber: " + store.getLargestUsedVersionNumber() + " in Store: "
          + store.getName() + ", it's smaller than one found in graveyard: " + largestUsedVersionNumber;
      LOGGER.error(errorMsg);
      throw new VeniceException(errorMsg);
    }

    // Store does not exist in graveyard OR store already exists but the re-created store is deleted again so we need to
    // update the ZNode.
    HelixUtils.update(dataAccessor, getStoreGraveyardPath(clusterName, store.getName()), store);
    LOGGER.info(
        "Put store: {} into graveyard with largestUsedVersionNumber {}.",
        store.getName(),
        largestUsedVersionNumber);
  }

  @Override
  public Store getStoreFromGraveyard(String clusterName, String storeName, Stat stat) {
    String path = getStoreGraveyardPath(clusterName, storeName);
    return dataAccessor.get(path, stat, AccessOption.PERSISTENT);
  }

  @Override
  public void removeStoreFromGraveyard(String clusterName, String storeName) {
    String path = getStoreGraveyardPath(clusterName, storeName);
    Store store = dataAccessor.get(path, null, AccessOption.PERSISTENT);
    if (store != null) {
      HelixUtils.remove(dataAccessor, path);
      LOGGER.info("Removed store: {} from graveyard in cluster: {}.", storeName, clusterName);
    }
  }

  @Override
  public List<String> listStoreNamesFromGraveyard(String clusterName) {
    return HelixUtils.listPathContents(dataAccessor, getStoreGraveyardParentPath(clusterName));
  }

  /**
   * Search for matching store in graveyard in all clusters
   * @param storeName Store of interest
   * @return  Matching store from each venice. Normally contains one element.
   * If the store existed in some other cluster before, there will be more than one element in the return value.
   */
  private List<Store> getStoreFromAllClusters(String storeName) {
    List<Store> stores = new ArrayList<>();
    for (String clusterName: clusterNames) {
      Store store = dataAccessor.get(getStoreGraveyardPath(clusterName, storeName), null, AccessOption.PERSISTENT);
      if (store != null) {
        stores.add(store);
      }
    }
    return stores;
  }

  private int getPerUserStoreSystemStoreLargestUsedVersionNumber(
      String userStoreName,
      VeniceSystemStoreType systemStoreType) {
    String systemStoreName = systemStoreType.getSystemStoreName(userStoreName);
    List<Store> deletedStores = getStoreFromAllClusters(userStoreName);
    if (deletedStores.isEmpty()) {
      LOGGER.info(
          "User store: {} does NOT exist in the store graveyard. Hence, no largest used version for its system store: {}",
          userStoreName,
          systemStoreName);
      return Store.NON_EXISTING_VERSION;
    }
    int largestUsedVersionNumber = Store.NON_EXISTING_VERSION;
    for (Store deletedStore: deletedStores) {
      Map<String, SystemStoreAttributes> systemStoreNamesToAttributes = deletedStore.getSystemStores();
      SystemStoreAttributes systemStoreAttributes =
          systemStoreNamesToAttributes.get(VeniceSystemStoreType.getSystemStoreType(systemStoreName).getPrefix());
      if (systemStoreAttributes != null) {
        largestUsedVersionNumber =
            Math.max(largestUsedVersionNumber, systemStoreAttributes.getLargestUsedVersionNumber());
      }
    }

    if (largestUsedVersionNumber == Store.NON_EXISTING_VERSION) {
      LOGGER.info("Can not find largest used version number for {}.", systemStoreName);
    }
    return largestUsedVersionNumber;
  }

  private String getGeneralStoreGraveyardPath() {
    return getStoreGraveyardPath(PathResourceRegistry.WILDCARD_MATCH_ANY, PathResourceRegistry.WILDCARD_MATCH_ANY);
  }

  private String getStoreGraveyardPath(String clusterName, String storeName) {
    return getStoreGraveyardParentPath(clusterName) + "/" + storeName;
  }

  private String getStoreGraveyardParentPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + STORE_GRAVEYARD_PATH;
  }
}
