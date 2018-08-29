package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;


public class HelixStoreGraveyard implements StoreGraveyard {
  private static final Logger logger = Logger.getLogger(StoreGraveyard.class);

  public static final String STORE_GRAVEYARD_PATH = "/StoreGraveyard";

  protected ZkBaseDataAccessor<Store> dataAccessor;
  // TODO we could put the Store gravyard znode to upper level to make it non-cluster-specific.
  private final Set<String> clusterNames;

  public HelixStoreGraveyard(ZkClient zkClient, HelixAdapterSerializer adapterSerializer,
      Collection<String> clusterNames) {
    this(zkClient, adapterSerializer, clusterNames, new StoreJSONSerializer());
  }

  public HelixStoreGraveyard(ZkClient zkClient, HelixAdapterSerializer adapterSerializer,
      Collection<String> clusterNames, VeniceSerializer<Store> storeSerializer) {
    this.clusterNames = new HashSet<>(clusterNames);
    adapterSerializer.registerSerializer(getGeneralDeletedStorePath(PathResourceRegistry.WILDCARD_MATCH_ANY),
        storeSerializer);
    zkClient.setZkSerializer(adapterSerializer);
    dataAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  @Override
  public int getLargestUsedVersionNumber(String storeName) {
    List<Store> stores = getStore(storeName);
    if (stores.isEmpty()) {
      logger.info(
          "Store: " + storeName + " does NOT exist in the store graveyard. Will initialize the new store at version: "
              + Store.NON_EXISTING_VERSION);
      // If store does NOT existing in graveyard, it means store has never been deleted, return 0 which is the default
      // value of largestUsedVersionNumber for a new store.
      return Store.NON_EXISTING_VERSION;
    }
    int largestUsedVersionNumber = Store.NON_EXISTING_VERSION;
    for (Store deletedStore : stores) {
      if (deletedStore.getLargestUsedVersionNumber() > largestUsedVersionNumber) {
        largestUsedVersionNumber = deletedStore.getLargestUsedVersionNumber();
      }
    }

    logger.info("Found store: " + storeName + " in the store graveyard. Will initialize the new store at version: "
        + largestUsedVersionNumber);
    return largestUsedVersionNumber;
  }

  @Override
  public void putStoreIntoGraveyard(String clusterName, Store store) {
    int largestUsedVersionNumber = getLargestUsedVersionNumber(store.getName());
    if (store.getLargestUsedVersionNumber() < largestUsedVersionNumber) {
      // largestUsedVersion number in re-created store is smaller than the deleted store. It's should be a issue.
      String errorMsg = "Invalid largestUsedVersionNumber: " + store.getLargestUsedVersionNumber() +
          " in Store: " + store.getName() + ", it's smaller than one found in graveyard: " + largestUsedVersionNumber;
      logger.error(errorMsg);
      throw new VeniceException(errorMsg);
    }

    // Store does not exist in graveyard OR store already exists but the re-created store is deleted again so we need to
    // update the ZNode.
    HelixUtils.update(dataAccessor, getClusterDeletedStorePath(clusterName, store.getName()), store);
    logger.info("Put store: " + store.getName() + " into graveyard.");
  }

  private List<Store> getStore(String storeName) {
    List<Store> stores = new ArrayList<>();
    for (String clusterName : clusterNames) {
      Store store = dataAccessor.get(getClusterDeletedStorePath(clusterName, storeName), null, AccessOption.PERSISTENT);
      if (store != null) {
        stores.add(store);
      }
    }
    return stores;
  }

  private String getGeneralDeletedStorePath(String storeName) {
    return HelixUtils.getHelixClusterZkPath(PathResourceRegistry.WILDCARD_MATCH_ANY) + STORE_GRAVEYARD_PATH + "/"
        + storeName;
  }

  private String getClusterDeletedStorePath(String clusterName, String storeName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + STORE_GRAVEYARD_PATH + "/" + storeName;
  }
}
