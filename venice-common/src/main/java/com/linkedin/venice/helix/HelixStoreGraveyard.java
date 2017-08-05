package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.Optional;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;


public class HelixStoreGraveyard implements StoreGraveyard {
  private static final Logger logger = Logger.getLogger(StoreGraveyard.class);
  /**
   * If store does NOT existing in graveyard, return -1 as last deletion time to indicate that this store has never
   * been deleted.
   */
  public static final long TIME_NEVER_BEEN_DELETED = -1L;

  public static final String STORE_GRAVEYARD_PATH = "/StoreGraveyard";

  protected ZkBaseDataAccessor<Store> dataAccessor;

  private final ZkClient zkClient;

  private final String clusterName;

  private final int retryCount = 3;

  public HelixStoreGraveyard(ZkClient zkClient, HelixAdapterSerializer adapterSerializer, String clusterName){
    this(zkClient, adapterSerializer,clusterName, new StoreJSONSerializer());
  }
  public HelixStoreGraveyard(ZkClient zkClient, HelixAdapterSerializer adapterSerializer, String clusterName,
      VeniceSerializer<Store> storeSerializer) {
    this.clusterName = clusterName;
    adapterSerializer.registerSerializer(getDeletedStorePath(PathResourceRegistry.WILDCARD_MATCH_ANY), storeSerializer);
    zkClient.setZkSerializer(adapterSerializer);
    this.zkClient = zkClient;
    dataAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  @Override
  public int getLargestUsedVersionNumber(String storeName) {
    Optional<Store> store = getStore(storeName);
    if (store.isPresent()) {
      int largestUsedVersionNumber = store.get().getLargestUsedVersionNumber();
      logger.info("Found store: " + storeName + " in the store graveyard. Will initialize the new store at version: "
          + largestUsedVersionNumber);
      return largestUsedVersionNumber;
    } else {
      logger.info(
          "Store: " + storeName + " does NOT exist in the store graveyard. Will initialize the new store at version: "
              + Store.NON_EXISTING_VERSION);
      // If store does NOT existing in graveyard, it means store has never been deleted, return 0 which is the default
      // value of largestUsedVersionNumber for a new store.
      return Store.NON_EXISTING_VERSION;
    }
  }

  @Override
  public long getLastDeletionTime(String storeName) {

    Stat stat = dataAccessor.getStat(getDeletedStorePath(storeName), AccessOption.PERSISTENT);
    if (stat != null) {
      //TODO verify in zooinspector that mtime is correct even if the node just be created.
      return stat.getMtime();
    } else {
      logger.info("Store: " + storeName + " does NOT exist in the store graveyard.");
      return TIME_NEVER_BEEN_DELETED;
    }
  }

  @Override
  public void putStoreIntoGraveyard(Store store) {
    Optional<Store> deletedStore = getStore(store.getName());
    if (deletedStore.isPresent()) {
      // Store already exists.
      if (store.getLargestUsedVersionNumber() < deletedStore.get().getLargestUsedVersionNumber()) {
        // largestUsedVersion number in re-created store is smaller than the deleted store. It's should be a issue.
        String errorMsg = "Invalid largestUsedVersionNumber: " + store.getLargestUsedVersionNumber() +
            " in Store: " + store.getName() + ", it's smaller than one found in graveyard: " + deletedStore.get()
            .getLargestUsedVersionNumber();
        logger.error(errorMsg);
        throw new VeniceException(errorMsg);
      }
    }
    // Store does not exist in graveyard OR store already exists but the re-created store is deleted again so we need to
    // update the ZNode.
    HelixUtils.update(dataAccessor, getDeletedStorePath(store.getName()), store, retryCount);
    logger.info("Put store: " + store.getName() + " into graveyard.");
  }

  private Optional<Store> getStore(String storeName) {
    Store store = dataAccessor.get(getDeletedStorePath(storeName), null, AccessOption.PERSISTENT);
    return store == null ? Optional.empty() : Optional.of(store);
  }

  private String getDeletedStorePath(String storeName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + STORE_GRAVEYARD_PATH + "/" + storeName;
  }
}
