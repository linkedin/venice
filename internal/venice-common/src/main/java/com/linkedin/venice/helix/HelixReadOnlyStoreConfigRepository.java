package com.linkedin.venice.helix;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to fetch all store configs from ZK and cached locally, then provide the way to read those
 * configs.
 * Also it will listen on the store configs change event and keep the cache up to date.
 * <p/>
 * This class is non-cluster specified.
 */
public class HelixReadOnlyStoreConfigRepository implements ReadOnlyStoreConfigRepository, VeniceResource {
  private static final Logger LOGGER = LogManager.getLogger(HelixReadOnlyStoreConfigRepository.class);

  private final Map<String, StoreConfig> loadedStoreConfigMap;
  private final AtomicReference<Set<String>> availableStoreSet;
  private final ZkStoreConfigAccessor accessor;
  private final StoreConfigChangedListener storeConfigChangedListener;
  private final StoreConfigAddedOrDeletedChangedListener storeConfigAddedOrDeletedListener;
  private final ZkClient zkClient;
  private final CachedResourceZkStateListener zkStateListener;

  public HelixReadOnlyStoreConfigRepository(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    this(zkClient, new ZkStoreConfigAccessor(zkClient, adapterSerializer, Optional.empty()));
  }

  public HelixReadOnlyStoreConfigRepository(ZkClient zkClient, ZkStoreConfigAccessor accessor) {
    this.zkClient = zkClient;
    this.accessor = accessor;
    this.loadedStoreConfigMap = new VeniceConcurrentHashMap<>();
    this.availableStoreSet = new AtomicReference<>(new HashSet<>());
    storeConfigChangedListener = new StoreConfigChangedListener();
    storeConfigAddedOrDeletedListener = new StoreConfigAddedOrDeletedChangedListener();
    // This repository already retry on getChildren, so do not need extra retry in listener.
    zkStateListener = new CachedResourceZkStateListener(this);
  }

  /**
   * Obtain all available stores and load them into cache, but it doesn't fetch the store configs and attach ZK watch yet
   */
  @Override
  public void refresh() {
    LOGGER.info("Loading all store names from zk.");
    accessor.subscribeStoreConfigAddedOrDeletedListener(storeConfigAddedOrDeletedListener);
    availableStoreSet.set(new HashSet<>(accessor.getAllStores()));
    LOGGER.info("Found {} stores.", availableStoreSet.get().size());
    zkClient.subscribeStateChanges(zkStateListener);
    LOGGER.info("All store names are loaded.");
  }

  @Override
  public void clear() {
    LOGGER.info("Clearing all store configs in local");
    accessor.unsubscribeStoreConfigAddedOrDeletedListener(storeConfigAddedOrDeletedListener);
    for (String storeName: loadedStoreConfigMap.keySet()) {
      accessor.unsubscribeStoreConfigDataChangedListener(storeName, storeConfigChangedListener);
    }
    this.loadedStoreConfigMap.clear();
    this.availableStoreSet.set(Collections.emptySet());
    zkClient.unsubscribeStateChanges(zkStateListener);
    LOGGER.info("Cleared all store configs in local");
  }

  /**
   * Get the store config by store name. It would fetch the store config from ZK if it's not in cache yet and attach ZK
   * watch.
   * The corresponding Venice store config is returned for metadata system store's store config. This is the most
   * natural way to handle cluster discovery for metadata system stores and store migration.
   */
  @Override
  public Optional<StoreConfig> getStoreConfig(String storeName) {
    String veniceStoreName = storeName;
    // To handle meta system store specifically
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.META_STORE)) {
      veniceStoreName = VeniceSystemStoreType.META_STORE.extractRegularStoreName(storeName);
    }
    if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
      veniceStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.extractRegularStoreName(storeName);
    }

    if (availableStoreSet.get().contains(veniceStoreName)) {
      StoreConfig config = loadedStoreConfigMap.get(veniceStoreName);
      if (config == null) {
        // lazy fetch from ZK and attach watch
        config = accessor.getStoreConfig(veniceStoreName);
        if (config == null) {
          LOGGER.warn("Store config is not found for store: {}", veniceStoreName);
        } else {
          loadedStoreConfigMap.put(config.getStoreName(), config);
          accessor.subscribeStoreConfigDataChangedListener(veniceStoreName, storeConfigChangedListener);
          StoreConfig configToVerify = accessor.getStoreConfig(veniceStoreName);
          if (!configToVerify.equals(config)) {
            LOGGER.debug("Store config is changed during fetching. Reload the store config for {}", configToVerify);
            config = configToVerify;
            loadedStoreConfigMap.put(config.getStoreName(), config);
          }
        }
      }
      return Optional.ofNullable(config != null ? config.cloneStoreConfig() : null);
    }
    return Optional.empty();
  }

  @Override
  public StoreConfig getStoreConfigOrThrow(String storeName) {
    Optional<StoreConfig> storeConfig = getStoreConfig(storeName);
    if (!storeConfig.isPresent()) {
      throw new VeniceNoStoreException(storeName);
    }
    return storeConfig.get();
  }

  @VisibleForTesting
  StoreConfigAddedOrDeletedChangedListener getStoreConfigAddedOrDeletedListener() {
    return storeConfigAddedOrDeletedListener;
  }

  @VisibleForTesting
  StoreConfigChangedListener getStoreConfigChangedListener() {
    return storeConfigChangedListener;
  }

  @VisibleForTesting
  Set<String> getAvailableStoreSet() {
    return availableStoreSet.get();
  }

  @VisibleForTesting
  Map<String, StoreConfig> getLoadedStoreConfigMap() {
    return loadedStoreConfigMap;
  }

  protected class StoreConfigAddedOrDeletedChangedListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
      synchronized (availableStoreSet) {
        Set<String> storeSetSnapshot = new HashSet<>(availableStoreSet.get());
        long newStoresCount = currentChildren.stream().filter(newStore -> !storeSetSnapshot.contains(newStore)).count();

        // obtain the stores that are removed
        currentChildren.forEach(storeSetSnapshot::remove);
        LOGGER.debug(
            "Store configs list is changed. {} new configs. And will delete {} configs.",
            newStoresCount,
            storeSetSnapshot.size());

        // update the available store set
        availableStoreSet.set(new HashSet<>(currentChildren));

        // Deleted store configs
        for (String deletedStore: storeSetSnapshot) {
          loadedStoreConfigMap.remove(deletedStore);
          accessor.unsubscribeStoreConfigDataChangedListener(deletedStore, storeConfigChangedListener);
        }
      }
    }
  }

  protected class StoreConfigChangedListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) {
      if (!(data instanceof StoreConfig)) {
        throw new VeniceException(
            "Invalid data from zk notification. Required: StoreConfig, but get: " + data.getClass().getName());
      }
      StoreConfig config = (StoreConfig) data;
      loadedStoreConfigMap.put(config.getStoreName(), config);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // ignore, already been handled in handleChildChange
    }
  }
}
