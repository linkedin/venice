package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.common.VeniceSystemStore;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.StoreConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.log4j.Logger;


/**
 * This class is used to fetch all store configs from ZK and cached locally, then provide the way to read those
 * configs.
 * Also it will listen on the store configs change event and keep the cache up to date.
 * <p/>
 * This class is non-cluster specified.
 */
public class HelixReadOnlyStoreConfigRepository implements ReadOnlyStoreConfigRepository, VeniceResource {
  public static final Logger logger = Logger.getLogger(HelixReadOnlyStoreConfigRepository.class);

  private AtomicReference<Map<String, StoreConfig>> storeConfigMap;

  private ZkStoreConfigAccessor accessor;

  private StoreConfigChangedListener storeConfigChangedListener;
  private StoreConfigAddedOrDeletedChangedListener storeConfigAddedOrDeletedListener;

  private ZkClient zkClient;
  private final CachedResourceZkStateListener zkStateListener;
  private final int refreshAttemptsForZkReconnect;
  private final long refreshIntervalForZkReconnectInMs;

  public HelixReadOnlyStoreConfigRepository(ZkClient zkClient, HelixAdapterSerializer adapterSerializer,
      int refreshAttemptsForZkReconnect, long refreshIntervalForZkReconnectInMs) {
    this(zkClient, new ZkStoreConfigAccessor(zkClient, adapterSerializer), refreshAttemptsForZkReconnect,
        refreshIntervalForZkReconnectInMs);
  }

  public HelixReadOnlyStoreConfigRepository(ZkClient zkClient, ZkStoreConfigAccessor accessor,
      int refreshAttemptsForZkReconnect, long refreshIntervalForZkReconnectInMs) {
    this.zkClient = zkClient;
    this.accessor = accessor;
    this.storeConfigMap = new AtomicReference<>(new HashMap<>());
    storeConfigChangedListener = new StoreConfigChangedListener();
    storeConfigAddedOrDeletedListener = new StoreConfigAddedOrDeletedChangedListener();
    this.refreshAttemptsForZkReconnect = refreshAttemptsForZkReconnect;
    this.refreshIntervalForZkReconnectInMs = refreshIntervalForZkReconnectInMs;
    // This repository already retry on getChildren, so do not need extra retry in listener.
    zkStateListener =
        new CachedResourceZkStateListener(this);
  }

  @Override
  public void refresh() {
    logger.info("Loading all store configs from zk.");
    accessor.subscribeStoreConfigAddedOrDeletedListener(storeConfigAddedOrDeletedListener);
    List<StoreConfig> configList = accessor.getAllStoreConfigs(refreshAttemptsForZkReconnect, refreshIntervalForZkReconnectInMs);
    logger.info("Found " + configList.size() + " store configs.");
    Map<String, StoreConfig> configMap = new HashMap<>();
    for (StoreConfig config : configList) {
      configMap.put(config.getStoreName(), config);
      accessor.subscribeStoreConfigDataChangedListener(config.getStoreName(), storeConfigChangedListener);
    }
    storeConfigMap.set(configMap);
    zkClient.subscribeStateChanges(zkStateListener);
    logger.info("All store configs are loaded.");
  }

  @Override
  public void clear() {
    logger.info("Clearing all store configs in local");
    accessor.unsubscribeStoreConfigAddedOrDeletedListener(storeConfigAddedOrDeletedListener);
    for (String storeName : storeConfigMap.get().keySet()) {
      accessor.unsubscribeStoreConfigDataChangedListener(storeName, storeConfigChangedListener);
    }
    this.storeConfigMap.set(Collections.emptyMap());
    zkClient.unsubscribeStateChanges(zkStateListener);
    logger.info("Cleared all store configs in local");
  }

  /**
   * The corresponding Venice store config is returned for metadata system store's store config. This is the most
   * natural way to handle cluster discovery for metadata system stores and store migration.
   */
  @Override
  public Optional<StoreConfig> getStoreConfig(String storeName) {
    String veniceStoreName = VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStore.METADATA_STORE
        ? VeniceSystemStoreUtils.getStoreNameFromMetadataStoreName(storeName): storeName;
    StoreConfig config = storeConfigMap.get().get(veniceStoreName);
    if (config != null) {
      return Optional.of(config);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public List<StoreConfig> getAllStoreConfigs() {
    return new ArrayList<>(storeConfigMap.get().values());
  }

  protected StoreConfigAddedOrDeletedChangedListener getStoreConfigAddedOrDeletedListener() {
    return storeConfigAddedOrDeletedListener;
  }

  protected StoreConfigChangedListener getStoreConfigChangedListener() {
    return storeConfigChangedListener;
  }

  protected class StoreConfigAddedOrDeletedChangedListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren)
        throws Exception {
      synchronized (storeConfigMap) {
        Map<String, StoreConfig> map = new HashMap<>(storeConfigMap.get());
        List<String> newStores =
            currentChildren.stream().filter(newStore -> !map.containsKey(newStore)).collect(Collectors.toList());

        Set<String> deletedStores = new HashSet<>(map.keySet());
        currentChildren.forEach(deletedStores::remove);
        logger.info("Store configs list is changed. " + newStores.size() + " new configs. And will delete "
            + deletedStores.size() + " configs.");
        //New added store configs
        List<StoreConfig> newConfigs = accessor.getStoreConfigs(newStores);
        for (StoreConfig config : newConfigs) {
          map.put(config.getStoreName(), config);
          accessor.subscribeStoreConfigDataChangedListener(config.getStoreName(), storeConfigChangedListener);
        }

        // Deleted store configs
        for (String deletedStore : deletedStores) {
          map.remove(deletedStore);
          accessor.unsubscribeStoreConfigDataChangedListener(deletedStore, storeConfigChangedListener);
        }
        storeConfigMap.set(map);
        logger.info("Store configs map is updated.");
      }
    }
  }

  protected class StoreConfigChangedListener implements IZkDataListener {

    @Override
    public void handleDataChange(String dataPath, Object data)
        throws Exception {
      if (!(data instanceof StoreConfig)) {
        throw new VeniceException(
            "Invalid data from zk notification. Required: StoreConfig, but get: " + data.getClass().getName());
      }
      StoreConfig config = (StoreConfig) data;
      logger.info("Store config is changed in ZK, store: "+config.getCluster());
      synchronized (storeConfigMap) {
        Map<String, StoreConfig> map = new HashMap<>(storeConfigMap.get());
        map.put(config.getStoreName(), config);
        storeConfigMap.set(map);
      }
      logger.info("Updated store config locally, store: " + config.getStoreName());
    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
      //ignore, already been handled in handleChildChange
    }
  }
}
