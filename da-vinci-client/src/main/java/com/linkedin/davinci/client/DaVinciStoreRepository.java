package com.linkedin.davinci.client;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;

import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class DaVinciStoreRepository extends HelixReadOnlyStoreRepository {
  private static final Logger logger = Logger.getLogger(HelixReadOnlyStoreRepository.class);

  private final Set<String> subscription = new HashSet<>();

  public DaVinciStoreRepository(ZkClient zkClient, HelixAdapterSerializer compositeSerializer, String clusterName) {
    super(zkClient, compositeSerializer, clusterName, 0, 0);
  }

  public void subscribe(String storeName) {
    updateLock.lock();
    try {
      subscription.add(storeName);
      refreshOneStore(storeName);
    } finally {
      updateLock.unlock();
    }
  }

  @Override
  protected Store putStore(Store newStore) {
    updateLock.lock();
    try {
      if (!VeniceSystemStoreUtils.isSystemStore(newStore.getName()) &&
          !subscription.contains(newStore.getName())) {
        logger.info("Ignoring not subscribed store put, storeName=" + newStore.getName());
        return null;
      }
      return super.putStore(newStore);
    } finally {
      updateLock.unlock();
    }
  }
}
