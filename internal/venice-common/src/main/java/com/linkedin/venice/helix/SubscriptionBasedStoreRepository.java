package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SubscriptionBasedStoreRepository extends HelixReadOnlyStoreRepository
    implements SubscriptionBasedReadOnlyStoreRepository {
  private static final Logger LOGGER = LogManager.getLogger(HelixReadOnlyStoreRepository.class);

  private final Set<String> subscription = new HashSet<>();

  public SubscriptionBasedStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String clusterName) {
    super(zkClient, compositeSerializer, clusterName);
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      subscription.add(storeName);
      Store store = refreshOneStore(storeName);
      if (store == null) {
        subscription.remove(storeName);
        throw new VeniceNoStoreException(storeName, clusterName);
      }
    }
  }

  @Override
  public void unsubscribe(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      if (!subscription.remove(storeName)) {
        throw new VeniceException("Cannot unsubscribe from not-subscribed store, storeName=" + storeName);
      }
      removeStore(storeName);
    }
  }

  @Override
  protected Store putStore(Store newStore) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(newStore.getName())) {
      if (subscription.contains(newStore.getName()) || VeniceSystemStoreUtils.isSystemStore(newStore.getName())) {
        return super.putStore(newStore);
      }
      LOGGER.info("Ignoring not-subscribed store, storeName=" + newStore.getName());
      return null;
    }
  }
}
