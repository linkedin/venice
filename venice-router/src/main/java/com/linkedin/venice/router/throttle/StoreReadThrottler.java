package com.linkedin.venice.router.throttle;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.throttle.EventThrottlingStrategy;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


/**
 * Throttler used to limit how many read request could hit this store and each storage node which has been assigned
 * with
 * this store's  replicas.
 */
public class StoreReadThrottler {
  private static final Logger logger = Logger.getLogger(StoreReadThrottler.class);
  private final String storeName;
  private final long localQuota;
  private final EventThrottlingStrategy throttlingStrategy;
  private final EventThrottler storeThrottler;

  private int currentVersion = Store.NON_EXISTING_VERSION;

  /**
   * The map which's is storage node Id and value is a reads throttler.
   * <p>
   * This class is thread safe. Only one thread could access to the method to update storage nodes' throttlers.
   */
  private ConcurrentMap<String, EventThrottler> storageNodesThrottlers;

  public StoreReadThrottler(String storeName, long localQuota, EventThrottlingStrategy throttlingStrategy,
      Optional<PartitionAssignment> partitionAssignment) {
    this.storeName = storeName;
    this.localQuota = localQuota;
    this.throttlingStrategy = throttlingStrategy;
    storageNodesThrottlers = new ConcurrentHashMap<>();
    storeThrottler = new EventThrottler(localQuota, true, throttlingStrategy);
    if (partitionAssignment.isPresent()) {
      updateStorageNodesThrottlers(partitionAssignment.get());
    }
  }

  public void mayThrottleRead(int readCapacityUnit, String storageNodeId) {
    EventThrottler storageNodeThrottler = storageNodesThrottlers.get(storageNodeId);
    // TODO While updating storage nodes' throttlers, there might be a very short period that we haven't create a
    // TODO throttler for the given storage node. Right now just accept this request, could add a default quota later.
    if (storageNodeThrottler != null) {
      storageNodeThrottler.maybeThrottle(readCapacityUnit);
    }
    storeThrottler.maybeThrottle(readCapacityUnit);
  }

  public synchronized void updateStorageNodesThrottlers(PartitionAssignment partitionAssignment) {
    this.currentVersion = Version.parseVersionFromKafkaTopicName(partitionAssignment.getTopic());
    logger.info("Updating throttlers for each storage node. Store: " + storeName + " currentVersion:" + currentVersion);

    // Calculated the latest quota for each storage node.
    Map<String, Long> storageNodeQuotaMap = new HashMap<>();
    long partitionQuota = localQuota / partitionAssignment.getExpectedNumberOfPartitions();
    for (Partition partition : partitionAssignment.getAllPartitions()) {
      List<Instance> readyToServeInstances = partition.getReadyToServeInstances();
      for (Instance instance : readyToServeInstances) {
        long replicaQuota = partitionQuota / readyToServeInstances.size();
        if (storageNodeQuotaMap.containsKey(instance.getNodeId())) {
          replicaQuota = storageNodeQuotaMap.get(instance.getNodeId()) + replicaQuota;
        }
        storageNodeQuotaMap.put(instance.getNodeId(), replicaQuota);
      }
    }

    // Update throttler for the storage node which is a new node or the its quota has been changed.
    storageNodeQuotaMap.entrySet()
        .stream()
        .filter(entry -> !storageNodesThrottlers.containsKey(entry.getKey())
            || storageNodesThrottlers.get(entry.getKey()).getMaxRatePerSecond() != entry.getValue())
        .forEach(entry -> storageNodesThrottlers.put(entry.getKey(),
            // We could add a buffer to the quota per storage node if needed in the future.
            new EventThrottler(entry.getValue(), true, throttlingStrategy)));

    // Delete the throttler for the storage node which has been deleted from the latest partition assignment.
    Iterator<String> iterator = storageNodesThrottlers.keySet().iterator();
    while (iterator.hasNext()) {
      if (!storageNodeQuotaMap.containsKey(iterator.next())) {
        iterator.remove();
      }
    }

    logger.info("Updated throttlers for each storage. Store: " + storeName + " currentVersion:" + currentVersion);
  }

  /**
   * Clear all throttlers for storage nodes. Put current version to 0. So this throttler goes back to the status that
   * the store just be created and no current version is assigned.
   */
  public synchronized void clearStorageNodesThrottlers() {
    currentVersion = Store.NON_EXISTING_VERSION;
    storageNodesThrottlers.clear();
  }

  public long getQuota() {
    return localQuota;
  }

  public synchronized int getCurrentVersion() {
    return currentVersion;
  }

  protected long getQuotaForStorageNode(String storageNodeId) {
    EventThrottler storageNodeThrottler = storageNodesThrottlers.get(storageNodeId);
    if (storageNodeThrottler != null) {
      return storageNodeThrottler.getMaxRatePerSecond();
    } else {
      return -1;
    }
  }
}
