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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Throttler used to limit how many read request could hit this store and each storage node which has been assigned
 * with
 * this store's  replicas.
 */
public class StoreReadThrottler {
  private static final Logger LOGGER = LogManager.getLogger(StoreReadThrottler.class);
  private final String storeName;
  private final long localQuota;
  private final EventThrottlingStrategy throttlingStrategy;
  private final EventThrottler storeThrottler;
  private final double perStorageNodeReadQuotaBuffer;
  private final long storageNodeQuotaCheckTimeWindow;

  private int currentVersion = Store.NON_EXISTING_VERSION;

  /**
   * The map which's is storage node Id and value is a reads throttler.
   * <p>
   * This class is thread safe. Only one thread could access to the method to update storage nodes' throttlers.
   */
  private ConcurrentMap<String, EventThrottler> storageNodesThrottlers;

  public StoreReadThrottler(
      String storeName,
      long localQuota,
      EventThrottlingStrategy throttlingStrategy,
      Optional<PartitionAssignment> partitionAssignment,
      double perStorageNodeReadQuotaBuffer,
      long storeQuotaCheckTimeWindow,
      long storageNodeQuotaCheckTimeWindow) {
    this.storeName = storeName;
    this.localQuota = localQuota;
    this.throttlingStrategy = throttlingStrategy;
    this.perStorageNodeReadQuotaBuffer = perStorageNodeReadQuotaBuffer;
    storageNodesThrottlers = new ConcurrentHashMap<>();
    storeThrottler =
        new EventThrottler(localQuota, storeQuotaCheckTimeWindow, storeName + "-throttler", true, throttlingStrategy);
    this.storageNodeQuotaCheckTimeWindow = storageNodeQuotaCheckTimeWindow;
    if (partitionAssignment.isPresent()) {
      updateStorageNodesThrottlers(partitionAssignment.get());
    }
  }

  public void mayThrottleRead(double readCapacityUnit, String storageNodeId) {
    if (storageNodeId != null) {
      EventThrottler storageNodeThrottler = storageNodesThrottlers.get(storageNodeId);
      // TODO While updating storage nodes' throttlers, there might be a very short period that we haven't create a
      // TODO throttler for the given storage node. Right now just accept this request, could add a default quota later.
      if (storageNodeThrottler != null) {
        storageNodeThrottler.maybeThrottle(readCapacityUnit);
      }
    }
    storeThrottler.maybeThrottle(readCapacityUnit);
  }

  public synchronized void updateStorageNodesThrottlers(PartitionAssignment partitionAssignment) {
    this.currentVersion = Version.parseVersionFromKafkaTopicName(partitionAssignment.getTopic());
    // Calculated the latest quota for each storage node.
    Map<String, Long> storageNodeQuotaMap = new HashMap<>();
    long partitionQuota = Math.max(localQuota / partitionAssignment.getExpectedNumberOfPartitions(), 10);
    for (Partition partition: partitionAssignment.getAllPartitions()) {
      List<Instance> readyToServeInstances = partition.getReadyToServeInstances();
      for (Instance instance: readyToServeInstances) {
        long replicaQuota = Math.max(partitionQuota / readyToServeInstances.size(), 5);
        if (storageNodeQuotaMap.containsKey(instance.getNodeId())) {
          replicaQuota = storageNodeQuotaMap.get(instance.getNodeId()) + replicaQuota;
        }
        storageNodeQuotaMap.put(instance.getNodeId(), replicaQuota);
      }
    }

    int[] addedOrUpdated = new int[1];

    // Update throttler for the storage node which is a new node or if the quota has been changed.
    // Add a buffer to per storage node quota to make our throttler more lenient.
    storageNodeQuotaMap.entrySet()
        .stream()
        .filter(
            entry -> !storageNodesThrottlers.containsKey(entry.getKey()) || storageNodesThrottlers.get(entry.getKey())
                .getMaxRatePerSecond() != (long) (entry.getValue() * (1 + perStorageNodeReadQuotaBuffer)))
        .forEach(entry -> {
          storageNodesThrottlers.put(
              entry.getKey(),
              new EventThrottler(
                  (long) (entry.getValue() * (1 + perStorageNodeReadQuotaBuffer)),
                  storageNodeQuotaCheckTimeWindow,
                  storeName + "-" + entry.getKey() + "-throttler",
                  true,
                  throttlingStrategy));
          addedOrUpdated[0]++;
        });
    int deleted = 0;
    // Delete the throttler for the storage node which has been deleted from the latest partition assignment.
    Iterator<String> iterator = storageNodesThrottlers.keySet().iterator();
    while (iterator.hasNext()) {
      if (!storageNodeQuotaMap.containsKey(iterator.next())) {
        iterator.remove();
        deleted++;
      }
    }

    if (addedOrUpdated[0] != 0 || deleted != 0) {
      LOGGER.info(
          "Added or updated throttlers for {} storage nodes. Deleted: {} throttlers for storage nodes. Store: {} currentVersion: {}",
          addedOrUpdated[0],
          deleted,
          storeName,
          currentVersion);
    }
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
