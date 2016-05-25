package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadonlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import org.apache.log4j.Logger;


/**
 * Utils used to calculate partition count for new version. If the version is the first one of the given store,
 * calculate the number by given store size and partition size. Otherwise use the number from the current active
 * version.
 */
public class PartitionCountUtils {
  private static final Logger logger = Logger.getLogger(PartitionCountUtils.class);

  // TODO. As there are a lot of parameters, we could transfer a configuration and keep some state instead of a utility static method.
  public static int calculatePartitionCount(String clusterName, String storeName, long storeSizeBytes,
      ReadonlyStoreRepository storeRepository, RoutingDataRepository routingDataRepository, long partitionSize,
      int minPartitionCount, int maxPartitionCount) {
    if (storeSizeBytes <= 0) {
      throw new VeniceException("Store size:" + storeSizeBytes + "is invalid.");
    }
    Store store = storeRepository.getStore(storeName);
    int previousPartitionCount = store.getPartitionCount();
    if (previousPartitionCount == 0) {
      // First Version, calculate partition count
      long partitionCount = storeSizeBytes / partitionSize;
      if (partitionCount > maxPartitionCount) {
        partitionCount = maxPartitionCount;
      } else if (partitionCount < minPartitionCount) {
        partitionCount = minPartitionCount;
      }
      logger.info("Assign partition count:" + partitionCount + " by given size:" + storeSizeBytes
          + " for the first version of store:" + storeName);
      return (int)partitionCount;
    } else {
      // Active version exists, use the partition count calculated before.
      logger.info("Assign partition count:" + previousPartitionCount +
          "  , which come from previous version, for store:" + storeName);
      return previousPartitionCount;
    }
  }
}
