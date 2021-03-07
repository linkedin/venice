package com.linkedin.venice.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.UserPartitionAwarePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class PartitionUtils {
  private static final Logger logger = Logger.getLogger(PartitionUtils.class);

  /**
   * Calculate partition count for new version. If the version is the first one of the given store,
   * calculate the number by given store size and partition size. Otherwise use the number from the current active
   * version.
   */
  // TODO. As there are a lot of parameters, we could transfer a configuration and keep some state instead of a utility static method.
  public static int calculatePartitionCount(String clusterName, String storeName, long storeSizeBytes,
                                            ReadOnlyStoreRepository storeRepository, RoutingDataRepository routingDataRepository, long partitionSize,
                                            int minPartitionCount, int maxPartitionCount) {
    if (storeSizeBytes <= 0) {
      throw new VeniceException("Store size:" + storeSizeBytes + "is invalid.");
    }
    Store store = storeRepository.getStoreOrThrow(storeName);
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

  private static void checkAmplificationFactor(int amplificationFactor) {
    if (amplificationFactor < 1) {
      throw new VeniceException(
          String.format("Invalid amplification factor %d. Amplification factor must be >= 1.", amplificationFactor));
    }
  }

  public static List<Integer> getSubPartitions(Collection<Integer> userPartitions, int amplificationFactor) {
    checkAmplificationFactor(amplificationFactor);
    List<Integer> subPartitions = new ArrayList<>();
    for (Integer userPartition : userPartitions) {
      int subPartition = userPartition * amplificationFactor;
      for (int i = 0; i < amplificationFactor; ++i, ++subPartition) {
        subPartitions.add(subPartition);
      }
    }
    return subPartitions;
  }

  public static List<Integer> getUserPartitions(Collection<Integer> subPartitions, int amplificationFactor) {
    List<Integer> userPartitions = new ArrayList<>();
    for (Integer subPartition : subPartitions) {
      userPartitions.add(getUserPartition(subPartition, amplificationFactor));
    }
    return userPartitions;
  }

  public static int getUserPartition(int subPartition, int amplificationFactor) {
    checkAmplificationFactor(amplificationFactor);
    return subPartition / amplificationFactor;
  }

  public static VenicePartitioner getVenicePartitioner(PartitionerConfig config) {
    Properties params = new Properties();
    if (config.getPartitionerParams() != null) {
      params.putAll(config.getPartitionerParams());
    }
    return getVenicePartitioner(
        config.getPartitionerClass(),
        config.getAmplificationFactor(),
        new VeniceProperties(params));
  }

  public static VenicePartitioner getVenicePartitioner(
      String partitionerClass,
      int amplificationFactor,
      VeniceProperties params) {
    VenicePartitioner partitioner = ReflectUtils.callConstructor(ReflectUtils.loadClass(partitionerClass),
        new Class<?>[]{VeniceProperties.class}, new Object[]{params});
    if (amplificationFactor == 1) {
      return partitioner;
    }
    return new UserPartitionAwarePartitioner(partitioner, amplificationFactor);
  }

  public static VenicePartitioner getVenicePartitioner(VeniceProperties props) {
    if (props.containsKey(ConfigKeys.PARTITIONER_CLASS)) {
      String partitionerClass = props.getString(ConfigKeys.PARTITIONER_CLASS);
      int amplificationFactor;
      if (props.containsKey(ConfigKeys.AMPLIFICATION_FACTOR)) {
        amplificationFactor = props.getInt(ConfigKeys.AMPLIFICATION_FACTOR);
      } else {
        amplificationFactor = 1;
      }
      return getVenicePartitioner(partitionerClass, amplificationFactor, props);
    } else {
      return new DefaultVenicePartitioner(props);
    }
  }
}
