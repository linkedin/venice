package com.linkedin.venice.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PartitionUtils {
  private static final Logger LOGGER = LogManager.getLogger(PartitionUtils.class);

  /**
   * Calculate partition count for new version. If store level partition count is not configured (0),
   * calculate the number by storage quota and partition size. Otherwise, use the store level partition count.
   */
  public static int calculatePartitionCount(
      String storeName,
      long storageQuota,
      int storePartitionCount,
      long partitionSize,
      int minPartitionCount,
      int maxPartitionCount,
      boolean isRoundUpEnabled,
      int roundUpSize) {

    if (storePartitionCount != 0) {
      // Store level partition count is configured, use the number
      LOGGER.info(
          "Assign partition count: {} from store level config to the new version of store: {}",
          storePartitionCount,
          storeName);
      return storePartitionCount;
    }

    if (storageQuota == Store.UNLIMITED_STORAGE_QUOTA) {
      LOGGER.info(
          "Assign partition count: {} calculated by storage quota: {} to the new version of store: {}",
          maxPartitionCount,
          storageQuota,
          storeName);
      return maxPartitionCount;
    }

    if (storageQuota <= 0) {
      throw new VeniceException("Storage quota: " + storageQuota + " is invalid.");
    }

    // Store level partition count is not configured, calculate partition count
    long partitionCount = storageQuota / partitionSize;
    if (isRoundUpEnabled) {
      // Round upwards to the next multiple of roundUpSize
      partitionCount = ((partitionCount + roundUpSize - 1) / roundUpSize) * roundUpSize;
    }
    if (partitionCount > maxPartitionCount) {
      partitionCount = maxPartitionCount;
    } else if (partitionCount < minPartitionCount) {
      partitionCount = minPartitionCount;
    }
    LOGGER.info(
        "Assign partition count: {} calculated by storage quota: {} to the new version of store: {}",
        partitionCount,
        storageQuota,
        storeName);
    return (int) partitionCount;
  }

  public static VenicePartitioner getVenicePartitioner(PartitionerConfig config) {
    Properties params = new Properties();
    if (config.getPartitionerParams() != null) {
      params.putAll(config.getPartitionerParams());
    }
    return getVenicePartitioner(config.getPartitionerClass(), new VeniceProperties(params));
  }

  /**
   * This util method returns a new venice partitioner that works for user-level partition, regardless of the amplification
   * factor in the partitioner config.
   */
  public static VenicePartitioner getUserPartitionLevelVenicePartitioner(PartitionerConfig config) {
    // Explicitly handle null config case.
    if (config == null) {
      config = new PartitionerConfigImpl();
    }
    Properties params = new Properties();
    if (config.getPartitionerParams() != null) {
      params.putAll(config.getPartitionerParams());
    }
    return getVenicePartitioner(config.getPartitionerClass(), new VeniceProperties(params));
  }

  public static VenicePartitioner getVenicePartitioner(String partitionerClass, String partitionerParamsString) {
    Properties params = new Properties();
    Map<String, String> partitionerParamsMap = getPartitionerParamsMap(partitionerParamsString);
    params.putAll(partitionerParamsMap);
    return getVenicePartitioner(partitionerClass, new VeniceProperties(params), null);
  }

  public static Map<String, String> getPartitionerParamsMap(String partitionerParamsString) {
    if (partitionerParamsString == null) {
      return Collections.emptyMap();
    }
    try {
      return ObjectMapperFactory.getInstance().readValue(partitionerParamsString, Map.class);
    } catch (JsonProcessingException e) {
      throw new VeniceException("Invalid partitioner params string: " + partitionerParamsString, e);
    }
  }

  public static VenicePartitioner getVenicePartitioner(String partitionerClass, VeniceProperties params) {
    return getVenicePartitioner(partitionerClass, params, null);
  }

  public static VenicePartitioner getVenicePartitioner(
      String partitionerClass,
      VeniceProperties params,
      Schema keySchema) {
    VenicePartitioner partitioner = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(partitionerClass),
        new Class<?>[] { VeniceProperties.class, Schema.class },
        new Object[] { params, keySchema });
    return partitioner;
  }

  public static VenicePartitioner getVenicePartitioner(VeniceProperties props) {
    if (props.containsKey(ConfigKeys.PARTITIONER_CLASS)) {
      String partitionerClass = props.getString(ConfigKeys.PARTITIONER_CLASS);
      return getVenicePartitioner(partitionerClass, props);
    } else {
      return new DefaultVenicePartitioner(props);
    }
  }
}
