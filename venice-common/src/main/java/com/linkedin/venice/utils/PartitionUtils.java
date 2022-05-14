package com.linkedin.venice.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.UserPartitionAwarePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PartitionUtils {
  private static final Logger logger = LogManager.getLogger(PartitionUtils.class);

  /**
   * Calculate partition count for new version. If the version is the first one of the given store,
   * calculate the number by given store size and partition size. Otherwise use the number from the current active
   * version.
   */
  public static int calculatePartitionCount(
      String storeName,
      long storeSizeBytes,
      int previousPartitionCount,
      long partitionSize,
      int minPartitionCount,
      int maxPartitionCount) {
    if (storeSizeBytes <= 0) {
      throw new VeniceException("Store size: " + storeSizeBytes + " is invalid.");
    }
    if (previousPartitionCount == 0) {
      // First Version, calculate partition count
      long partitionCount = storeSizeBytes / partitionSize;
      if (partitionCount > maxPartitionCount) {
        partitionCount = maxPartitionCount;
      } else if (partitionCount < minPartitionCount) {
        partitionCount = minPartitionCount;
      }
      logger.info("Assign partition count: {} by given size: {} for the first version of store: {}",
          partitionCount, storeSizeBytes, storeName);
      return (int)partitionCount;
    } else {
      // Active version exists, use the partition count calculated before.
      logger.info("Assign partition count: {}, which come from previous version, for store: {}",
          previousPartitionCount, storeName);
      return previousPartitionCount;
    }
  }

  private static void checkAmplificationFactor(int amplificationFactor) {
    if (amplificationFactor < 1) {
      throw new VeniceException(
          String.format("Invalid amplification factor %d. Amplification factor must be >= 1.", amplificationFactor));
    }
  }

  public static IntList getSubPartitions(Collection<Integer> userPartitions, int amplificationFactor) {
    checkAmplificationFactor(amplificationFactor);
    IntList subPartitions = new IntArrayList();
    for (int userPartition : userPartitions) {
      int subPartition = userPartition * amplificationFactor;
      for (int i = 0; i < amplificationFactor; ++i, ++subPartition) {
        subPartitions.add(subPartition);
      }
    }
    return subPartitions;
  }

  /**
   * @param topic the consumed topic which the record is from
   * @param partition the partition in the consumed topic
   * @param amplificationFactor
   * @return leaderSubPartition if is consuming from a Real-time topic, else return partition itself
   */
  public static int getSubPartition(String topic, int partition, int amplificationFactor) {
    return Version.isRealTimeTopic(topic) ?
        getLeaderSubPartition(partition, amplificationFactor) : partition;
  }

  public static IntList getSubPartitions(int userPartition, int amplificationFactor) {
    checkAmplificationFactor(amplificationFactor);
    IntList subPartitions = new IntArrayList(amplificationFactor);
    int subPartition = userPartition * amplificationFactor;
    for (int i = 0; i < amplificationFactor; ++i, ++subPartition) {
      subPartitions.add(subPartition);
    }
    return subPartitions;
  }

  public static IntList getUserPartitions(Collection<Integer> subPartitions, int amplificationFactor) {
    IntList userPartitions = new IntArrayList(subPartitions.size());
    for (Integer subPartition : subPartitions) {
      userPartitions.add(getUserPartition(subPartition, amplificationFactor));
    }
    return userPartitions;
  }

  public static int getUserPartition(int subPartition, int amplificationFactor) {
    checkAmplificationFactor(amplificationFactor);
    return subPartition / amplificationFactor;
  }

  public static int getLeaderSubPartition(int userPartition, int amplificationFactor) {
    checkAmplificationFactor(amplificationFactor);
    return userPartition * amplificationFactor;
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
    return getVenicePartitioner(config.getPartitionerClass(), 1, new VeniceProperties(params));
  }

  public static VenicePartitioner getVenicePartitioner(
      String partitionerClass,
      int amplificationFactor,
      VeniceProperties params) {
    return getVenicePartitioner(partitionerClass, amplificationFactor, params, null);
  }

  public static VenicePartitioner getVenicePartitioner(
      String partitionerClass,
      int amplificationFactor,
      VeniceProperties params,
      Schema keySchema) {
    VenicePartitioner partitioner = ReflectUtils.callConstructor(ReflectUtils.loadClass(partitionerClass),
        new Class<?>[]{VeniceProperties.class, Schema.class}, new Object[]{params, keySchema});
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

  public static int getAmplificationFactor(ReadOnlyStoreRepository readOnlyStoreRepository, String topicName) {
    // any exception throw during creation of AbstractParticipantModel could result in severe spamming log in Helix
    // surround the block with try-catch to be safe
    try {
      String storeName = Version.parseStoreFromKafkaTopicName(topicName);
      int versionNumber = Version.parseVersionFromKafkaTopicName(topicName);
      return getAmplificationFactor(readOnlyStoreRepository, storeName, versionNumber);
    } catch (Exception e) {
      return 1;
    }
  }

  public static int getAmplificationFactor(ReadOnlyStoreRepository readOnlyStoreRepository, String storeName, int versionNumber) {
    int amplifcationFactor = 1;
    if (readOnlyStoreRepository == null) {
      return amplifcationFactor;
    }
    try {
      Optional<Version> version = readOnlyStoreRepository.getStore(storeName).getVersion(versionNumber);
      if (version.isPresent()) {
        amplifcationFactor = version.get().getPartitionerConfig().getAmplificationFactor();
      } else {
        logger.warn("Version " + versionNumber + " does not exist.");
        amplifcationFactor = readOnlyStoreRepository.getStore(storeName).getPartitionerConfig().getAmplificationFactor();
      }
    } catch (Exception e) {
      logger.warn("Failed to fetch amplificationFactor from for store " + storeName + ". Using default value 1.");
      amplifcationFactor = 1;
    }
    return amplifcationFactor;
  }
}
