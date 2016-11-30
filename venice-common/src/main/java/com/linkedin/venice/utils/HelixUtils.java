package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.ZkDataAccessException;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;


/**
 * Helper functions for Helix.
 */
public class HelixUtils {

  private static final Logger logger = Logger.getLogger(HelixUtils.class);

  /**
   * The constraint that helix would apply on CRUSH alg. Based on this constraint, Helix would NOT allocate replicas in
   * same partition to same instance. If we need rack aware ability in the future, we could add rack constraint as
   * well.
   */
  public static final String TOPOLOGY_CONSTRAINT = "instance";

  public static String getHelixClusterZkPath(String clusterName){
    return "/"+clusterName;
  }
  /**
   * Converts the Venice Server Node Id to Helix Participant name.
   */
  public static String convertNodeIdToHelixParticipantName(int nodeId) {
    return "Participant_" + String.valueOf(nodeId);
  }

  private final static Character SEPARATOR = '_';
  public static int getPartitionId(String helixPartitionName) {
    int lastUnderscoreIdx = helixPartitionName.lastIndexOf(SEPARATOR);
    if(lastUnderscoreIdx == -1) {
      throw new IllegalArgumentException(" Incorrect Helix Partition Name " + helixPartitionName);
    }
    return Integer.valueOf(helixPartitionName.substring(lastUnderscoreIdx+1));
  }

  public static String getPartitionName(String resourceName, int partitionId) {
    return resourceName + SEPARATOR + partitionId;
  }

  public static String getResourceName(String helixPartitionName){
    int lastUnderscoreIdx = helixPartitionName.lastIndexOf(SEPARATOR);
    if(lastUnderscoreIdx == -1) {
      throw new IllegalArgumentException(" Incorrect Helix Partition Name " + helixPartitionName);
    }
    String resourceName = helixPartitionName.substring(0, lastUnderscoreIdx);
    if(resourceName == null || resourceName.length() == 0) {
      throw new IllegalArgumentException(" Could not determine resource name from Helix Partition Id " + helixPartitionName);
    }
    return resourceName;
  }

  public static <T> void create(ZkBaseDataAccessor<T> dataAccessor, String path, T data, int retryCount) {
    int retry = 0;
    while (retry < retryCount) {
      if (dataAccessor.create(path, data, AccessOption.PERSISTENT)) {
        return;
      }
      retry++;
    }
    throw new ZkDataAccessException(path, "create", retryCount);
  }

  public static <T> void update(ZkBaseDataAccessor<T> dataAccessor, String path, T data, int retryCount) {
    int retry = 0;
    while (retry < retryCount) {
      if (dataAccessor.set(path, data, AccessOption.PERSISTENT)) {
        return;
      }
      retry++;
    }
    throw new ZkDataAccessException(path, "set", retryCount);
  }

  //TODO there is not atomic operations to update multiple node to ZK. We should ask Helix library to rollback if it's only partial successful.
  public static <T> void updateChildren(ZkBaseDataAccessor<T> dataAccessor, List<String> pathes, List<T> data,
      int retryCount) {
    int retry = 0;
    while (retry < retryCount) {
      boolean[] results = dataAccessor.setChildren(pathes, data, AccessOption.PERSISTENT);
      boolean isAllSuccessful = true;
      for (Boolean r : results) {
        if (!r) {
          isAllSuccessful = false;
          break;
        }
      }
      if (isAllSuccessful) {
        return;
      }
      retry++;
      if (retry == retryCount) {
        throw new ZkDataAccessException(pathes.get(0).substring(0, pathes.get(0).lastIndexOf('/')), "update children",
            retryCount);
      }
    }
  }

  public static <T> void remove(ZkBaseDataAccessor<T> dataAccessor, String path, int retryCount) {
    int retry = 0;
    while (retry < retryCount) {
      if (dataAccessor.remove(path, AccessOption.PERSISTENT)) {
        return;
      }
      retry++;
    }
    throw new ZkDataAccessException(path, "remove", retryCount);
 }

  public static <T> void compareAndUpdate(ZkBaseDataAccessor<T> dataAccessor, String path, int retryCount,
      DataUpdater<T> dataUpdater) {
    int retry = 0;
    while (retry < retryCount) {
      if (dataAccessor.update(path, dataUpdater, AccessOption.PERSISTENT)) {
        return;
      }
      retry++;
    }
    throw new ZkDataAccessException(path, "compare and update", retryCount);
  }

  /**
   * Try to connect Helix Manger. If failed, waits for certain time and retry. If Helix Manager can not be
   * connected after certain number of retry, throws exception. This method is most likely being used asynchronously since
   * it is going to block and wait if connection fails.
   * @param manager HelixManager instance
   * @param maxRetries retry time
   * @param sleepTimeSeconds time in second that it blocks until next retry.
   * @exception VeniceException if connection keeps failing after certain number of retry
   */
  public static void connectHelixManager(HelixManager manager, int maxRetries, int sleepTimeSeconds) {
    int attempt = 1;
    boolean isSuccess = false;
    while (!isSuccess) {
      try {
        manager.connect();
        isSuccess = true;
      } catch (Exception e) {
        if (attempt <= maxRetries) {
          logger.warn("failed to connect " + manager.toString() + " on attempt " + attempt + "/" + maxRetries +
              ". Will retry in " + sleepTimeSeconds + " seconds.");
          attempt++;
          Utils.sleep(sleepTimeSeconds * 1000);
        } else {
          throw new VeniceException("Error connecting to Helix Manager for Cluster '" +
              manager.getClusterName() + "' after " + maxRetries + " attempts.", e);
        }
      }
    }
  }

  public static void setupInstanceConfig(String clusterName, String instanceId, String zkAddress) {
    HelixConfigScope instanceScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT).forCluster(clusterName)
            .forParticipant(instanceId)
            .build();
    HelixAdmin admin = null;
    try {
      admin = new ZKHelixAdmin(zkAddress);
      Map<String, String> instanceProperties = new HashMap<>();
      instanceProperties.put(InstanceConfig.InstanceConfigProperty.DOMAIN.name(),
          TOPOLOGY_CONSTRAINT + "=" + instanceId);
      admin.setConfig(instanceScope, instanceProperties);
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  public static boolean isLiveInstance(String clusterName, String instanceId, HelixManager manager) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    // Get session id at first then get current states of given instance and give session.
    LiveInstance instance = accessor.getProperty(keyBuilder.liveInstance(instanceId));
    if (instance == null) {
      logger.info("Instance:" + instanceId + " is not a live instance");
      return false;
    } else {
      return true;
    }
  }
}
