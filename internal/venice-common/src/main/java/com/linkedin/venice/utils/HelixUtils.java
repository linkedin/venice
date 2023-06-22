package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.ZkDataAccessException;
import com.linkedin.venice.helix.HelixPartitionState;
import com.linkedin.venice.helix.SafeHelixDataAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.meta.Instance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Helper functions for Helix.
 */
public class HelixUtils {
  private static final Logger LOGGER = LogManager.getLogger(HelixUtils.class);

  /**
   * Retry 3 times for each helix operation in case of getting the error by default.
   */
  public static final int DEFAULT_HELIX_OP_RETRY_COUNT = 3;

  /**
   * The constraint that helix would apply on CRUSH alg. Based on this constraint, Helix would NOT allocate replicas in
   * same partition to same instance. If we need rack aware ability in the future, we could add rack constraint as
   * well.
   */
  public static final String TOPOLOGY_CONSTRAINT = "instance";

  public static String getHelixClusterZkPath(String clusterName) {
    return "/" + clusterName;
  }

  private static final Character SEPARATOR = '_';

  public static int getPartitionId(String helixPartitionName) {
    int lastUnderscoreIdx = helixPartitionName.lastIndexOf(SEPARATOR);
    if (lastUnderscoreIdx == -1) {
      throw new IllegalArgumentException("Incorrect Helix Partition Name " + helixPartitionName);
    }
    return Integer.parseInt(helixPartitionName.substring(lastUnderscoreIdx + 1));
  }

  public static String getPartitionName(String resourceName, int partitionId) {
    return resourceName + SEPARATOR + partitionId;
  }

  public static String getResourceName(String helixPartitionName) {
    int lastUnderscoreIdx = helixPartitionName.lastIndexOf(SEPARATOR);
    if (lastUnderscoreIdx == -1) {
      throw new IllegalArgumentException("Incorrect Helix Partition Name " + helixPartitionName);
    }
    String resourceName = helixPartitionName.substring(0, lastUnderscoreIdx);
    if (resourceName.isEmpty()) {
      throw new IllegalArgumentException(
          "Could not determine resource name from Helix Partition Id " + helixPartitionName);
    }
    return resourceName;
  }

  /**
   * @param helixInstanceName of form host_port.
   * @return Instance object with correct host and port.
   */
  public static Instance getInstanceFromHelixInstanceName(String helixInstanceName) {
    String hostname = Utils.parseHostFromHelixNodeIdentifier(helixInstanceName);
    int port = Utils.parsePortFromHelixNodeIdentifier(helixInstanceName);
    return new Instance(helixInstanceName, hostname, port);
  }

  /**
   * @param path parent path
   * @return a list of objects that are under parent path. It will return null if parent path is not existing
   */
  public static <T> List<T> getChildren(
      ZkBaseDataAccessor<T> dataAccessor,
      String path,
      int retryCount,
      long retryInterval) {
    int attempt = 1;
    while (attempt <= retryCount) {
      attempt++;
      List<String> childrenNames = dataAccessor.getChildNames(path, AccessOption.PERSISTENT);
      int expectedCount = 0;
      if (childrenNames == null) {
        LOGGER.warn("Get child names for path: {} return null.", path);
      } else {
        expectedCount = childrenNames.size();
      }
      List<T> children = dataAccessor.getChildren(path, null, AccessOption.PERSISTENT);
      if (children.size() != expectedCount) {
        // Data is inconsistent
        if (attempt < retryCount) {
          LOGGER.info(
              "dataAccessor.getChildNames() did not return the expected number of elements from path: {}\nExpected: {}, but got {}. Attempt:{}/{}, will sleep {} and retry.",
              path,
              expectedCount,
              children.size(),
              attempt,
              retryCount,
              retryInterval);
          Utils.sleep(retryInterval);
        }
      } else {
        return children;
      }
    }
    throw new VeniceException("Got partial children from zk after retry " + attempt + " times.");
  }

  public static <T> List<String> listPathContents(ZkBaseDataAccessor<T> dataAccessor, String path) {
    try {
      List<String> paths = dataAccessor.getChildNames(path, AccessOption.PERSISTENT);
      return paths == null ? Collections.emptyList() : paths;
    } catch (Exception e) {
      LOGGER.error("Error when listing contents in path: {}", path, e);
      throw e;
    }
  }

  public static <T> void create(ZkBaseDataAccessor<T> dataAccessor, String path, T data) {
    create(dataAccessor, path, data, DEFAULT_HELIX_OP_RETRY_COUNT);
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

  public static <T> void update(ZkBaseDataAccessor<T> dataAccessor, String path, T data) {
    update(dataAccessor, path, data, DEFAULT_HELIX_OP_RETRY_COUNT);
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

  public static <T> void updateChildren(ZkBaseDataAccessor<T> dataAccessor, List<String> paths, List<T> data) {
    updateChildren(dataAccessor, paths, data, DEFAULT_HELIX_OP_RETRY_COUNT);
  }

  // TODO there is not atomic operations to update multiple node to ZK. We should ask Helix library to rollback if it's
  // only partial successful.
  public static <T> void updateChildren(
      ZkBaseDataAccessor<T> dataAccessor,
      List<String> paths,
      List<T> data,
      int retryCount) {
    int retry = 0;
    while (retry < retryCount) {
      boolean[] results = dataAccessor.setChildren(paths, data, AccessOption.PERSISTENT);
      boolean isAllSuccessful = true;
      for (Boolean r: results) {
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
        throw new ZkDataAccessException(
            paths.get(0).substring(0, paths.get(0).lastIndexOf('/')),
            "update children",
            retryCount);
      }
    }
  }

  public static <T> void remove(ZkBaseDataAccessor<T> dataAccessor, String path) {
    remove(dataAccessor, path, DEFAULT_HELIX_OP_RETRY_COUNT);
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

  public static <T> void compareAndUpdate(ZkBaseDataAccessor<T> dataAccessor, String path, DataUpdater<T> dataUpdater) {
    compareAndUpdate(dataAccessor, path, DEFAULT_HELIX_OP_RETRY_COUNT, dataUpdater);
  }

  public static <T> void compareAndUpdate(
      ZkBaseDataAccessor<T> dataAccessor,
      String path,
      int retryCount,
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
  public static void connectHelixManager(SafeHelixManager manager, int maxRetries, int sleepTimeSeconds) {
    int attempt = 1;
    boolean isSuccess = false;
    while (!isSuccess) {
      try {
        manager.connect();
        isSuccess = true;
      } catch (Exception e) {
        if (attempt <= maxRetries) {
          LOGGER.warn(
              "Failed to connect {} on attempt {}/{}. Will retry in {} seconds.",
              manager.toString(),
              attempt,
              maxRetries,
              sleepTimeSeconds);
          attempt++;
          Utils.sleep(TimeUnit.SECONDS.toMillis(sleepTimeSeconds));
        } else {
          throw new VeniceException(
              "Error connecting to Helix Manager for Cluster '" + manager.getClusterName() + "' after " + maxRetries
                  + " attempts.",
              e);
        }
      }
    }
  }

  /**
   * Before this participant/spectator joining the Helix cluster, we need to make sure the cluster has been setup by
   * controller.
   * Otherwise, the following operations issued by participant/spectator would fail.
   */
  public static void checkClusterSetup(HelixAdmin admin, String cluster, int maxRetry, int retryIntervalSec) {
    int attempt = 1;
    while (true) {
      if (admin.getClusters().contains(cluster)) {
        // Cluster is ready.
        break;
      } else {
        if (attempt <= maxRetry) {
          LOGGER.warn(
              "Cluster has not been initialized by controller. Attempt: {}. Will retry in {} seconds.",
              attempt,
              retryIntervalSec);
          attempt++;
          Utils.sleep(TimeUnit.SECONDS.toMillis(retryIntervalSec));
        } else {
          throw new VeniceException("Cluster has not been initialized by controller after attempted: " + attempt);
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

      String instanceDomainKey = InstanceConfig.InstanceConfigProperty.DOMAIN.name();

      Map<String, String> currentDomainValue =
          admin.getConfig(instanceScope, Collections.singletonList(instanceDomainKey));
      if (currentDomainValue == null || !currentDomainValue.containsKey(instanceDomainKey)) {
        Map<String, String> instanceProperties = new HashMap<>();
        instanceProperties.put(instanceDomainKey, TOPOLOGY_CONSTRAINT + "=" + instanceId);
        admin.setConfig(instanceScope, instanceProperties);

      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  public static boolean isLiveInstance(String clusterName, String instanceId, SafeHelixManager manager) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    SafeHelixDataAccessor accessor = manager.getHelixDataAccessor();
    // Get session id at first then get current states of given instance and give session.
    LiveInstance instance = accessor.getProperty(keyBuilder.liveInstance(instanceId));
    if (instance == null) {
      LOGGER.info("Instance: {} is not a live instance", instanceId);
      return false;
    } else {
      return true;
    }
  }

  /**
   * If a cluster is in maintenance mode it returns all the signals associated with it regarding the reason
   * and other details for creating the maintenance mode.
   * @param clusterName
   * @param manager
   * @return the maintenance mode signal if the cluster is in maintenance mode, otherwise returns null.
   */
  public static MaintenanceSignal getClusterMaintenanceSignal(String clusterName, SafeHelixManager manager) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    SafeHelixDataAccessor accessor = manager.getHelixDataAccessor();
    return accessor.getProperty(keyBuilder.maintenance());
  }

  /**
   * This method tells helix what properties to aggregate for customized view.
   * How customized view works in steps:
   * 1. Venice SN reports its instance level states e.g. ingestion progress / hybrid store quota for each partition.
   * 2. Helix aggregates the per instance level states to store version level. This method tells Helix what states to aggregate.
   * So far we need to aggregate two properties {@link HelixPartitionState#OFFLINE_PUSH} and {@link HelixPartitionState#HYBRID_STORE_QUOTA}.
   * 3. Other components e.g. Venice routers listen to the store version level states to serve traffic.
   * @param admin
   * @param clusterName
   */
  public static void setupCustomizedStateConfig(HelixAdmin admin, String clusterName) {
    CustomizedStateConfig.Builder customizedStateConfigBuilder = new CustomizedStateConfig.Builder();
    List<String> aggregationEnabledTypes = new ArrayList<>(2);
    aggregationEnabledTypes.add(HelixPartitionState.OFFLINE_PUSH.name());
    aggregationEnabledTypes.add(HelixPartitionState.HYBRID_STORE_QUOTA.name());
    customizedStateConfigBuilder.setAggregationEnabledTypes(aggregationEnabledTypes);
    CustomizedStateConfig customizedStateConfig = customizedStateConfigBuilder.build();
    admin.addCustomizedStateConfig(clusterName, customizedStateConfig);
  }

  /**
   * Convert a replica's instance ID to its URL. An example is "localhost_port" to "https://localhost:port".
   * @param instanceId
   * @return replica URL
   */
  public static String instanceIdToUrl(String instanceId) {
    return "https://" + instanceId.replace("_", ":");
  }
}
