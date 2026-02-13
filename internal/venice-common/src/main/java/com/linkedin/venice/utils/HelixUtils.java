package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.ZkDataAccessException;
import com.linkedin.venice.helix.HelixPartitionState;
import com.linkedin.venice.helix.SafeHelixDataAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.meta.Instance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyKey;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Helper functions for Helix.
 */
public final class HelixUtils {
  private static final Logger LOGGER = LogManager.getLogger(HelixUtils.class);

  private HelixUtils() {
  }

  /**
   * Retry 9 times for each helix operation in case of getting the error by default.
   */
  public static final int DEFAULT_HELIX_OP_RETRY_COUNT = 9;

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
   * @param dataAccessor ZK data accessor
   * @param path parent path
   * @param maxAttempts maximum number of retry attempts
   * @return a list of objects that are under parent path. It will return an empty list of objects if parent path is
   * not existing
   * @throws VeniceException if data inconsistency persists after all retry attempts
   */
  public static <T> List<T> getChildren(ZkBaseDataAccessor<T> dataAccessor, String path, int maxAttempts) {
    int attempt = 0;
    while (attempt < maxAttempts) {
      // Get expected count from child names
      List<String> childrenNames = dataAccessor.getChildNames(path, AccessOption.PERSISTENT);
      int expectedCount = 0;
      if (childrenNames == null) {
        LOGGER.warn("getChildNames for path: {} returned null.", path);
      } else {
        expectedCount = childrenNames.size();
      }

      // Get actual children
      List<T> children = dataAccessor.getChildren(path, null, AccessOption.PERSISTENT);

      // Data is consistent
      if (children.size() == expectedCount) {
        return children;
      }

      // Data is inconsistent, retry
      attempt++;
      LOGGER.error(
          "dataAccessor.getChildNames() and dataAccessor.getChildren() did not return the same number "
              + "of elements from path: {}\nExpected: {}, but got {}. Attempt:{}/{}.",
          path,
          expectedCount,
          children.size(),
          attempt,
          maxAttempts);
      Utils.sleep(TimeUnit.SECONDS.toMillis((long) Math.pow(2, attempt)));
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

  public static <T> void create(ZkBaseDataAccessor<T> dataAccessor, String path, T data, int maxAttempts) {
    int attempt = 0;
    while (attempt < maxAttempts) {
      if (dataAccessor.create(path, data, AccessOption.PERSISTENT)) {
        return;
      }
      attempt++;
      handleFailedHelixOperation(path, "create", attempt, maxAttempts);
    }
  }

  public static <T> void update(ZkBaseDataAccessor<T> dataAccessor, String path, T data) {
    update(dataAccessor, path, data, DEFAULT_HELIX_OP_RETRY_COUNT);
  }

  public static <T> void update(ZkBaseDataAccessor<T> dataAccessor, String path, T data, int maxAttempts) {
    int attempt = 0;
    while (attempt < maxAttempts) {
      if (dataAccessor.set(path, data, AccessOption.PERSISTENT)) {
        return;
      }
      attempt++;
      handleFailedHelixOperation(path, "update", attempt, maxAttempts);
    }
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
      int maxAttempts) {
    int attempt = 0;
    while (attempt < maxAttempts) {
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
      attempt++;
      String path = paths.get(0).substring(0, paths.get(0).lastIndexOf('/'));
      handleFailedHelixOperation(path, "updateChildren", attempt, maxAttempts);
    }
  }

  public static <T> boolean exists(ZkBaseDataAccessor<T> dataAccessor, String path) {
    return dataAccessor.exists(path, AccessOption.PERSISTENT);
  }

  public static <T> void remove(ZkBaseDataAccessor<T> dataAccessor, String path) {
    remove(dataAccessor, path, DEFAULT_HELIX_OP_RETRY_COUNT);
  }

  public static <T> void remove(ZkBaseDataAccessor<T> dataAccessor, String path, int maxAttempts) {
    int attempt = 0;
    while (attempt < maxAttempts) {
      if (dataAccessor.remove(path, AccessOption.PERSISTENT)) {
        return;
      }
      attempt++;
      handleFailedHelixOperation(path, "remove", attempt, maxAttempts);
    }
  }

  public static <T> void compareAndUpdate(ZkBaseDataAccessor<T> dataAccessor, String path, DataUpdater<T> dataUpdater) {
    compareAndUpdate(dataAccessor, path, DEFAULT_HELIX_OP_RETRY_COUNT, dataUpdater);
  }

  public static <T> void compareAndUpdate(
      ZkBaseDataAccessor<T> dataAccessor,
      String path,
      int maxAttempts,
      DataUpdater<T> dataUpdater) {
    int attempt = 0;
    while (attempt < maxAttempts) {
      if (dataAccessor.update(path, dataUpdater, AccessOption.PERSISTENT)) {
        return;
      }
      attempt++;
      handleFailedHelixOperation(path, "compareAndUpdate", attempt, maxAttempts);
    }
  }

  /**
   * Helper method for logging Helix operation failures, sleeping for retry, and throwing exceptions
   *
   * @param path ZooKeeper path that was being operated on
   * @param helixOperation name of the Helix operation that failed
   * @param attempt current attempt number
   * @param maxAttempts maximum number of retry attempts
   * @throws ZkDataAccessException if max retry attempts have been reached
   */
  private static void handleFailedHelixOperation(String path, String helixOperation, int attempt, int maxAttempts) {
    if (attempt < maxAttempts) {
      long retryIntervalSec = (long) Math.pow(2, attempt);
      if (path.isEmpty()) {
        LOGGER.error(
            "{} failed on attempt {}/{}. Will retry in {} seconds.",
            helixOperation,
            attempt,
            maxAttempts,
            retryIntervalSec);
      } else {
        LOGGER.error(
            "{} failed with path: {} on attempt {}/{}. Will retry in {} seconds.",
            helixOperation,
            path,
            attempt,
            maxAttempts,
            retryIntervalSec);
      }
      Utils.sleep(TimeUnit.SECONDS.toMillis(retryIntervalSec));
    } else {
      throw new ZkDataAccessException(path, helixOperation, maxAttempts);
    }
  }

  /**
   * Try to connect Helix Manager. If failed, waits for certain time and retry. If Helix Manager can not be
   * connected after certain number of retry, throws exception. This method is most likely being used asynchronously since
   * it is going to block and wait if connection fails.
   *
   * @param manager HelixManager instance
   * @param maxAttempts maximum number of attempts to retry on failure
   * @throws VeniceException if connection keeps failing after certain number of retry
   */
  public static void connectHelixManager(SafeHelixManager manager, int maxAttempts) {
    int attempt = 0;
    while (attempt < maxAttempts) {
      try {
        manager.connect();
        return; // Success, exit immediately
      } catch (Exception e) {
        attempt++;
        if (attempt < maxAttempts) {
          long retryIntervalSec = (long) Math.pow(2, attempt);
          LOGGER.warn(
              "Failed to connect {} on attempt {}/{}. Will retry in {} seconds.",
              manager.toString(),
              attempt,
              maxAttempts,
              retryIntervalSec);
          Utils.sleep(TimeUnit.SECONDS.toMillis(retryIntervalSec));
        } else {
          throw new VeniceException(
              "Error connecting to Helix Manager for Cluster '" + manager.getClusterName() + "' after " + maxAttempts
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
  public static void checkClusterSetup(HelixAdmin admin, String cluster, int maxAttempts) {
    int attempt = 0;
    while (attempt < maxAttempts) {
      if (admin.getClusters().contains(cluster)) {
        // Success, exit immediately
        return;
      } else {
        attempt++;
        if (attempt < maxAttempts) {
          long retryIntervalSec = (long) Math.pow(2, attempt);
          LOGGER.warn(
              "Cluster has not been initialized by controller. Attempt: {}. Will retry in {} seconds.",
              attempt,
              retryIntervalSec);
          Utils.sleep(TimeUnit.SECONDS.toMillis(retryIntervalSec));
        } else {
          throw new VeniceException("Cluster has not been initialized by controller after attempted: " + attempt);
        }
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

  public static IdealState getIdealState(String clusterName, String resourceName, SafeHelixManager manager) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    SafeHelixDataAccessor accessor = manager.getHelixDataAccessor();
    return accessor.getProperty(keyBuilder.idealStates(resourceName));
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
    return instanceIdToUrl(instanceId, true);
  }

  /**
   * Convert a replica's instance ID to its URL with a configurable scheme.
   * @param instanceId the instance ID in the format "host_port"
   * @param https if true, use "https://"; otherwise, use "http://"
   * @return replica URL
   */
  public static String instanceIdToUrl(String instanceId, boolean https) {
    String scheme = https ? "https://" : "http://";
    return scheme + instanceId.replace("_", ":");
  }

  public static CloudConfig getCloudConfig(
      CloudProvider cloudProvider,
      String cloudId,
      List<String> cloudInfoSources,
      String cloudInfoProcessorPackage,
      String cloudInfoProcessorName) {
    CloudConfig.Builder cloudConfigBuilder =
        new CloudConfig.Builder().setCloudEnabled(true).setCloudProvider(cloudProvider);

    if (!StringUtils.isEmpty(cloudId)) {
      cloudConfigBuilder.setCloudID(cloudId);
    }

    if (cloudInfoSources != null && !cloudInfoSources.isEmpty()) {
      cloudConfigBuilder.setCloudInfoSources(cloudInfoSources);
    }

    if (!StringUtils.isEmpty(cloudInfoProcessorPackage)) {
      cloudConfigBuilder.setCloudInfoProcessorPackageName(cloudInfoProcessorPackage);
    }

    if (!StringUtils.isEmpty(cloudInfoProcessorName)) {
      cloudConfigBuilder.setCloudInfoProcessorName(cloudInfoProcessorName);
    }

    return cloudConfigBuilder.build();
  }
}
