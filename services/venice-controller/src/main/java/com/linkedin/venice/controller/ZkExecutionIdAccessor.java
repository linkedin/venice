package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.consumer.StringToLongMapJSONSerializer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.ZkDataAccessException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.meta.SimpleStringSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ZkExecutionIdAccessor implements ExecutionIdAccessor {
  private static final String EXECUTION_ID_DIR = "/executionids";
  private static final int ZK_RETRY_COUNT = 3;
  private static final Logger LOGGER = LogManager.getLogger(ZkExecutionIdAccessor.class);
  private final ZkClient zkclient;
  private final ZkBaseDataAccessor<Map<String, Long>> zkMapAccessor;
  private final ZkBaseDataAccessor<String> executionIdAccessor;

  public ZkExecutionIdAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    this.zkclient = zkClient;
    this.zkMapAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.executionIdAccessor = new ZkBaseDataAccessor<>(zkClient);
    adapterSerializer.registerSerializer(
        getLastSucceededExecutionIdPath(PathResourceRegistry.WILDCARD_MATCH_ANY),
        new SimpleStringSerializer());
    adapterSerializer.registerSerializer(
        getLastSucceededExecutionIdMapPath(PathResourceRegistry.WILDCARD_MATCH_ANY),
        new StringToLongMapJSONSerializer());
    adapterSerializer.registerSerializer(
        getLastGeneratedExecutionIdPath(PathResourceRegistry.WILDCARD_MATCH_ANY),
        new SimpleStringSerializer());
    zkClient.setZkSerializer(adapterSerializer);
  }

  /**
   * @see ExecutionIdAccessor#getLastSucceededExecutionId(String)
   */
  @Override
  public Long getLastSucceededExecutionId(String clusterName) {
    String path = getLastSucceededExecutionIdPath(clusterName);
    return getExecutionIdFromZk(path);
  }

  /**
   * @see ExecutionIdAccessor#updateLastSucceededExecutionId(String, Long)
   */
  @Override
  public void updateLastSucceededExecutionId(String clusterName, Long lastSucceedExecutionId) {
    String path = getLastSucceededExecutionIdPath(clusterName);
    updateExecutionToZk(path, lastSucceedExecutionId);
  }

  /**
   * @see ExecutionIdAccessor#getLastSucceededExecutionIdMap(String)
   */
  @Override
  public synchronized Map<String, Long> getLastSucceededExecutionIdMap(String clusterName) {
    String path = getLastSucceededExecutionIdMapPath(clusterName);
    return getExecutionIdMapFromZk(path);
  }

  /**
   * @see ExecutionIdAccessor#updateLastSucceededExecutionIdMap(String, String, Long)
   */
  @Override
  public synchronized void updateLastSucceededExecutionIdMap(
      String clusterName,
      String storeName,
      Long lastSucceededExecutionId) {
    String path = getLastSucceededExecutionIdMapPath(clusterName);
    updateExecutionIdMapToZk(path, storeName, lastSucceededExecutionId);
  }

  /**
   * @see ExecutionIdAccessor#getLastGeneratedExecutionId(String)
   */
  @Override
  public Long getLastGeneratedExecutionId(String clusterName) {
    String path = getLastGeneratedExecutionIdPath(clusterName);
    return getExecutionIdFromZk(path);
  }

  /**
   * @see ExecutionIdAccessor#updateLastGeneratedExecutionId(String, Long)
   */
  @Override
  public void updateLastGeneratedExecutionId(String clusterName, Long lastGeneratedExecutionId) {
    String path = getLastGeneratedExecutionIdPath(clusterName);
    updateExecutionToZk(path, lastGeneratedExecutionId);
  }

  /**
   * Using AtomicLong here only as a workaround to get next execution id from
   * {@link HelixUtils#compareAndUpdate(ZkBaseDataAccessor, String, int, DataUpdater)}
   *
   * @throws ZkDataAccessException will be thrown if it fails to update the data
   */
  @Override
  public Long incrementAndGetExecutionId(String clusterName) {
    AtomicLong executionId = new AtomicLong();
    HelixUtils.compareAndUpdate(
        executionIdAccessor,
        getLastGeneratedExecutionIdPath(clusterName),
        ZK_RETRY_COUNT,
        currentData -> {
          long nextId;
          if (currentData == null) {
            // the id hasn't been initialized yet
            nextId = 0;
          } else {
            nextId = Long.parseLong(currentData) + 1;
          }
          executionId.set(nextId);
          return String.valueOf(nextId);
        });

    return executionId.get();
  }

  private Map<String, Long> getExecutionIdMapFromZk(String path) {
    int retry = ZK_RETRY_COUNT;
    while (retry > 0) {
      try {
        Map<String, Long> executionIdMap = zkclient.readData(path, true);
        if (executionIdMap == null) {
          executionIdMap = new HashMap<>();
        }
        return executionIdMap;
      } catch (Exception e) {
        LOGGER.warn("Could not get the execution id map from ZK with: {}. Will retry the query.", path, e);
        retry--;
      }
    }
    throw new VeniceException(
        "After retry " + ZK_RETRY_COUNT + " times, could not get the execution id map from ZK with: " + path);
  }

  private void updateExecutionIdMapToZk(String path, String storeName, Long lastSucceededExecutionId) {
    HelixUtils.compareAndUpdate(zkMapAccessor, path, ZK_RETRY_COUNT, executionIdMap -> {
      if (executionIdMap == null) {
        executionIdMap = new HashMap<>();
      }
      executionIdMap.put(storeName, lastSucceededExecutionId);
      return executionIdMap;
    });
  }

  private Long getExecutionIdFromZk(String path) {
    int retry = ZK_RETRY_COUNT;
    while (retry > 0) {
      try {
        String lastSucceedExecutionId = executionIdAccessor.get(path, null, AccessOption.PERSISTENT);
        if (lastSucceedExecutionId == null) {
          return -1L;
        }
        return Long.valueOf(lastSucceedExecutionId);
      } catch (Exception e) {
        LOGGER.warn("Could not get the execution id from ZK from: {}. Will retry the query.", path, e);
        retry--;
      }
    }
    throw new VeniceException(
        "After retry " + ZK_RETRY_COUNT + " times, could not get the execution id from ZK from: " + path);
  }

  private void updateExecutionToZk(String path, Long executionId) {
    int retry = ZK_RETRY_COUNT;
    while (retry > 1) {
      try {
        zkclient.writeData(path, executionId.toString());
        return;
      } catch (ZkNoNodeException e) {
        zkclient.createPersistent(path, true);
        zkclient.writeData(path, executionId.toString());
      } catch (Exception e) {
        e.printStackTrace();
        LOGGER.warn("Could not update the execution id to ZK in: {}. Will retry the query.", path, e);
        retry--;
      }
    }
    throw new VeniceException(
        "After retry " + ZK_RETRY_COUNT + " times, could not update the execution id to ZK in: " + path);
  }

  private static String getLastSucceededExecutionIdPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + EXECUTION_ID_DIR + "/lastSucceedExecutionId";
  }

  private static String getLastSucceededExecutionIdMapPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + EXECUTION_ID_DIR + "/succeededPerStore";
  }

  private static String getLastGeneratedExecutionIdPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + EXECUTION_ID_DIR + "/lastGeneratedExecutionId";
  }
}
