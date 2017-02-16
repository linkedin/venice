package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.meta.SimpleStringSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


public class ZkExecutionIdAccessor implements ExecutionIdAccessor {
  public static final String EXECUTION_ID_DIR = "executionids";
  private static final int ZK_RETRY_COUNT = 3;
  private static final Logger logger = Logger.getLogger(ZkExecutionIdAccessor.class);
  private final ZkClient zkclient;

  public ZkExecutionIdAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    this.zkclient = zkClient;
    adapterSerializer.registerSerializer(getLastSucceedExecutionIdPath(PathResourceRegistry.WILDCARD_MATCH_ANY),
        new SimpleStringSerializer());
    adapterSerializer.registerSerializer(getLastGeneratedExecutionIdPath(PathResourceRegistry.WILDCARD_MATCH_ANY),
        new SimpleStringSerializer());
    zkClient.setZkSerializer(adapterSerializer);
  }

  @Override
  public Long getLastSucceedExecutionId(String clusterName) {
    String path = getLastSucceedExecutionIdPath(clusterName);
    return getExecutionIdFromZk(path);
  }

  @Override
  public void updateLastSucceedExecutionId(String clusterName, Long lastSucceedExecutionId) {
    String path = getLastSucceedExecutionIdPath(clusterName);
    updateExecutionToZk(path, lastSucceedExecutionId);
  }

  @Override
  public Long getLastGeneratedExecutionId(String clusterName) {
    String path = getLastGeneratedExecutionIdPath(clusterName);
    return getExecutionIdFromZk(path);
  }

  @Override
  public void updateLastGeneratedExecutionId(String clusterName, Long lastGeneratedExecutionId) {
    String path = getLastGeneratedExecutionIdPath(clusterName);
    updateExecutionToZk(path, lastGeneratedExecutionId);
  }

  private Long getExecutionIdFromZk(String path) {
    int retry = ZK_RETRY_COUNT;
    while (retry > 1) {
      try {
        String lastSucceedExecutionId = zkclient.readData(path, true);
        if (lastSucceedExecutionId == null || lastSucceedExecutionId.isEmpty()) {
          return -1L;
        }
        return Long.valueOf(lastSucceedExecutionId);
      } catch (Exception e) {
        logger.warn("Could not get the execution id from ZK from: " + path + ". Will retry the query.", e);
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
        logger.warn("Could not update the execution id to ZK in: " + path + ". Will retry the query.", e);
        retry--;
      }
    }
    throw new VeniceException(
        "After retry " + ZK_RETRY_COUNT + " times, could not update the execution id to ZK in: " + path);
  }

  public static String getLastSucceedExecutionIdPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + EXECUTION_ID_DIR + "/lastSucceedExecutionId";
  }

  public static String getLastGeneratedExecutionIdPath(String clusterName) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + EXECUTION_ID_DIR + "/lastGeneratedExecutionId";
  }
}
