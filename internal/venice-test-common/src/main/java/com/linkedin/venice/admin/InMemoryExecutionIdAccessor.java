package com.linkedin.venice.admin;

import com.linkedin.venice.controller.ExecutionIdAccessor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * In memory implementation of {@link ExecutionIdAccessor}, should only be used for testing purposes.
 */
public class InMemoryExecutionIdAccessor implements ExecutionIdAccessor {
  Map<String, Long> lastSucceededExecutionIdInMem = new HashMap<>();
  Map<String, Map<String, Long>> executionIdMapInMem = new HashMap<>();

  long executionId = 0L;

  @Override
  public Long getLastSucceededExecutionId(String clusterName) {
    return lastSucceededExecutionIdInMem.getOrDefault(clusterName, -1L);
  }

  @Override
  public void updateLastSucceededExecutionId(String clusterName, Long lastSucceedExecutionId) {
    lastSucceededExecutionIdInMem.put(clusterName, lastSucceedExecutionId);
  }

  @Override
  public synchronized Map<String, Long> getLastSucceededExecutionIdMap(String clusterName) {
    return executionIdMapInMem.getOrDefault(clusterName, new HashMap<>());
  }

  @Override
  public synchronized void updateLastSucceededExecutionIdMap(
      String clusterName,
      String storeName,
      Long lastSucceededExecutionId) {
    if (executionIdMapInMem.get(clusterName) == null) {
      executionIdMapInMem.put(clusterName, new HashMap<>());
    }
    executionIdMapInMem.get(clusterName).put(storeName, lastSucceededExecutionId);
  }

  @Override
  public Long getLastGeneratedExecutionId(String clusterName) {
    return executionId;
  }

  @Override
  public void updateLastGeneratedExecutionId(String clusterName, Long lastGeneratedExecutionId) {
    // not used, no op.
  }

  @Override
  public Map<String, Long> cleanExecutionIdMap(String clusterName, Set<String> allStores) {
    // initializing `executionIdsCleaned` with all the entries
    Map<String, Long> executionIdsCleaned = new HashMap<>(executionIdMapInMem.get(clusterName));
    Map<String, Long> executionIdsToKeep = executionIdMapInMem.get(clusterName)
        .entrySet()
        .parallelStream()
        .filter(entry -> allStores.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // update `executionIdsCleaned` by removing the entries that are kept
    executionIdsCleaned.keySet().removeAll(executionIdsToKeep.keySet());
    executionIdMapInMem.put(clusterName, executionIdsToKeep);
    return executionIdsCleaned;
  }

  @Override
  public Long incrementAndGetExecutionId(String clusterName) {
    return ++executionId;
  }

  public void setExecutionId(long value) {
    executionId = value;
  }
}
