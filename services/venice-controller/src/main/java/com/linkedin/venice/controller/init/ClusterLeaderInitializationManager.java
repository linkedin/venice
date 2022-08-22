package com.linkedin.venice.controller.init;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Takes care of executing each routine once per cluster. If a routine fails, it will have
 * another chance of executing next time the same controller becomes leader of the cluster
 * for which the routine previously failed.
 */
public class ClusterLeaderInitializationManager implements ClusterLeaderInitializationRoutine {
  private static final Logger LOGGER = LogManager.getLogger(ClusterLeaderInitializationManager.class);

  /**
   * Used to keep track of which clusters have been initialized with which routine.
   *
   * The outer map's key is the cluster name and its value is an inner map.
   *
   * The inner maps' keys are the routines to execute, and the value is ignored (i.e.: it is used as a set).
   */
  private final Map<String, Map<ClusterLeaderInitializationRoutine, Object>> initializedClusters =
      new VeniceConcurrentHashMap<>();
  private final List<ClusterLeaderInitializationRoutine> initRoutines;
  private final boolean concurrentInit;

  public ClusterLeaderInitializationManager(
      List<ClusterLeaderInitializationRoutine> initRoutines,
      boolean concurrentInit) {
    this.initRoutines = initRoutines;
    this.concurrentInit = concurrentInit;
  }

  /**
   * @see ClusterLeaderInitializationRoutine#execute(String)
   */
  @Override
  public void execute(String clusterToInit) {
    Map<ClusterLeaderInitializationRoutine, Object> initializedRoutinesForCluster =
        initializedClusters.computeIfAbsent(clusterToInit, k -> new VeniceConcurrentHashMap());

    if (concurrentInit) {
      initRoutines.forEach(
          routine -> CompletableFuture
              .runAsync(() -> initRoutine(clusterToInit, initializedRoutinesForCluster, routine)));
    } else {
      CompletableFuture.runAsync(
          () -> initRoutines.forEach(routine -> initRoutine(clusterToInit, initializedRoutinesForCluster, routine)));
    }
  }

  private void initRoutine(
      String clusterToInit,
      Map<ClusterLeaderInitializationRoutine, Object> initializedRoutinesForCluster,
      ClusterLeaderInitializationRoutine routine) {
    initializedRoutinesForCluster.computeIfAbsent(routine, k -> {
      try {
        LOGGER.info(logMessage("Starting", routine, clusterToInit));
        routine.execute(clusterToInit);
        LOGGER.info(logMessage("Finished", routine, clusterToInit));
      } catch (Exception e) {
        LOGGER.error(
            logMessage("Failed", routine, clusterToInit) + (concurrentInit
                ? " Other initialization routines are unaffected."
                : " Will proceed to the next initialization routine."),
            e);
        return null; // Will not populate the inner map...
      }
      return new Object(); // Success
    });
  }

  private String logMessage(String action, ClusterLeaderInitializationRoutine routine, String clusterToInit) {
    return action + " execution of '" + routine.toString() + "' for cluster '" + clusterToInit + "'.";
  }
}
