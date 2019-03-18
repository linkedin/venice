package com.linkedin.venice.controller.init;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.CompletableFuture;
import org.apache.log4j.Logger;

/**
 * Takes care of executing each routine once per cluster. If a routine fails, it will have
 * another chance of executing next time the same controller becomes leader of the cluster
 * for which the routine previously failed.
 */
public class ControllerInitializationManager implements ControllerInitializationRoutine {
  private static final Logger LOGGER = Logger.getLogger(ControllerInitializationManager.class);

  /**
   * Used to keep track of which clusters have been initialized with which routine.
   *
   * The outer map's key is the cluster name and its value is an inner map.
   *
   * The inner maps' keys are the routines to execute, and the value is ignored (i.e.: it is used as a set).
   */
  private final Map<String, Map<ControllerInitializationRoutine, Object>> initializedClusters =
      new VeniceConcurrentHashMap<>();
  private final List<ControllerInitializationRoutine> initRoutines;

  public ControllerInitializationManager(List<ControllerInitializationRoutine> initRoutines) {
    this.initRoutines = initRoutines;
  }

  @Override
  public void execute(String clusterToInit) {
    CompletableFuture.runAsync(() -> {
      Map<ControllerInitializationRoutine, Object> initializedRoutinesForCluster =
          initializedClusters.computeIfAbsent(clusterToInit, k -> new VeniceConcurrentHashMap());

      initRoutines.forEach(routine -> {
        initializedRoutinesForCluster.computeIfAbsent(routine, k -> {
          try {
            LOGGER.info(logMessage("Starting", routine, clusterToInit));
            routine.execute(clusterToInit);
            LOGGER.info(logMessage("Finished", routine, clusterToInit));
          } catch (Exception e) {
            LOGGER.error(logMessage("Failed", routine, clusterToInit) + " Will proceed to the next initialization routine.", e);
            return null; // Will not populate the inner map...
          }
          return new Object(); // Success
        });
      });
    });
  }

  private String logMessage(String action, ControllerInitializationRoutine routine, String clusterToInit) {
    return action + " execution of '" + routine.getClass().getSimpleName()
        + "' for cluster '" + clusterToInit + "'.";
  }
}
