package com.linkedin.venice.controller.init;

import com.linkedin.venice.utils.LogContext;
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
  private final boolean blockUntilComplete;
  private final LogContext logContext;
  private volatile boolean hasExecutedOnce = false;

  public ClusterLeaderInitializationManager(
      List<ClusterLeaderInitializationRoutine> initRoutines,
      boolean concurrentInit,
      LogContext logContext) {
    this(initRoutines, concurrentInit, false, logContext);
  }

  public ClusterLeaderInitializationManager(
      List<ClusterLeaderInitializationRoutine> initRoutines,
      boolean concurrentInit,
      boolean blockUntilComplete,
      LogContext logContext) {
    this.initRoutines = initRoutines;
    this.concurrentInit = concurrentInit;
    this.blockUntilComplete = blockUntilComplete;
    this.logContext = logContext;
  }

  /**
   * @see ClusterLeaderInitializationRoutine#execute(String)
   */
  @Override
  public void execute(String clusterToInit) {
    Map<ClusterLeaderInitializationRoutine, Object> initializedRoutinesForCluster =
        initializedClusters.computeIfAbsent(clusterToInit, k -> new VeniceConcurrentHashMap<>());

    // Check if this is the first time execute() has been called on this manager instance
    boolean isFirstExecution = !hasExecutedOnce;

    LOGGER.info(
        "Starting initialization routines for cluster: {}. Mode: {}, BlockUntilComplete: {}, FirstExecution: {}, RoutineCount: {}",
        clusterToInit,
        concurrentInit ? "CONCURRENT" : "SEQUENTIAL",
        blockUntilComplete,
        isFirstExecution,
        initRoutines.size());

    long startTime = System.currentTimeMillis();
    CompletableFuture<Void> allRoutinesFuture;
    if (concurrentInit) {
      CompletableFuture<?>[] futures = initRoutines.stream()
          .map(
              routine -> CompletableFuture
                  .runAsync(() -> initRoutine(clusterToInit, initializedRoutinesForCluster, routine)))
          .toArray(CompletableFuture[]::new);
      allRoutinesFuture = CompletableFuture.allOf(futures);
    } else {
      allRoutinesFuture = CompletableFuture.runAsync(
          () -> initRoutines.forEach(routine -> initRoutine(clusterToInit, initializedRoutinesForCluster, routine)));
    }

    // Block on the first execution if the flag is enabled
    if (blockUntilComplete && isFirstExecution) {
      try {
        LOGGER.info(
            "Blocking until all initialization routines complete for cluster: {} (first time execute called)",
            clusterToInit);
        allRoutinesFuture.join();
        long elapsedMs = System.currentTimeMillis() - startTime;
        LOGGER.info(
            "All initialization routines completed for cluster: {} (first time execute called). Elapsed time: {} ms",
            clusterToInit,
            elapsedMs);
      } catch (Exception e) {
        long elapsedMs = System.currentTimeMillis() - startTime;
        LOGGER.error(
            "Error while waiting for initialization routines to complete for cluster: {}. Elapsed time: {} ms",
            clusterToInit,
            elapsedMs,
            e);
      } finally {
        hasExecutedOnce = true;
      }
    } else {
      if (!hasExecutedOnce) {
        hasExecutedOnce = true;
      }
      LOGGER.info("Initialization routines started asynchronously for cluster: {}. Blocking: false", clusterToInit);
    }
  }

  private void initRoutine(
      String clusterToInit,
      Map<ClusterLeaderInitializationRoutine, Object> initializedRoutinesForCluster,
      ClusterLeaderInitializationRoutine routine) {
    initializedRoutinesForCluster.computeIfAbsent(routine, k -> {
      long routineStartTime = System.currentTimeMillis();
      try {
        LogContext.setLogContext(logContext);
        LOGGER.info(
            "Starting execution of initialization routine '{}' for cluster '{}'",
            routine.toString(),
            clusterToInit);
        routine.execute(clusterToInit);
        long routineElapsedMs = System.currentTimeMillis() - routineStartTime;
        LOGGER.info(
            "Finished execution of initialization routine '{}' for cluster '{}'. Elapsed time: {} ms",
            routine.toString(),
            clusterToInit,
            routineElapsedMs);
      } catch (Exception e) {
        long routineElapsedMs = System.currentTimeMillis() - routineStartTime;
        LOGGER.error(
            "Failed execution of initialization routine '{}' for cluster '{}'. Elapsed time: {} ms. {}",
            routine.toString(),
            clusterToInit,
            routineElapsedMs,
            (concurrentInit
                ? "Other initialization routines are unaffected."
                : "Will proceed to the next initialization routine."),
            e);
        return null; // Will not populate the inner map...
      } finally {
        LogContext.clearLogContext();
      }
      return new Object(); // Success
    });
  }
}
