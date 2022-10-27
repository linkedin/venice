package com.linkedin.venice.controller.init;

/**
 * This class encapsulates business logic which needs to be executed once per cluster the
 * first time a controller becomes leader of that cluster.
 *
 * It is executed asynchronously by the {@link ClusterInitializationManager} after the
 * STANDBY -> LEADER transition completed.
 *
 * The logic should be idempotent, since it will be executed many times during the life
 * of a cluster.
 */
public interface ClusterInitializationRoutine {
  void execute(String clusterToInit);
}
