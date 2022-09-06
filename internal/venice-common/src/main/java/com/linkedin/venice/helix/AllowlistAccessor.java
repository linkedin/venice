package com.linkedin.venice.helix;

import java.io.Closeable;
import java.util.Set;


/**
 * Interface defines the ways to access to the allowlist of Helix NodeId.
 */
public interface AllowlistAccessor extends Closeable {
  /**
   * Judge whether the given helix nodeId is in the allowlist or not.
   */
  boolean isInstanceInAllowlist(String clusterName, String helixNodeId);

  /**
   * Get all helix nodeIds in the allowlist of the given cluster from ZK.
   */
  Set<String> getAllowList(String clusterName);

  /**
   * Add the given helix nodeId into the allowlist in ZK.
   */
  void addInstanceToAllowList(String clusterName, String helixNodeId);

  /**
   * Remove the given helix nodeId from the allowlist in ZK.
   */
  void removeInstanceFromAllowList(String clusterName, String helixNodeId);
}
