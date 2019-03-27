package com.linkedin.venice.helix;

import java.io.Closeable;
import java.util.Set;


/**
 * Interface defines the ways to access to the white list of Helix NodeId.
 */
public interface WhitelistAccessor extends Closeable {
  /**
   * Judge whether the given helix nodeId is in the white list or not.
   */
  boolean isInstanceInWhitelist(String clusterName, String helixNodeId);

  /**
   * Get all helix nodeIds in the white list of the given cluster from ZK.
   */
  Set<String> getWhiteList(String clusterName);

  /**
   * Add the given helix nodeId into the white list in ZK.
   */
  void addInstanceToWhiteList(String clusterName, String helixNodeId);

  /**
   * Remove the given helix nodeId from the white list in ZK.
   */
  void removeInstanceFromWhiteList(String clusterName, String helixNodeId);
}
