package com.linkedin.venice;

import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class CloneVeniceZKPaths {
  // Required Venice paths in clusters
  private static final Set<String> requiredPaths = new HashSet<>(
      Arrays
          .asList("adminTopicMetadata", "executionids", "ParentOfflinePushes", "routers", "StoreGraveyard", "Stores"));

  public static void clone(String srcZK, String destZK, String clusters, String basePath) {
    Set<String> clusterNames = Utils.parseCommaSeparatedStringToSet(clusters);

    // get Venice-specific paths from source ZK
    // ArrayList<String> venicePaths = getVenicePaths(srcZK, clusterNames, basePath, requiredPaths);
    // TODO: build zk client and connect zookeeper server
  }

  // TODO: research GLOB java, regex format, (**) means all subdirectories and files to optimize code

  /**
   * Get Venice-specific paths filtered by base path, cluster names, and required paths from a list of ZK paths.
   */
  public static ArrayList<String> getVenicePaths(
      ArrayList<String> zkPaths,
      Set<String> clusterNames,
      String basePath,
      Set<String> requiredPaths) {
    ArrayList<String> venicePaths = new ArrayList<>();
    for (String path: zkPaths) {
      if (path.startsWith(basePath)) {
        String[] pathParts = path.substring(1).split("/");
        if (pathParts.length == 2) {
          if (pathParts[1].equals("storeConfigs") || clusterNames.contains(pathParts[1])) {
            venicePaths.add(path);
          }
        } else if (pathParts.length > 2) {
          if (clusterNames.contains(pathParts[1]) && requiredPaths.contains(pathParts[2])) {
            venicePaths.add(path);
          }
        }
      }
    }
    return venicePaths;
  }
}
