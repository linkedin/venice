package com.linkedin.venice;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class CloneVeniceZKPaths {
  // Required Venice-specific paths in clusters
  private static final String ADMIN_TOPIC_METADATA = "adminTopicMetadata";
  private static final String EXECUTION_IDS = "executionids";
  private static final String PARENT_OFFLINE_PUSHES = "ParentOfflinePushes";
  private static final String ROUTERS = "routers";
  private static final String STORE_GRAVEYARD = "StoreGraveyard";
  private static final String STORES = "Stores";
  private static final Set<String> requiredPaths = new HashSet<>(
      Arrays.asList(ADMIN_TOPIC_METADATA, EXECUTION_IDS, PARENT_OFFLINE_PUSHES, ROUTERS, STORE_GRAVEYARD, STORES));

  /**
   * Clone Venice-specific paths from a source ZK to a destination ZK.
   */
  public static void cloneVenicePaths(String srcZK, String destZK, Set<String> clusterNames, String basePath) {
    // get Venice-specific paths from source ZK
    // TODO: build zk client and connect zookeeper server
    // ZkClient foo = new ZkClient(zkAddress);
    // String root = foo.readData("<basepath>");
    // List<String> rootChildren = foo.getChildren("<basepath>");
    // foo.close();
    // ArrayList<String> venicePaths = getVenicePaths(list of paths from srcZK, clusterNames, basePath, requiredPaths);
  }

  /**
   * Extract Venice-specific paths from an input text file to an output text file.
   * @param inputPath path to the ZK snapshot input text file
   * @param outputPath path to output text file with extracted Venice-specific paths
   */
  public static void extractVenicePaths(
      String inputPath,
      String outputPath,
      Set<String> clusterNames,
      String basePath) {
    ArrayList<String> zkPaths = new ArrayList<>();
    // read ZK snapshot input file and store all paths into a list
    try {
      BufferedReader br = new BufferedReader(new FileReader(inputPath));
      String line = br.readLine();
      while (line != null) {
        zkPaths.add(line);
        line = br.readLine();
      }
      br.close();
    } catch (IOException e) {
      throw new VeniceException("Error reading ZK snapshot input file :" + e.getMessage());
    }

    // write Venice-specific paths to output file
    ArrayList<String> venicePaths = getVenicePaths(zkPaths, clusterNames, basePath, requiredPaths);
    File venicePathsFile = new File(outputPath);
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(venicePathsFile));
      for (String str: venicePaths) {
        bw.write(str + System.lineSeparator());
      }
      bw.close();
    } catch (IOException e) {
      throw new VeniceException("Error writing to output file :" + e.getMessage());
    }
  }

  /**
   * Get Venice-specific paths filtered by base path, cluster names, and required paths from a list of ZK paths.
   */
  public static ArrayList<String> getVenicePaths(
      ArrayList<String> zkPaths,
      Set<String> clusterNames,
      String basePath,
      Set<String> requiredPaths) {
    ArrayList<String> venicePaths = new ArrayList<>();
    // TODO: Use HashMap to store ZK paths and their children
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

  public static Set<String> getRequiredPaths() {
    return requiredPaths;
  }
}
