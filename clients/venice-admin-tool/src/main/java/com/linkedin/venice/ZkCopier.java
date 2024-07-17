package com.linkedin.venice;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.zk.VeniceZkPaths.EXECUTION_IDS;
import static com.linkedin.venice.zk.VeniceZkPaths.PARENT_OFFLINE_PUSHES;
import static com.linkedin.venice.zk.VeniceZkPaths.ROUTERS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_CONFIGS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_GRAVEYARD;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;


public class ZkCopier {
  private static final Set<String> CLUSTER_ZK_PATHS = new HashSet<>(
      Arrays.asList(ADMIN_TOPIC_METADATA, EXECUTION_IDS, PARENT_OFFLINE_PUSHES, ROUTERS, STORE_GRAVEYARD, STORES));

  /**
   * Extract Venice-specific paths from an input text file to an output text file.
   * @param inputPath absolute path to the ZK snapshot input text file, which contains one path per line
   * @param outputPath absolute path to output text file with extracted Venice-specific paths, which contains one path per line
   * @param clusterNames set of cluster names
   * @param basePath base path for ZK
   */
  public static void extractVenicePaths(
      String inputPath,
      String outputPath,
      Set<String> clusterNames,
      String basePath) {
    if (!basePath.startsWith("/")) {
      throw new VeniceException("Base path must start with a forward slash (/)");
    }
    try {
      // read ZK snapshot input file and store all paths in a list
      File inputFile = new File(inputPath);
      List<String> zkPaths = FileUtils.readLines(inputFile);

      // write Venice-specific paths to output file
      List<String> venicePaths = getVenicePaths(zkPaths, clusterNames, basePath);
      File outputFile = new File(outputPath);
      FileUtils.writeLines(outputFile, venicePaths);
    } catch (IOException e) {
      throw new VeniceException(e.getMessage());
    }
  }

  /**
   * Get Venice-specific paths from a list of ZK paths filtered by 1)base path, 2)cluster names, and 3)required cluster ZK paths.
   * @return a list of Venice-specific paths filtered from {@code zkPaths}
   */
  static List<String> getVenicePaths(List<String> zkPaths, Set<String> clusterNames, String basePath) {
    TreeNode requiredPathsTreeRoot = buildRequiredPathsTree(clusterNames, basePath);
    TreeNode zkPathsTreeRoot = pathsListToTree(zkPaths, basePath);
    if (!zkPathsTreeRoot.getName().equals(requiredPathsTreeRoot.getName())) {
      throw new VeniceException(
          "Base path mismatch: " + zkPathsTreeRoot.getName() + " != " + requiredPathsTreeRoot.getName());
    }
    getVenicePathsHelper(zkPathsTreeRoot, requiredPathsTreeRoot);
    return pathsTreeToList(zkPathsTreeRoot);
  }

  /**
   * Recursively match children nodes of {@code zkPathsTreeNode} and {@code requiredPathsTreeNode} and removes unmatched children nodes from {@code zkPathsTreeNode}.
   * @param zkPathsTreeNode node in the ZK paths tree
   * @param requiredPathsTreeNode node in the required paths tree
   * @Note: This method directly modifies {@code zkPathsTreeNode} in the parent method {@code getVenicePaths()}
   */
  private static void getVenicePathsHelper(TreeNode zkPathsTreeNode, TreeNode requiredPathsTreeNode) {
    if (requiredPathsTreeNode.getChildren().isEmpty() || zkPathsTreeNode.getChildren().isEmpty()) {
      return;
    }
    Iterator<Map.Entry<String, TreeNode>> iterator = zkPathsTreeNode.getChildren().entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, TreeNode> zkEntry = iterator.next();
      String zkChildName = zkEntry.getKey();
      TreeNode zkChild = zkEntry.getValue();
      if (requiredPathsTreeNode.containsChild(zkChildName)) {
        getVenicePathsHelper(zkChild, requiredPathsTreeNode.getChildren().get(zkChildName));
      } else {
        iterator.remove();
      }
    }
  }

  /**
   * Build a tree of required paths for Venice-specific ZK paths in clusters.
   * @return the root (base path) of the tree with cluster names as children and cluster ZK paths as grandchildren
   */
  static TreeNode buildRequiredPathsTree(Set<String> clusterNames, String basePath) {
    TreeNode root = new TreeNode(basePath);
    root.addChild(STORE_CONFIGS);
    for (String cluster: clusterNames) {
      TreeNode child = root.addChild(cluster);
      for (String path: CLUSTER_ZK_PATHS) {
        child.addChild(path);
      }
    }
    return root;
  }

  /**
   * Convert a list of paths to a tree of paths as nested children.
   * @return the root of the tree
   */
  static TreeNode pathsListToTree(List<String> zkPaths, String basePath) {
    TreeNode root = new TreeNode(basePath);
    for (String path: zkPaths) {
      if (!path.startsWith(basePath)) {
        continue;
      }
      String[] pathParts = path.substring(basePath.length()).split("/");
      TreeNode current = root;
      for (int i = 1; i < pathParts.length; i++) {
        String part = pathParts[i];
        if (!current.containsChild(part)) {
          current.addChild(part);
        }
        current = current.getChildren().get(part);
      }
    }
    return root;
  }

  /**
   * Convert a tree of paths to a sorted list of paths.
   * @return a sorted list of paths
   */
  static List<String> pathsTreeToList(TreeNode root) {
    List<String> paths = new ArrayList<>();
    StringBuilder currentPath = new StringBuilder();
    pathsTreeToListHelper(root, paths, currentPath);
    Collections.sort(paths);
    return paths;
  }

  /**
   * Recursively traverse the tree and add each node's full path to the list.
   * @Note: This method directly modifies the list {@code paths} in the parent method {@code pathsTreeToList()}
   */
  private static void pathsTreeToListHelper(TreeNode node, List<String> paths, StringBuilder currentPath) {
    StringBuilder currentPathCopy = new StringBuilder(currentPath);
    currentPathCopy.append(node.getName());
    paths.add(currentPathCopy.toString());
    currentPathCopy.append("/");
    for (TreeNode child: node.getChildren().values()) {
      pathsTreeToListHelper(child, paths, currentPathCopy);
    }
  }

  /**
   * TreeNodes are similar to directories in a file system. A chain of nested {@code TreeNode} children can form paths.
   */
  static final class TreeNode {
    /** the name of this folder */
    private String name;
    /** the subfolders of this folder */
    private Map<String, TreeNode> children;

    /**
     * Constructs a root {@code TreeNode} with a given name, no children, and a path equal to the name.
     * @param name the name of this folder
     * @Example: {@code TreeNode("folder")} creates a {@code TreeNode} with {@code name="folder"}
     */
    public TreeNode(String name) {
      this.name = name;
      this.children = new HashMap<>();
    }

    public String getName() {
      return name;
    }

    public Map<String, TreeNode> getChildren() {
      return children;
    }

    /**
     * Adds a child {@code TreeNode} with a given name to this folder.
     * @return the added child {@code TreeNode}
     */
    public TreeNode addChild(String name) {
      TreeNode child = new TreeNode(name);
      children.put(name, child);
      return child;
    }

    /**
     * Removes a child with the given name from this folder.
     */
    public void deleteChild(String name) {
      children.remove(name);
    }

    /**
     * Checks if this folder contains an immediate child with the given name.
     * @return true if this folder contains the child, false otherwise
     */
    public boolean containsChild(String name) {
      return children.containsKey(name);
    }
  }
}
