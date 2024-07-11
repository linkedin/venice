package com.linkedin.venice;

import static com.linkedin.venice.zk.VeniceZkPaths.CLUSTER_ZK_PATHS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_CONFIGS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkClientFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;


/**
 * <p>
 *   This class contains methods to 1)migrate Venice-specific metadata from a source ZooKeeper (ZK) to a destination ZK
 *   and 2)extract Venice-specific paths from an input text file containing ZK paths to an output text file.
 *   We implement a tree data structure to represent ZK paths as nested children and use it to filter out Venice-specific
 *   paths efficiently.
 * </p>
 */
public class ZkCopier {
  /**
   * Migrate Venice-specific metadata from a source ZK to a destination ZK.
   * @param srcZkAddress source ZK address
   * @param destZkAddress destination ZK address
   * @param clusterNames set of cluster names
   * @param basePath base path for ZK
   */
  public static void migrateVenicePaths(
      String srcZkAddress,
      String destZkAddress,
      Set<String> clusterNames,
      String basePath) {
    if (!basePath.startsWith("/")) {
      throw new VeniceException("Base path must start with a forward slash (/)");
    }
    ZkClient srcZkClient = ZkClientFactory.newZkClient(srcZkAddress);
    TreeNode srcZkPathsTree = getZkClientPathsTree(srcZkClient, clusterNames, basePath);
    List<String> srcZkPathsList = pathsTreeToList(srcZkPathsTree);
    List<String> destZkPathsList = getVenicePaths(srcZkPathsList, clusterNames, basePath);
    ZkClient destZkClient = ZkClientFactory.newZkClient(destZkAddress);
    for (String path: destZkPathsList) {
      destZkClient.create(path, srcZkClient.readData(path), CreateMode.PERSISTENT);
    }
  }

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
   * Build a tree of ZK paths with {@code basePath} and {@code clusterNames} by fetching children of each cluster path using the ZK client.
   * @return the root (base path) of the tree
   */
  static TreeNode getZkClientPathsTree(ZkClient zkClient, Set<String> clusterNames, String basePath) {
    TreeNode root = new TreeNode(basePath);
    TreeNode storeConfigsNode = root.addChild(STORE_CONFIGS);
    addChildrenToTreeNode(zkClient, storeConfigsNode, basePath + "/" + STORE_CONFIGS);
    for (String cluster: clusterNames) {
      TreeNode clusterChild = root.addChild(cluster);
      String clusterPath = basePath + "/" + cluster;
      for (String venicePath: CLUSTER_ZK_PATHS) {
        TreeNode venicePathChild = clusterChild.addChild(venicePath);
        addChildrenToTreeNode(zkClient, venicePathChild, clusterPath + "/" + venicePath);
      }
    }
    return root;
  }

  /**
   * Recursively add children to a {@code TreeNode} by fetching children of a ZK path using the ZK client.
   * @Note: This method directly modifies the tree {@code node} in the parent method {@code getZkClientPathsTree()}
   */
  private static void addChildrenToTreeNode(ZkClient zkClient, TreeNode node, String path) {
    if (!zkClient.exists(path)) {
      return;
    }
    List<String> children = zkClient.getChildren(path);
    for (String child: children) {
      TreeNode childNode = node.addChild(child);
      addChildrenToTreeNode(zkClient, childNode, path + "/" + child);
    }
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
