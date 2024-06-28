package com.linkedin.venice;

import static com.linkedin.venice.VeniceZkPaths.ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.VeniceZkPaths.EXECUTION_IDS;
import static com.linkedin.venice.VeniceZkPaths.PARENT_OFFLINE_PUSHES;
import static com.linkedin.venice.VeniceZkPaths.ROUTERS;
import static com.linkedin.venice.VeniceZkPaths.STORES;
import static com.linkedin.venice.VeniceZkPaths.STORE_GRAVEYARD;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;


public class ZkCopier {
  private static final Set<String> VENICE_ZK_PATHS = new HashSet<>(
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
    try {
      // read ZK snapshot input file and store all paths in a list
      File inputFile = new File(inputPath);
      List<String> zkPaths = FileUtils.readLines(inputFile);

      // write Venice-specific paths to output file
      ArrayList<String> venicePaths = getVenicePaths(new ArrayList<>(zkPaths), clusterNames, basePath, VENICE_ZK_PATHS);
      File outputFile = new File(outputPath);
      FileUtils.writeLines(outputFile, venicePaths);
    } catch (IOException e) {
      throw new VeniceException(e.getMessage());
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
    TreeNode requiredPathsTree = buildRequiredPathsTree(clusterNames, basePath, requiredPaths);
    TreeNode zkPathsTree = listToTree(zkPaths, basePath);
    getVenicePathsHelper(zkPathsTree, requiredPathsTree);
    return treeToList(zkPathsTree);
  }

  private static void getVenicePathsHelper(TreeNode zkPathsTree, TreeNode requiredPathsTree) {
    if (requiredPathsTree.getChildren().isEmpty() || zkPathsTree.getChildren().isEmpty()) {
      return;
    }
    ArrayList<String> TreeNodesToBeDeleted = new ArrayList<>();
    for (TreeNode zkChild: zkPathsTree.getChildren().values()) {
      if (!requiredPathsTree.contains(zkChild.getVal())) {
        TreeNodesToBeDeleted.add(zkChild.getVal());
      }
    }
    for (String str: TreeNodesToBeDeleted) {
      zkPathsTree.deleteChild(str);
    }
    for (TreeNode requiredChild: requiredPathsTree.getChildren().values()) {
      if (zkPathsTree.contains(requiredChild.getVal())) {
        getVenicePathsHelper(zkPathsTree.getChildren().get(requiredChild.getVal()), requiredChild);
      }
    }
  }

  public static TreeNode buildRequiredPathsTree(Set<String> clusterNames, String basePath, Set<String> requiredPaths) {
    TreeNode root = new TreeNode(basePath);
    root.addChild("storeConfigs");
    for (String cluster: clusterNames) {
      root.addChild(cluster);
    }
    for (TreeNode child: root.getChildren().values()) {
      if (!child.getVal().equals("storeConfigs")) {
        for (String path: requiredPaths) {
          child.addChild(path);
        }
      }
    }
    return root;
  }

  public static TreeNode listToTree(ArrayList<String> zkPaths, String basePath) {
    TreeNode root = new TreeNode(basePath);
    for (String path: zkPaths) {
      String[] pathParts = path.substring(1).split("/");
      TreeNode current = root;
      for (int i = 1; i < pathParts.length; i++) {
        String part = pathParts[i];
        if (!current.contains(part)) {
          current.addChild(part);
        }
        current = current.getChildren().get(part);
      }
    }
    return root;
  }

  public static ArrayList<String> treeToList(TreeNode root) {
    ArrayList<String> paths = new ArrayList<>();
    treeToListHelper(root, paths);
    Collections.sort(paths);
    return paths;
  }

  private static void treeToListHelper(TreeNode node, ArrayList<String> paths) {
    paths.add(node.getPath());
    for (TreeNode child: node.getChildren().values()) {
      treeToListHelper(child, paths);
    }
  }

  public static Set<String> getRequiredPaths() {
    return new HashSet<>(VENICE_ZK_PATHS);
  }

  /**
   * TreeNodes are similar to directories in a file system. A chain of nested {@code TreeNode} children can form paths.
   */
  static final class TreeNode {
    /** the name of this folder */
    private String val;
    /** the subfolders of this folder */
    private Map<String, TreeNode> children;
    /** the full path of this folder */
    private String path;

    /**
     * Constructs a root {@code TreeNode} with a given value, no children, and a path equal to the value.
     * @param val the name of this folder
     * @Example: {@code TreeNode("folder")} creates a {@code TreeNode} with {@code val="folder"} and {@code path="folder"}
     */
    public TreeNode(String val) {
      this.val = val;
      this.children = new HashMap<>();
      this.path = val;
    }

    /**
     * Constructs a non-root {@code TreeNode} with a given value, no children, and a path equal to the concatenation of the parent's path and value.
     * @param val the name of this folder
     * @param parentPath the path of the parent folder
     * @Example: {@code TreeNode("folder", "/path/to/parent")} creates a {@code TreeNode} with {@code val = "folder"} and {@code path = "/path/to/parent/folder"}
     */
    public TreeNode(String val, String parentPath) {
      this.val = val;
      this.children = new HashMap<>();
      this.path = parentPath + "/" + val;
    }

    public String getVal() {
      return val;
    }

    public Map<String, TreeNode> getChildren() {
      return children;
    }

    public String getPath() {
      return path;
    }

    public void addChild(String folder) {
      children.put(folder, new TreeNode(folder, this.path));
    }

    public void deleteChild(String folder) {
      children.remove(folder);
    }

    public boolean contains(String folder) {
      return children.containsKey(folder);
    }
  }
}
