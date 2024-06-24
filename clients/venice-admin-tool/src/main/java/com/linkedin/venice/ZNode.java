package com.linkedin.venice;

import java.util.HashSet;
import java.util.Set;


/**
 * Represents a folder in ZooKeeper.
 * ZNodes are similar to directories in a file system so a chain of children can form paths.
 */
public class ZNode {
  /** val represents the name of this current folder */
  private String val;
  /** children represents the subfolders of this current folder */
  private Set<ZNode> children;
  /** path represents the full path of this folder */
  private String path;

  // root ZNode
  public ZNode(String val) {
    this.val = val;
    this.children = new HashSet<>();
    this.path = val;
  }

  // non-root ZNode
  public ZNode(String val, String path) {
    this.val = val;
    this.children = new HashSet<>();
    this.path = path + "/" + val;
  }

  public String getVal() {
    return val;
  }

  public Set<ZNode> getChildren() {
    return children;
  }

  public String getPath() {
    return path;
  }

  public void addChild(String folder) {
    children.add(new ZNode(folder, this.path));
  }

  public void deleteChild(String folder) {
    children.removeIf(node -> node.getVal().equals(folder));
  }

  public boolean contains(String folder) {
    return children.stream().anyMatch(node -> node.getVal().equals(folder));
  }
}
