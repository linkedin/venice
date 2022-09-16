package com.linkedin.venice.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * This class implements a generic trie-based lookup tree.
 * One feature in this implementation is to support wildcard match,
 * right now, it only supports '*' to match any paths.
 *
 * You can register the following path patterns, which can be associate w/o wildcard:
 * /a/b\/*\/*   => resource1
 * /a/b\/*      => resource2
 * /a/b/c       => resource3
 * You will get following results when do lookup:
 * /a/b/c/d       => resource1
 * /a/b/ccc/eee   => resource1
 * /a/b/c         => resource3
 * /a/b/d         => resource2
 * /aa/           => null
 *
 * The operations support here:
 * 1. Register paths w/o wildcard associated with resources;
 * 2. Unregister paths;
 * 3. Lookup/Match, by default it enables wildcard matching logic;
 *    3.1 Exact match is preferable to wildcard match for a given path;
 *    3.2 '*' is used to match any single level path;
 *
 * @param <T>
 */

public class TrieBasedPathResourceRegistry<T> implements PathResourceRegistry<T> {
  private final ReadWriteLock trieLock = new ReentrantReadWriteLock();
  // Root node for Trie
  private TrieNode<T> root;

  public TrieBasedPathResourceRegistry() {
    root = new TrieNode<>();
  }

  private List<String> getPathList(String path) {
    String[] paths = path.split("/");
    List<String> pathList = new ArrayList<>();
    for (String cPath: paths) {
      if (!cPath.isEmpty()) {
        pathList.add(cPath);
      }
    }

    return pathList;
  }

  @Override
  public void register(String path, T resource) {
    trieLock.writeLock().lock();
    try {
      root.addChild(getPathList(path), resource);
    } finally {
      trieLock.writeLock().unlock();
    }
  }

  @Override
  public void unregister(String path) {
    trieLock.writeLock().lock();
    try {
      root.removeChild(getPathList(path));
    } finally {
      trieLock.writeLock().unlock();
    }
  }

  @Override
  public T find(String path) {
    trieLock.readLock().lock();
    try {
      return root.match(getPathList(path));
    } finally {
      trieLock.readLock().unlock();
    }
  }

  private static class TrieNode<T> {
    private TrieNode<T> parent;
    private String path;
    private Map<String, TrieNode<T>> children;
    private T resource;

    public TrieNode() {
      this(null, null);
    }

    public TrieNode(String path, TrieNode<T> parent) {
      this.path = path;
      this.parent = parent;
      children = new HashMap<>();
    }

    private void setResource(T resource) {
      this.resource = resource;
    }

    private T getResource() {
      return resource;
    }

    private Map<String, TrieNode<T>> getChildren() {
      return children;
    }

    private int getChildNum() {
      return children.size();
    }

    private TrieNode<T> getParent() {
      return parent;
    }

    private String getPath() {
      return path;
    }

    private boolean isValidNode() {
      return getPath() != null;
    }

    private boolean hasResource() {
      return getResource() != null;
    }

    public void addChild(List<String> paths, T resource) {
      addChild(paths, 0, resource);
    }

    private void addChild(List<String> paths, int index, T resource) {
      int pathSize = paths.size();
      if (index >= pathSize) {
        return;
      }
      String currentPath = paths.get(index);
      TrieNode<T> child = children.get(currentPath);
      if (child == null) {
        // Add new child
        child = new TrieNode<>(currentPath, this);
        children.put(currentPath, child);
      }
      if (index == pathSize - 1) {
        child.setResource(resource);
      } else {
        child.addChild(paths, index + 1, resource);
      }
    }

    public T match(List<String> paths) {
      TrieNode<T> matchedNode = match(paths, 0, false);
      if (matchedNode == null) {
        return null;
      }
      return matchedNode.getResource();
    }

    private TrieNode<T> match(List<String> paths, int index, boolean exactMatch) {
      int pathSize = paths.size();
      if (index >= pathSize) {
        return null;
      }
      TrieNode<T> matchedNode = null;
      String currentPath = paths.get(index);
      // Try exact match first
      TrieNode<T> child = children.get(currentPath);
      if (child != null) {
        if (index == pathSize - 1) {
          matchedNode = child;
        } else {
          matchedNode = child.match(paths, index + 1, exactMatch);
        }
        if (matchedNode != null) {
          return matchedNode;
        }
      }
      if (!exactMatch && !WILDCARD_MATCH_ANY.equals(currentPath)) {
        // No exact match, try wildcard match
        child = children.get(WILDCARD_MATCH_ANY);
        if (child != null) {
          if (index == pathSize - 1) {
            matchedNode = child;
          } else {
            matchedNode = child.match(paths, index + 1, exactMatch);
          }
        }
      }
      return matchedNode;
    }

    public void removeChild(List<String> paths) {
      if (paths.isEmpty()) {
        return;
      }
      TrieNode<T> matchedNode = match(paths, 0, true);
      if (matchedNode == null) {
        // No match
        return;
      }
      // Remove unused path
      TrieNode<T> currentNode = matchedNode;
      TrieNode<T> parentNode = currentNode.getParent();

      while (parentNode != null) {
        if (parentNode.getChildNum() > 1) {
          // Parent has more than 1 children, so we only can remove one of its children
          break;
        }
        if (!parentNode.isValidNode()) {
          // We hit root node
          break;
        }
        if (parentNode.hasResource()) {
          // Parent node has resource associated with itself
          break;
        }
        currentNode = parentNode;
        parentNode = currentNode.getParent();
      }
      // Should always be not null since the root node is with 'null' path
      if (parentNode != null) {
        parentNode.getChildren().remove(currentNode.getPath());
      }
    }
  }
}
