package com.linkedin.venice.etl.publisher;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;


/**
 * A helper class that stores the version to snapshot/delta list mapping.
 */
public class StoreFilesInfo {
  private TreeMap<Integer, PriorityQueue<Path>> versionToFilesPath;
  private Comparator<Path> comparator;

  public StoreFilesInfo(Comparator<Path> comparator) {
    // The versions are sorted for time stamp.
    this.versionToFilesPath = new TreeMap<>();
    this.comparator = comparator;
  }

  public Set<Integer> getVersions() {
    return versionToFilesPath.keySet();
  }
  /**
   * Adds each file path corresponding to a store version to the queue
   * @param version
   * @param filePath
   */
  protected void addFileForStoreVersion(int version, Path filePath) {
    PriorityQueue<Path> filesQueue = this.versionToFilesPath.get(version);
    if (null == filesQueue) {
      filesQueue = new PriorityQueue<>(this.comparator);
      this.versionToFilesPath.put(version, filesQueue);
    }
    filesQueue.offer(filePath);
  }

  protected PriorityQueue<Path> getFilesByVersion(int version) {
    return versionToFilesPath.get(version);
  }

  protected int getLargestVersionNumber() {
    return this.versionToFilesPath.lastKey();
  }

}

