package com.linkedin.venice.meta;

public interface StoreCleaner {
  void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber);

  void retireOldStoreVersions(
      String clusterName,
      String storeName,
      boolean deleteBackupOnStartPush,
      int currentVersionBeforePush);

  /**
   * This purpose of this function is to execute some topic related cleanup when push job is completed.
   * For example, we might want to enable Kafka compaction to remove duplicate entries from the topic,
   * since all the messages in the topic has been verified, and it is not necessary to verify them
   * again during re-bootstrap, which could be triggered by Helix load rebalance.
   */
  void topicCleanupWhenPushComplete(String clusterName, String storeName, int versionNumber);

  /**
   * This purpose of the function is to check if the given resource exists in the Helix cluster.
   * @param clusterName The Venice cluster that the resource belongs to.
   * @param resourceName it's usually the store version name (version topic name).
   * @return
   */
  boolean containsHelixResource(String clusterName, String resourceName);

  /**
   * This purpose of the function is to delete the given resource from the Helix cluster.
   *
   * Different from {@link #deleteOneStoreVersion(String, String, int)}, this function will not check
   * whether the store version is still a valid version inside Venice backend, and it will send the delete
   * request to Helix cluster directly. Do enough sanity check before calling this function.
   * @param clusterName The Venice cluster that the resource belongs to.
   * @param resourceName It's usually the store version name (version topic name).
   */
  void deleteHelixResource(String clusterName, String resourceName);
}
