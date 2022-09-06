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
}
