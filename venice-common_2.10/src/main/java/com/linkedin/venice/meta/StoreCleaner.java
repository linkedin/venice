package com.linkedin.venice.meta;

public interface StoreCleaner {
  void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber);

  void retireOldStoreVersions(String clusterName, String storeName);
}
