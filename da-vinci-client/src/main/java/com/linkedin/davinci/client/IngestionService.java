package com.linkedin.davinci.client;

import java.util.List;


public interface IngestionService {
  // Check if a specific partition of a version is ready.
  boolean isPartitionReadyToServe(String storeName, int versionId, int partitionId);

  // Return all ready-to-serve partitions related to specific version.
  List<Integer> getReadyToServePartitions(String storeName, int versionId);
}
