package com.linkedin.davinci.client;

import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.server.StorageEngineRepository;
import com.linkedin.venice.store.AbstractStorageEngine;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class DaVinciVersionFinder {
  private final ReadOnlyStoreRepository storeRepository;
  private final IngestionController ingestionController;
  private final StorageEngineRepository storageEngineRepository;
  private final String storeName;

  public DaVinciVersionFinder(String storeName,
                              ReadOnlyStoreRepository storeRepository,
                              IngestionController ingestionController,
                              StorageEngineRepository storageEngineRepository) {
    this.storeName = storeName;
    this.storeRepository = storeRepository;
    this.ingestionController = ingestionController;
    this.storageEngineRepository = storageEngineRepository;
  }

  // Get latest available version of a specific partition this DaVinci client subscribes to.
  public int getLatestVersion(int partitionId) throws VeniceException {
    List<Version> storeVersions = getSortedDaVinciStoreVersionList();
    for (Version version : storeVersions) {
      AbstractStorageEngine engine = storageEngineRepository.getLocalStorageEngine(version.kafkaTopicName());
      if ((engine == null) || (!engine.containsPartition(partitionId))) {
        // Local store does not have corresponding storage engine for this version or partition of this version.
        continue;
      }
      // Check if this partition is ready.
      if ((ingestionController.isPartitionReady(storeName, version.getNumber(), partitionId))) {
        return version.getNumber();
      }
    }
    return Store.NON_EXISTING_VERSION;
  }

  // Get latest available version on all partitions this DaVinci client subscribes to.
  public int getLatestVersion(List<Integer> subscribedPartitions) throws VeniceException {
    List<Version> storeVersions = getSortedDaVinciStoreVersionList();
    for (Version version : storeVersions) {
      AbstractStorageEngine engine = storageEngineRepository.getLocalStorageEngine(version.kafkaTopicName());
      if (engine == null) {
        // Local store does not have corresponding storage engine for this version.
        continue;
      }
      // Set of partition that local storage engine contains for this version.
      Set<Integer> localPartitions = engine.getPartitionIds();
      // List of partitions of this version that are ready to serve.
      Set<Integer> readyToServePartitions = ingestionController.getReadyPartitions(storeName, version.getNumber());
      if (readyToServePartitions.containsAll(subscribedPartitions) && localPartitions.containsAll(subscribedPartitions)) {
        return version.getNumber();
      }
    }
    return Store.NON_EXISTING_VERSION;
  }

  private List<Version> getSortedDaVinciStoreVersionList() throws VeniceException {
    Store daVinciStore = storeRepository.getStoreOrThrow(storeName);
    // Check read permission of this store.
    if (!daVinciStore.isEnableReads()) {
      throw new StoreDisabledException(storeName, "read");
    }

    // Return a list of globally available versions sorted in reversed order.
    return daVinciStore.getVersions().stream()
        .sorted(Comparator.comparingInt(Version::getNumber).reversed())
        .collect(Collectors.toList());
  }
}
