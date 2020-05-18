package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.PartitionFinder;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;


public class VenicePartitionFinder implements PartitionFinder<RouterKey> {
  private final RoutingDataRepository dataRepository;
  private final HelixReadOnlyStoreRepository metadataRepository;

  // a map of map: each store could have multiple versions and each version has a specific partitioner
  private Map<String, Map<Integer, VenicePartitioner>> storeByVersionByPartitionerMap = new VeniceConcurrentHashMap<>();

  public VenicePartitionFinder(RoutingDataRepository dataRepository, HelixReadOnlyStoreRepository metadataRepository){
    this.dataRepository = dataRepository;
    this.metadataRepository = metadataRepository;
    this.metadataRepository.registerStoreDataChangedListener(storeChangeListener);
  }

  /***
   *
   * @param resourceName
   * @param partitionKey
   * @return partition Name, ex "store_v3_5"
   */
  @Override
  public String findPartitionName(String resourceName, RouterKey partitionKey) {
    int partitionId = findPartitionNumber(partitionKey, getNumPartitions(resourceName),
        Version.parseStoreFromKafkaTopicName(resourceName), Version.parseVersionFromKafkaTopicName(resourceName));
    return HelixUtils.getPartitionName(resourceName, partitionId);
  }

  public int findPartitionNumber(RouterKey partitionKey, int numPartitions, String storeName, int versionNumber) {
    return findPartitioner(storeName, versionNumber).getPartitionId(partitionKey.getKeyBuffer(), numPartitions);
  }

  @Override
  public List<String> getAllPartitionNames(String resourceName) {
    return dataRepository.getPartitionAssignments(resourceName).getAllPartitions()
        .stream()
        .map(p -> HelixUtils.getPartitionName(resourceName, p.getId()))
        .collect(Collectors.toList());
  }

  @Override
  public int getNumPartitions(String resourceName) {
    return dataRepository.getNumberOfPartitions(resourceName);
  }

  /**
   * Query the map to find the partitioner in need.
   * If miss, real search using store info happens in {@link #searchPartitioner}
   */
  private VenicePartitioner findPartitioner(String storeName, int versionNum) {
    Map versionByPartitionerMap = storeByVersionByPartitionerMap.computeIfAbsent(storeName, k -> new VeniceConcurrentHashMap<>());
    return (VenicePartitioner) versionByPartitionerMap.computeIfAbsent(versionNum, k -> searchPartitioner(storeName, versionNum));
  }

  private VenicePartitioner searchPartitioner(String storeName, int versionNum) {
    Store store = metadataRepository.getStore(storeName);
    if (store == null) {
      throw new VeniceException("Unknown store: " + storeName);
    }
    Optional<Version> version = store.getVersion(versionNum);
    if (! version.isPresent()) {
      throw new VeniceException("Unknown version: " + versionNum + " in store: " + storeName);
    }
    PartitionerConfig partitionerConfig = version.get().getPartitionerConfig();
    Properties params = new Properties();
    params.putAll(partitionerConfig.getPartitionerParams());
    VeniceProperties partitionerProperties = new VeniceProperties(params);
    return PartitionUtils.getVenicePartitioner(partitionerConfig.getPartitionerClass(),
        partitionerConfig.getAmplificationFactor(), partitionerProperties);
  }

  private final StoreDataChangedListener storeChangeListener = new StoreDataChangedListener() {
    @Override
    public void handleStoreChanged(Store store) {
      String storeName = store.getName();
      Set<Integer> upToDateVersionsSet = store.getVersions().stream().map(Version::getNumber).collect(Collectors.toSet());

      // remove out dated versions (if any) from the map
      if (storeByVersionByPartitionerMap.containsKey(storeName)) {
        Map<Integer, VenicePartitioner> versionByPartitionerMap = storeByVersionByPartitionerMap.get(storeName);
        for (Integer candidateVersion : versionByPartitionerMap.keySet()) {
          if (!upToDateVersionsSet.contains(candidateVersion)) {
            versionByPartitionerMap.remove(candidateVersion);
          }
        }
      }

      // add new versions to the map proactively to accelerate future possible access
      for (Integer version : upToDateVersionsSet) {
        findPartitioner(storeName, version);
      }
    }

    @Override
    public void handleStoreDeleted(Store store) {
      storeByVersionByPartitionerMap.remove(store.getName());
    }
  };
}
