package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.PartitionFinder;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.HelixUtils;
import java.util.List;
import java.util.stream.Collectors;


public class VenicePartitionFinder implements PartitionFinder<RouterKey> {

  private RoutingDataRepository dataRepository;
  private final VenicePartitioner partitioner = new DefaultVenicePartitioner();

  public VenicePartitionFinder(RoutingDataRepository dataRepository){
    this.dataRepository = dataRepository;
  }

  /***
   *
   * @param resourceName
   * @param partitionKey
   * @return partition Name, ex "store_v3_5"
   */
  @Override
  public String findPartitionName(String resourceName, RouterKey partitionKey) {
    int partitionId = findPartitionNumber(resourceName, partitionKey);
    return HelixUtils.getPartitionName(resourceName, partitionId);
  }

  public int findPartitionNumber(String resourceName, RouterKey partitionKey){
    return partitioner.getPartitionId(partitionKey.getBytes(), getNumPartitions(resourceName));
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
}
