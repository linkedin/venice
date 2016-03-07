package com.linkedin.venice.router;

import com.linkedin.ddsstorage.router.api.PartitionFinder;
import com.linkedin.venice.kafka.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.kafka.partitioner.VenicePartitioner;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.List;
import java.util.stream.Collectors;


public class VenicePartitionFinder implements PartitionFinder<RouterKey> {

  private RoutingDataRepository dataRepository;
  //TODO: the VenicePartitioner creates a kafka dependency.  Can we avoid kafka dependency in the venice router
  private final VenicePartitioner partitioner = new DefaultVenicePartitioner();

  public VenicePartitionFinder(RoutingDataRepository dataRepository){
    this.dataRepository = dataRepository;
  }

  @Override
  public String findPartitionName(String resourceName, RouterKey partitionKey) {
    KafkaKey kafkaKey = new KafkaKey(null, partitionKey.getBytes());
    int partitionId = partitioner.getPartitionId(kafkaKey, getNumPartitions(resourceName));
    return Partition.getPartitionName(resourceName, partitionId);
  }

  @Override
  public List<String> getAllPartitionNames(String resourceName) {
    return dataRepository.getPartitions(resourceName).values()
        .stream()
        .map(p -> Partition.getPartitionName(resourceName, p.getId()))
        .collect(Collectors.toList());
  }

  @Override
  public int getNumPartitions(String resourceName) {
    return dataRepository.getNumberOfPartitions(resourceName);
  }
}
