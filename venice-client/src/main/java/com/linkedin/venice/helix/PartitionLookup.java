package com.linkedin.venice.helix;

import com.linkedin.venice.utils.HostPort;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableProvider;


/**
 * Hold onto a reference to this object in order to use Helix to lookup which host has data for a partition
 * The PartitionLookup object should get passed to the HelixSpectatorService
 */
public class PartitionLookup {
  private RoutingTableProvider routingTableProvider;
  private Map<String, HostPort> instanceMap = new ConcurrentHashMap<>();

  public PartitionLookup(){
    routingTableProvider = new RoutingTableProvider();
  }

  public RoutingTableProvider getRoutingTableProvider(){
    return routingTableProvider;
  }

  public void updateLiveInstances(List<LiveInstance> instances){
    Map<String, HostPort> newInstanceMap = new ConcurrentHashMap<>();
    for (LiveInstance i : instances){
      ZNRecord rec = i.getRecord();
      HostPort hostport = new HostPort(
          rec.getSimpleField(LiveInstanceProperty.HOST),
          rec.getSimpleField(LiveInstanceProperty.PORT)
      );
      newInstanceMap.put(rec.getId(), hostport);
    }

    //Is this update atomic or should it really be wrapped in a synchronized block?
    instanceMap = newInstanceMap;
  }

  //TODO: this should work with a list of instances, instead of blindly grabbing the first
  public List<String> getParticipantsForPartition(String storeName, String partitionNumber){
    List<InstanceConfig> instances =
        routingTableProvider.getInstances(storeName, storeName + "_" + partitionNumber, State.ONLINE_STATE);
    return instances.stream().map(InstanceConfig::getId).collect(Collectors.toList());
  }

  public List<HostPort> getHostPortForPartition(String storeName, String partitionNumber){
    List<String> participantIds = getParticipantsForPartition(storeName, partitionNumber);
    return participantIds.stream().map(id -> instanceMap.get(id)).collect(Collectors.toList());
  }

}
