package com.linkedin.venice.helix;


import com.linkedin.venice.service.AbstractVeniceService;
import java.util.List;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.LiveInstance;


/**
 * Venice Spectator Service
 */
public class HelixSpectatorService extends AbstractVeniceService {

  private static final String VENICE_SPECTATOR_SERVICE_NAME = "venice-spectator-service";
  private HelixManager manager;
  private PartitionLookup partitionLookup;
  private String clusterName;

  /*
  Create an org.apache.helix.spectator.RoutingTableProvider and hold onto a reference to it.  Pass that object to the
  HelixSpectatorService constructor.  Once you start the service, you will be able to query the RoutingTableProvider
  for the node responsible for a partition.

  Example: instances = routingTableProvider.getInstances("store-name", "partition-name", "ONLINE");
   instances = routingTableProvider.getInstances("default_topic", "default_topic_0", "ONLINE");

  We may decide to wrap the RoutingTableProvider with a Venice object to simplify the code of doing node lookups.
   */
  public HelixSpectatorService(String zkAddress, String clusterName, String instanceName, PartitionLookup partitionLookup) {
    super(VENICE_SPECTATOR_SERVICE_NAME);
    /*
    HelixConnection connection = new ZkHelixConnection(zkAddress);
    connection.connect();
    ClusterId clusterId = ClusterId.from(clusterName);
    ParticipantId participantId = ParticipantId.from(participantName);
    */
    manager = new ZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkAddress);
    this.partitionLookup = partitionLookup;
    this.clusterName = clusterName;
  }

  @Override
  public void startInner() {
    try {
      manager.connect();
      manager.addExternalViewChangeListener(partitionLookup.getRoutingTableProvider());
      LiveInstanceChangeListener listener = new LiveInstanceChangeListener() {
        @Override
        public void onLiveInstanceChange(List<LiveInstance> liveInstances,
            NotificationContext changeContext) {
          if (changeContext.getType() != NotificationContext.Type.FINALIZE) {
            refreshPartitionLookup();
          }
        }
      };
      manager.addLiveInstanceChangeListener(listener);
    } catch (Exception e) {
      e.printStackTrace();
      //TODO: Deal with a failed connection to Helix
    }
  }

  @Override
  public void stopInner() {
    if(manager != null) {
      manager.disconnect();
    }
  }

  private void refreshPartitionLookup() {
    PropertyKey.Builder propertyKeyBuilder = new PropertyKey.Builder(clusterName);
    HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
    List<LiveInstance> liveInstances =
        helixDataAccessor.getChildValues(propertyKeyBuilder.liveInstances());
    partitionLookup.updateLiveInstances(liveInstances);
  }

}
