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
  Create a com.linkedin.venice.helix.PartitionLookup and hold onto a reference to it.  Pass that object to the
  HelixSpectatorService constructor.  Once you start the service, you will be able to query the PartitionLookup
  for the node responsible for a partition.
   */
  public HelixSpectatorService(String zkAddress, String clusterName, String instanceName, PartitionLookup partitionLookup) {
    super(VENICE_SPECTATOR_SERVICE_NAME);
    manager = new ZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkAddress);
    this.partitionLookup = partitionLookup;
    this.clusterName = clusterName;
  }

  @Override
  public void startInner() {
    try {
      manager.connect();
      manager.addExternalViewChangeListener(partitionLookup.getRoutingTableProvider());
      manager.addExternalViewChangeListener(partitionLookup.getPartitionCountProvider());
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
      throw new RuntimeException(e);
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
