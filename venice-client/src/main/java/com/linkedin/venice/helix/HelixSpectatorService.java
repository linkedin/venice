package com.linkedin.venice.helix;


import com.linkedin.venice.service.AbstractVeniceService;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;


/**
 * Venice Spectator Service
 */
public class HelixSpectatorService extends AbstractVeniceService {

  private static final String VENICE_SPECTATOR_SERVICE_NAME = "venice-spectator-service";
  private HelixManager manager;
  private String clusterName;
  private HelixRoutingDataRepository repository;

  /*
  Create a com.linkedin.venice.helix.PartitionLookup and hold onto a reference to it.  Pass that object to the
  HelixSpectatorService constructor.  Once you start the service, you will be able to query the PartitionLookup
  for the node responsible for a partition.
   */
  public HelixSpectatorService(String zkAddress, String clusterName, String instanceName) {
    super(VENICE_SPECTATOR_SERVICE_NAME);
    manager = new ZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkAddress);
    this.repository = new HelixRoutingDataRepository(manager);
    this.clusterName = clusterName;
  }

  @Override
  public void startInner() {
    try {
      manager.connect();
      repository.refresh();
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

  public HelixRoutingDataRepository getRoutingDataRepository() {
    return this.repository;
  }
}
