package com.linkedin.venice.controller;

import com.linkedin.venice.config.VeniceStorePartitionInformation;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.log4j.Logger;

/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(VeniceControllerService.class.getName());
  private static final String VENICE_CONTROLLER_SERVICE_NAME = "venice-controller-service";

  private final Admin admin;
  private final String clusterName;

  private final Map<String, VeniceStorePartitionInformation> storeToPartitionInformationMap;

  public VeniceControllerService (String zkAddress, String clusterName, String controllerName,
      Map<String, VeniceStorePartitionInformation> storeToPartitionInformationMap) {
    super(VENICE_CONTROLLER_SERVICE_NAME);

    this.admin = new VeniceHelixAdmin(controllerName, zkAddress);
    this.clusterName = clusterName;
    this.storeToPartitionInformationMap = storeToPartitionInformationMap;
  }

  @Override
  public void startInner() {
    addStoresAsHelixResources();
    admin.start(clusterName);
  }

  @Override
  public void stopInner() {
    admin.stop(clusterName);
  }

  /**
   * Adds Venice stores as a Helix Resource.
   * TODO: Optimize this and do this only for non existing stores.
   */
  private void addStoresAsHelixResources() {

    for(VeniceStorePartitionInformation storePartitionInformation: storeToPartitionInformationMap.values()) {
      admin.addStore(clusterName , storePartitionInformation);
    }
  }
}
