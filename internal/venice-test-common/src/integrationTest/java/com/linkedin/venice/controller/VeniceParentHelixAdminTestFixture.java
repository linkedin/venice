package com.linkedin.venice.controller;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.Utils;


/**
 * Shared test fixture for VeniceParentHelixAdmin integration tests.
 * Encapsulates VeniceTwoLayerMultiRegionMultiClusterWrapper + controller URL + cluster name.
 */
public class VeniceParentHelixAdminTestFixture implements AutoCloseable {
  private final VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private final VeniceClusterWrapper venice;
  private final String clusterName;

  public VeniceParentHelixAdminTestFixture() {
    Utils.thisIsLocalhost();
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];
    venice = multiRegionMultiClusterWrapper.getChildRegions().get(0).getClusters().get(clusterName);
  }

  public VeniceTwoLayerMultiRegionMultiClusterWrapper getMultiRegionMultiClusterWrapper() {
    return multiRegionMultiClusterWrapper;
  }

  public VeniceClusterWrapper getVenice() {
    return venice;
  }

  public String getClusterName() {
    return clusterName;
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }
}
