package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public abstract class AbstractMultiRegionTest {
  protected static final String CLUSTER_NAME = "venice-cluster0";
  protected static final int DEFAULT_NUMBER_OF_REGIONS = 2;
  protected static final int DEFAULT_NUMBER_OF_CLUSTERS = 1;
  protected static final int DEFAULT_NUMBER_OF_SERVERS = 2;
  protected static final int DEFAULT_REPLICATION_FACTOR = 2;
  protected static final int DEFAULT_NUMBER_OF_PARENT_CONTROLLERS = 1;
  protected static final int DEFAULT_NUMBER_OF_CHILD_CONTROLLERS = 1;
  protected static final int DEFAULT_NUMBER_OF_ROUTERS = 1;

  protected VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  protected VeniceControllerWrapper parentController;
  protected List<VeniceMultiClusterWrapper> childDatacenters;
  protected D2Client d2ClientDC0;

  protected int getNumberOfRegions() {
    return DEFAULT_NUMBER_OF_REGIONS;
  }

  protected int getNumberOfClusters() {
    return DEFAULT_NUMBER_OF_CLUSTERS;
  }

  protected int getNumberOfServers() {
    return DEFAULT_NUMBER_OF_SERVERS;
  }

  protected int getReplicationFactor() {
    return DEFAULT_REPLICATION_FACTOR;
  }

  protected boolean isForkServer() {
    return false;
  }

  protected Properties getExtraServerProperties() {
    return new Properties();
  }

  protected Properties getExtraControllerProperties() {
    return new Properties();
  }

  /**
   * Override to provide properties applied only to parent controllers.
   * By default, returns the same as {@link #getExtraControllerProperties()}.
   */
  protected Properties getExtraParentControllerProperties() {
    return getExtraControllerProperties();
  }

  /**
   * Override to provide properties applied only to child controllers.
   * By default, returns the same as {@link #getExtraControllerProperties()}.
   */
  protected Properties getExtraChildControllerProperties() {
    return getExtraControllerProperties();
  }

  protected boolean shouldCreateD2Client() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.putAll(getExtraServerProperties());
    Properties parentControllerProps = new Properties();
    parentControllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, true);
    parentControllerProps.putAll(getExtraParentControllerProperties());
    Properties childControllerProps = new Properties();
    childControllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, true);
    childControllerProps.putAll(getExtraChildControllerProperties());
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(getNumberOfRegions())
            .numberOfClusters(getNumberOfClusters())
            .numberOfParentControllers(DEFAULT_NUMBER_OF_PARENT_CONTROLLERS)
            .numberOfChildControllers(DEFAULT_NUMBER_OF_CHILD_CONTROLLERS)
            .numberOfServers(getNumberOfServers())
            .numberOfRouters(DEFAULT_NUMBER_OF_ROUTERS)
            .replicationFactor(getReplicationFactor())
            .forkServer(isForkServer())
            .parentControllerProperties(parentControllerProps)
            .childControllerProperties(childControllerProps)
            .serverProperties(serverProperties);
    this.multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    this.childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    if (parentControllers.size() != 1) {
      throw new IllegalStateException("Expect only one parent controller. Got: " + parentControllers.size());
    }
    this.parentController = parentControllers.get(0);
    if (shouldCreateD2Client()) {
      this.d2ClientDC0 = new D2ClientBuilder()
          .setZkHosts(multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper().getAddress())
          .setZkSessionTimeout(3, TimeUnit.SECONDS)
          .setZkStartupTimeout(3, TimeUnit.SECONDS)
          .build();
      D2ClientUtils.startClient(d2ClientDC0);
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (d2ClientDC0 != null) {
      D2ClientUtils.shutdownClient(d2ClientDC0);
    }
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  protected VeniceClusterWrapper getClusterDC0() {
    return childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
  }

  protected VeniceClusterWrapper getCluster(int dcIndex) {
    return childDatacenters.get(dcIndex).getClusters().get(CLUSTER_NAME);
  }

  protected String getParentControllerUrl() {
    return parentController.getControllerUrl();
  }
}
