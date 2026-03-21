package com.linkedin.venice.featurematrix;

import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.setup.ClusterConfigBuilder;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Creates and manages multi-region Venice cluster instances for the feature matrix test.
 *
 * Server, router, and controller configs are set at startup and cannot change per-store.
 * Therefore, test cases are grouped by unique (S, RT, C) tuples, and one cluster is
 * created per unique tuple.
 *
 * Each cluster instance uses VeniceTwoLayerMultiRegionMultiClusterWrapper to support
 * multi-region features (native replication, active-active, target region push).
 */
public class FeatureMatrixClusterSetup {
  private static final Logger LOGGER = LogManager.getLogger(FeatureMatrixClusterSetup.class);

  private static final int NUMBER_OF_REGIONS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int NUMBER_OF_CONTROLLERS = 1;
  private static final int NUMBER_OF_SERVERS = 2;
  private static final int NUMBER_OF_ROUTERS = 1;
  private static final int REPLICATION_FACTOR = 2;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper cluster;
  private final TestCaseConfig representativeConfig;

  public FeatureMatrixClusterSetup(TestCaseConfig representativeConfig) {
    this.representativeConfig = representativeConfig;
  }

  /**
   * Creates the multi-region cluster with Server/Router/Controller configs
   * derived from the representative test case's S/RT/C dimensions.
   */
  public void create() {
    LOGGER.info("Creating multi-region cluster for config key: {}", representativeConfig.getClusterConfigKey());

    Properties serverProps = ClusterConfigBuilder.buildServerProperties(representativeConfig);
    Properties parentControllerProps = ClusterConfigBuilder.buildParentControllerProperties(representativeConfig);
    Properties childControllerProps = ClusterConfigBuilder.buildChildControllerProperties(representativeConfig);

    // TODO: Router properties not yet supported in VeniceMultiRegionClusterCreateOptions.
    // See PICT model comments for details. Will be wired once routerProperties support is added.

    VeniceMultiRegionClusterCreateOptions options =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_REGIONS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(NUMBER_OF_CONTROLLERS)
            .numberOfChildControllers(NUMBER_OF_CONTROLLERS)
            .numberOfServers(NUMBER_OF_SERVERS)
            .numberOfRouters(NUMBER_OF_ROUTERS)
            .replicationFactor(REPLICATION_FACTOR)
            .parentControllerProperties(parentControllerProps)
            .childControllerProperties(childControllerProps)
            .serverProperties(serverProps)
            .build();

    cluster = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(options);
    LOGGER.info("Multi-region cluster created successfully");
  }

  /**
   * Returns the cluster wrapper for use in tests.
   */
  public VeniceTwoLayerMultiRegionMultiClusterWrapper getCluster() {
    return cluster;
  }

  public VeniceTwoLayerMultiRegionMultiClusterWrapper getMultiRegionCluster() {
    return cluster;
  }

  /**
   * Returns the parent controller URL.
   */
  public String getParentControllerUrl() {
    return cluster.getParentControllers().get(0).getControllerUrl();
  }

  /**
   * Returns the cluster name (first cluster in the multi-cluster setup).
   */
  public String getClusterName() {
    return cluster.getClusterNames()[0];
  }

  /**
   * Returns a random router URL from the first child cluster.
   */
  public String getRandomRouterURL() {
    return cluster.getChildRegions().get(0).getClusters().get(getClusterName()).getRandomRouterURL();
  }

  /**
   * Tears down the cluster.
   */
  public void tearDown() {
    if (cluster != null) {
      LOGGER.info("Tearing down multi-region cluster");
      try {
        cluster.close();
      } catch (Exception e) {
        LOGGER.error("Error tearing down cluster", e);
      }
    }
  }
}
