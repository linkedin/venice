package com.linkedin.venice.controller.server;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;

import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * A common base class to provide setup and teardown routines to be used in venice AdminSparkServer related test cases.
 */
public class AbstractTestAdminSparkServer {
  protected static final int TEST_TIMEOUT = 5 * Time.MS_PER_MINUTE;
  protected static final int STORAGE_NODE_COUNT = 1;

  protected VeniceTwoLayerMultiRegionMultiClusterWrapper venice;
  protected ControllerClient controllerClient;
  protected VeniceControllerWrapper parentController;
  protected ControllerClient parentControllerClient;

  public void setUp(
      boolean useParentRestEndpoint,
      Optional<AuthorizerService> authorizerService,
      Properties extraProperties) {
    Properties parentControllerProps = new Properties();
    parentControllerProps.putAll(extraProperties);
    Properties childControllerProps = new Properties();
    childControllerProps.putAll(extraProperties);

    // The cluster does not have router setup
    parentControllerProps.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "false");
    parentControllerProps.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "false");

    childControllerProps.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "false");
    childControllerProps.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "false");

    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(STORAGE_NODE_COUNT)
            .numberOfRouters(0)
            .replicationFactor(1)
            .parentControllerProperties(parentControllerProps)
            .childControllerProperties(childControllerProps)
            .serverProperties(extraProperties)
            .parentAuthorizerService(authorizerService.orElse(null))
            .build());
    parentController = venice.getParentControllers().get(0);
    parentControllerClient = new ControllerClient(venice.getClusterNames()[0], parentController.getControllerUrl());

    String clusterName = venice.getClusterNames()[0];
    if (!useParentRestEndpoint) {
      controllerClient = ControllerClient
          .constructClusterControllerClient(clusterName, venice.getChildRegions().get(0).getControllerConnectString());
    } else {
      controllerClient =
          ControllerClient.constructClusterControllerClient(clusterName, parentController.getControllerUrl());
    }

    TestUtils.waitForNonDeterministicCompletion(
        TEST_TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> parentController.isLeaderController(clusterName));
  }

  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(venice);
  }
}
