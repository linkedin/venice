package com.linkedin.venice.controller.server;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;

import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
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
  protected static final int TEST_TIMEOUT = 300 * Time.MS_PER_SECOND;
  protected static final int STORAGE_NODE_COUNT = 1;

  protected VeniceClusterWrapper cluster;
  protected ControllerClient controllerClient;
  protected VeniceControllerWrapper parentController;
  protected ZkServerWrapper parentZk;

  public void setUp(
      boolean useParentRestEndpoint,
      Optional<AuthorizerService> authorizerService,
      Properties extraProperties) {
    cluster = ServiceFactory.getVeniceCluster(1, STORAGE_NODE_COUNT, 0, 1, 100, false, false, extraProperties);

    parentZk = ServiceFactory.getZkServer();
    // The cluster does not have router setup
    extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "false");
    extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "false");
    VeniceControllerCreateOptions options =
        new VeniceControllerCreateOptions.Builder(cluster.getClusterName(), parentZk, cluster.getKafka())
            .replicationFactor(1)
            .childControllers(new VeniceControllerWrapper[] { cluster.getLeaderVeniceController() })
            .extraProperties(extraProperties)
            .authorizerService(authorizerService.orElse(null))
            .build();
    parentController = ServiceFactory.getVeniceController(options);

    if (!useParentRestEndpoint) {
      controllerClient =
          ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
    } else {
      controllerClient = ControllerClient
          .constructClusterControllerClient(cluster.getClusterName(), parentController.getControllerUrl());
    }

    TestUtils.waitForNonDeterministicCompletion(
        TEST_TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> parentController.isLeaderController(cluster.getClusterName()));
  }

  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(parentController);
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(cluster);
    Utils.closeQuietlyWithErrorLogged(parentZk);
  }
}
