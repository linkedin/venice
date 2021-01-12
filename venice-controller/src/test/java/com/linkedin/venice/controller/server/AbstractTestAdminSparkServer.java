package com.linkedin.venice.controller.server;

import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;


/**
 * A common base class to provide setup and teardown routines to be used in venice AdminSparkServer related test cases.
 */
public class AbstractTestAdminSparkServer {
  static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;
  static final int STORAGE_NODE_COUNT = 1;

  VeniceClusterWrapper cluster;
  ControllerClient controllerClient;
  VeniceControllerWrapper parentController;
  ZkServerWrapper parentZk;

  public void setUp(boolean useParentRestEndpoint, Optional<AuthorizerService> authorizerService) {
    cluster = ServiceFactory.getVeniceCluster(1, STORAGE_NODE_COUNT, 0);

    parentZk = ServiceFactory.getZkServer();
    parentController =
        ServiceFactory.getVeniceParentController(cluster.getClusterName(), parentZk.getAddress(), cluster.getKafka(),
            new VeniceControllerWrapper[]{cluster.getMasterVeniceController()}, false, authorizerService);

    if (!useParentRestEndpoint) {
      controllerClient = new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs());
    } else {
      controllerClient = new ControllerClient(cluster.getClusterName(), parentController.getControllerUrl());
    }

    TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
        () -> parentController.isMasterController(cluster.getClusterName()));
  }

  public void tearDown() {
    IOUtils.closeQuietly(controllerClient);
    IOUtils.closeQuietly(cluster);
    IOUtils.closeQuietly(parentController);
    IOUtils.closeQuietly(parentZk);
  }
}
