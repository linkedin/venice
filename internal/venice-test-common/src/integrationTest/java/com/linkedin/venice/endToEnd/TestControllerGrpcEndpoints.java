package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerGrpcEndpoints {
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfRouters(1)
        .numberOfServers(2)
        .numberOfControllers(1)
        .sslToKafka(false)
        .sslToStorageNodes(false)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test
  public void testGrpcEndpoints() {
    String storeName = Utils.getUniqueString("store");
    // String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    try (ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs())) {
      TestUtils.assertCommand(controllerClient.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, "\"string\""));
      Thread.sleep(10);
      System.out.println();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
