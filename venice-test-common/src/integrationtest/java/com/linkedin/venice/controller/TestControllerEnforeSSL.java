package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


public class TestControllerEnforeSSL {
  private static final String CLUSTER_NAME = Utils.getUniqueString("test-cluster");
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "\"string\"";

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testInsecureRouteFailWhenEnforcingSSL() {
    /**
     * Once controller enforce SSL, all routes except cluster/leader controller discovery in insecure port will fail.
     */
    Properties extraProperties = new Properties();
    extraProperties.setProperty(CONTROLLER_ENFORCE_SSL, "true");

    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServer);
         VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceChildController(
             new String[]{CLUSTER_NAME}, kafkaBrokerWrapper, 1, 10, 0, 1,
             null, false, false, extraProperties);
         ControllerClient controllerClient = ControllerClient.constructClusterControllerClient(CLUSTER_NAME, controllerWrapper.getControllerUrl())) {
      TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> controllerWrapper.isLeaderController(CLUSTER_NAME));

      /**
       * Add a test store through backend API directly without going though Controller listener service ({@link com.linkedin.venice.controller.server.AdminSparkServer}).
       */
      Admin admin = controllerWrapper.getVeniceAdmin();
      String storeName = Utils.getUniqueString("test");
      admin.createStore(CLUSTER_NAME, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);

      /**
       * leader controller discovery should still work.
       */
      try {
        controllerClient.getLeaderControllerUrl();
      } catch (Exception e) {
        Assert.fail("Leader controller discover should still work after enforcing SSL.");
      }

      /**
       * All other routes like getStore, updateStore, etc. should fail.
       */
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.isError(), true);
      ControllerResponse updateResponse = controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionCount(100));
      Assert.assertEquals(updateResponse.isError(), true);

      /**
       * Cluster discovery should still work; explicitly put the test after getStore and updateStore to confirm that
       * Controller listener service doesn't crash if it "halts" some requests.
       */
      D2ServiceDiscoveryResponse clusterDiscovery = controllerClient.discoverCluster(storeName);
      Assert.assertEquals(clusterDiscovery.getCluster(), CLUSTER_NAME);
    }
  }
}
