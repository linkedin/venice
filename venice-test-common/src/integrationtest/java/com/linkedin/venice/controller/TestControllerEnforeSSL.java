package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.utils.TestUtils;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


public class TestControllerEnforeSSL {
  private static final String CLUSTER_NAME = TestUtils.getUniqueString("test-cluster");
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "\"string\"";

  @Test
  public void testInsecureRouteFailWhenEnforcingSSL() {
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    Properties extraProperties = new Properties();
    /**
     * Once controller enforce SSL, all routes except cluster/master controller discovery in insecure port will fail.
     */
    extraProperties.setProperty(CONTROLLER_ENFORCE_SSL, "true");
    VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(new String[]{CLUSTER_NAME}, kafkaBrokerWrapper,
        1, 10, 0, 1, null, null, false, false, extraProperties);

    /**
     * Add a test store through backend API directly without going though Controller listener service ({@link com.linkedin.venice.controller.server.AdminSparkServer}).
     */
    Admin admin = controllerWrapper.getVeniceAdmin();
    String storeName = TestUtils.getUniqueString("test");
    admin.addStore(CLUSTER_NAME, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);

    /**
     * Build a ControllerClient with the insecure controller url.
     */
    ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, controllerWrapper.getControllerUrl());

    /**
     * Master controller discovery should still work.
     */
    try {
      controllerClient.getMasterControllerUrl();
    } catch (Exception e) {
      Assert.fail("Master controller discover should still work after enforcing SSL.");
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
