package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENFORCE_SSL;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestControllerEnforceSSL {
  private static final String CLUSTER_NAME = Utils.getUniqueString("test-cluster");
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "\"string\"";

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testInsecureRouteFailWhenEnforcingSSL() {
    Utils.thisIsLocalhost();
    /**
     * Once controller enforce SSL, all routes except cluster/leader controller discovery in insecure port will fail.
     */
    Properties extraProperties = new Properties();
    extraProperties.setProperty(CONTROLLER_ENFORCE_SSL, "true");

    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        PubSubBrokerWrapper pubSubBrokerWrapper =
            ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer).build());
        VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(
            new VeniceControllerCreateOptions.Builder(CLUSTER_NAME, zkServer, pubSubBrokerWrapper).replicationFactor(1)
                .partitionSize(10)
                .rebalanceDelayMs(0)
                .minActiveReplica(1)
                .sslToKafka(true)
                .extraProperties(extraProperties)
                .build());
        ControllerClient controllerClient =
            ControllerClient.constructClusterControllerClient(CLUSTER_NAME, controllerWrapper.getControllerUrl());
        ControllerClient secureControllerClient = ControllerClient.constructClusterControllerClient(
            CLUSTER_NAME,
            controllerWrapper.getSecureControllerUrl(),
            Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
      TestUtils.waitForNonDeterministicCompletion(
          5,
          TimeUnit.SECONDS,
          () -> controllerWrapper.isLeaderController(CLUSTER_NAME));

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
        secureControllerClient.getLeaderControllerUrl();
        controllerClient.getLeaderControllerUrl();
      } catch (Exception e) {
        Assert.fail("Leader controller discover should still work after enforcing SSL.", e);
      }

      /**
       * All other routes like getStore, updateStore, etc. should fail for non-secure controller clients to http URLs
       * but succeed for controller clients to https URLs
       */
      StoreResponse storeResponseSecure = secureControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponseSecure.isError(), storeResponseSecure.getError());
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertTrue(storeResponse.isError(), storeResponse.getError());

      ControllerResponse updateResponseSecure =
          secureControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionCount(2));
      Assert.assertFalse(updateResponseSecure.isError(), updateResponseSecure.getError());
      ControllerResponse updateResponse =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionCount(2));
      Assert.assertTrue(updateResponse.isError(), updateResponse.getError());

      /**
       * Cluster discovery should still work; explicitly put the test after getStore and updateStore to confirm that
       * Controller listener service doesn't crash if it "halts" some requests.
       */
      D2ServiceDiscoveryResponse clusterDiscovery = controllerClient.discoverCluster(storeName);
      Assert.assertEquals(clusterDiscovery.getCluster(), CLUSTER_NAME);
    }
  }
}
