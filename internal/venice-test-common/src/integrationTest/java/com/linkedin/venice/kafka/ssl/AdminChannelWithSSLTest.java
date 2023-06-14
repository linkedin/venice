package com.linkedin.venice.kafka.ssl;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
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
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdminChannelWithSSLTest {
  /**
   * End-to-end test with SSL enabled
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testEnd2EndWithKafkaSSLEnabled() {
    Utils.thisIsLocalhost();
    String clusterName = "test-cluster";
    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        PubSubBrokerWrapper pubSubBrokerWrapper =
            ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer).build());
        VeniceControllerWrapper childControllerWrapper = ServiceFactory.getVeniceController(
            new VeniceControllerCreateOptions.Builder(clusterName, zkServer, pubSubBrokerWrapper).replicationFactor(1)
                .partitionSize(10)
                .rebalanceDelayMs(0)
                .minActiveReplica(1)
                .sslToKafka(true)
                .build());
        ZkServerWrapper parentZk = ServiceFactory.getZkServer();

        VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(
            new VeniceControllerCreateOptions.Builder(clusterName, parentZk, pubSubBrokerWrapper)
                .childControllers(new VeniceControllerWrapper[] { childControllerWrapper })
                .sslToKafka(true)
                .build())) {
      String secureControllerUrl = controllerWrapper.getSecureControllerUrl();
      // Adding store
      String storeName = "test_store";
      String owner = "test_owner";
      String keySchemaStr = "\"long\"";
      String valueSchemaStr = "\"string\"";

      try (ControllerClient controllerClient =
          new ControllerClient(clusterName, secureControllerUrl, Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
        controllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);
        TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
          MultiStoreResponse response = controllerClient.queryStoreList(false);
          Assert.assertFalse(response.isError());
          String[] stores = response.getStores();
          Assert.assertEquals(stores.length, 1);
          Assert.assertEquals(stores[0], storeName);
        });
      }

      try (ControllerClient childControllerClient = new ControllerClient(
          clusterName,
          childControllerWrapper.getSecureControllerUrl(),
          Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
        // Child controller is talking SSL to Kafka
        TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
          MultiStoreResponse response = childControllerClient.queryStoreList(false);
          Assert.assertFalse(response.isError());
          String[] stores = response.getStores();
          Assert.assertEquals(stores.length, 1);
          Assert.assertEquals(stores[0], storeName);
        });
      }
    }
  }
}
