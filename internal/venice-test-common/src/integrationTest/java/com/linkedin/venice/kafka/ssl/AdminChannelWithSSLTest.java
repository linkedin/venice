package com.linkedin.venice.kafka.ssl;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
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

    try (VeniceTwoLayerMultiRegionMultiClusterWrapper venice =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
            new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                .numberOfClusters(1)
                .numberOfParentControllers(1)
                .numberOfChildControllers(1)
                .numberOfServers(1)
                .numberOfRouters(1)
                .replicationFactor(1)
                .sslToKafka(true)
                .build())) {

      String clusterName = venice.getClusterNames()[0];
      VeniceControllerWrapper childControllerWrapper = venice.getChildRegions().get(0).getLeaderController(clusterName);

      String parentSecureControllerUrl = venice.getParentControllers().get(0).getSecureControllerUrl();
      // Adding store
      String storeName = "test_store";
      String owner = "test_owner";
      String keySchemaStr = "\"long\"";
      String valueSchemaStr = "\"string\"";

      try (ControllerClient controllerClient = new ControllerClient(
          clusterName,
          parentSecureControllerUrl,
          Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
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
