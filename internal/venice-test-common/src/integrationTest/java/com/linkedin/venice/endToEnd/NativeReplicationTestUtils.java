package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


public class NativeReplicationTestUtils {
  private static final Logger LOGGER = LogManager.getLogger(NativeReplicationTestUtils.class);

  public static void verifyDCConfigNativeRepl(
      List<ControllerClient> controllerClients,
      String storeName,
      boolean enabled,
      Optional<String> nativeReplicationSourceOptional) {
    for (ControllerClient controllerClient: controllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertEquals(
            storeResponse.getStore().isNativeReplicationEnabled(),
            enabled,
            "The native replication config does not match.");
        nativeReplicationSourceOptional.ifPresent(
            s -> Assert.assertEquals(
                storeResponse.getStore().getNativeReplicationSourceFabric(),
                s,
                "Native replication source doesn't match"));
      });
    }
  }

  public static void verifyDCConfigNativeRepl(
      List<ControllerClient> controllerClients,
      String storeName,
      boolean enabled) {
    verifyDCConfigNativeRepl(controllerClients, storeName, enabled, Optional.empty());
  }

  public static void verifyIncrementalPushData(
      List<VeniceMultiClusterWrapper> childDatacenters,
      String clusterName,
      String storeName,
      int maxKey,
      int valueMultiplier) throws Exception {
    // Verify the data in the first child fabric which consumes remotely
    for (int idx = 0; idx < childDatacenters.size(); idx++) {
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(idx);
      LOGGER.info("verifying dc-{}", idx);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client = ClientFactory
          .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= maxKey; ++i) {
          String expected = i <= 50 ? "test_name_" + i : "test_name_" + (i * valueMultiplier);
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    }
  }
}
