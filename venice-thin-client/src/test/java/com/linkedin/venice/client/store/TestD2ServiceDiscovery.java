package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MockVeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestD2ServiceDiscovery {
  @Test
  public void testD2ServiceDiscovery() {
    String storeName = "test";
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);
    try (ZkServerWrapper zk = ServiceFactory.getZkServer()) {
      // Set up d2 config before announcing
      D2TestUtils.setupD2Config(zk.getAddress(), false);
      // Start a mock server which will serve for the d2 service.
      try (MockVeniceRouterWrapper router =
          ServiceFactory.getMockVeniceRouter(zk.getAddress(), false, new Properties())) {
        // Set up client config to use the d2 service that router serving for
        clientConfig.setD2ServiceName(router.getRouterD2Service()).setVeniceURL(zk.getAddress());
        // Find the d2 service for that store.
        try (D2TransportClient client = new D2TransportClient(router.getRouterD2Service(), clientConfig)) {
          String d2ServiceName = new D2ServiceDiscovery().find(client, storeName).getD2Service();
          Assert.assertEquals(d2ServiceName, router.getD2ServiceNameForCluster(router.getClusterName()),
              "Should find the correct d2 service associated with the given cluster.");
        }
      }
    }
  }
}
