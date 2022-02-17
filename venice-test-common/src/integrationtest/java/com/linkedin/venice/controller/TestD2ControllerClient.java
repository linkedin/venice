package com.linkedin.venice.controller;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestD2ControllerClient {
  private static final String CLUSTER_NAME = Utils.getUniqueString("test-cluster");

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testD2ControllerClientEnd2End() {
    D2Client d2Client = null;
    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker(zkServer);
        VeniceControllerWrapper controllerWrapper =
        ServiceFactory.getVeniceController(new String[]{CLUSTER_NAME}, kafkaBrokerWrapper, 1, 10, 0, 1, null, null, true, true, new Properties())) {
      String zkAddress = kafkaBrokerWrapper.getZkAddress();
      D2TestUtils.setupD2Config(zkAddress, false, D2TestUtils.CONTROLLER_CLUSTER_NAME, D2TestUtils.CONTROLLER_SERVICE_NAME, false);

      d2Client = D2TestUtils.getAndStartD2Client(zkAddress);
      String d2ServiceName = D2TestUtils.CONTROLLER_SERVICE_NAME;

      try (D2ControllerClient d2ControllerClient = new D2ControllerClient(d2ServiceName, CLUSTER_NAME, d2Client)) {
        // Test store creation
        String storeName = Utils.getUniqueString("test_store");
        String schema = "\"string\"";
        d2ControllerClient.createNewStore(storeName, "test_owner", schema, schema);

        StoreResponse store = d2ControllerClient.getStore(storeName);
        Assert.assertEquals(store.getName(), storeName);
        Assert.assertEquals(store.getCluster(), CLUSTER_NAME);

        // Test cluster discovery
        D2ServiceDiscoveryResponse discoveryResponse = D2ControllerClient.discoverCluster(d2Client, d2ServiceName, storeName);
        Assert.assertEquals(discoveryResponse.getD2Service(), D2TestUtils.DEFAULT_TEST_SERVICE_NAME);
      }
    } finally {
      if (null != d2Client) {
        D2ClientUtils.shutdownClient(d2Client);
      }
    }
  }

  /**
   * TODO: Remove the below unit test after controller ACL migration
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testHelperFunctionToConvertUrl() throws MalformedURLException {
    URL testUrl = new URL("http://localhost:1576");
    Assert.assertEquals(D2ControllerClient.convertToSecureUrl(testUrl, 1578).toString(), "https://localhost:1578");
  }
}
