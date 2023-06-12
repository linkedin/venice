package com.linkedin.venice.controller;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestD2ControllerClient {
  private static final String CLUSTER_NAME = Utils.getUniqueString("test-cluster");

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testD2ControllerClientEnd2End() {
    D2Client d2Client = null;
    String clusterD2Service = "d2_service";
    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        PubSubBrokerWrapper pubSubBrokerWrapper =
            ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer).build());
        VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(
            new VeniceControllerCreateOptions.Builder(CLUSTER_NAME, zkServer, pubSubBrokerWrapper).replicationFactor(1)
                .partitionSize(10)
                .rebalanceDelayMs(0)
                .minActiveReplica(1)
                .sslToKafka(true)
                .d2Enabled(true)
                .clusterToD2(Collections.singletonMap(CLUSTER_NAME, clusterD2Service))
                .build())) {
      D2TestUtils.setupD2Config(
          zkServer.getAddress(),
          false,
          VeniceControllerWrapper.D2_CLUSTER_NAME,
          VeniceControllerWrapper.D2_SERVICE_NAME);

      d2Client = D2TestUtils.getAndStartD2Client(zkServer.getAddress());
      String d2ServiceName = VeniceControllerWrapper.D2_SERVICE_NAME;

      try (D2ControllerClient d2ControllerClient = new D2ControllerClient(d2ServiceName, CLUSTER_NAME, d2Client)) {
        // Test store creation
        String storeName = Utils.getUniqueString("test_store");
        String schema = "\"string\"";
        d2ControllerClient.createNewStore(storeName, "test_owner", schema, schema);

        StoreResponse store = d2ControllerClient.getStore(storeName);
        Assert.assertEquals(store.getName(), storeName);
        Assert.assertEquals(store.getCluster(), CLUSTER_NAME);

        // Test cluster discovery
        D2ServiceDiscoveryResponse discoveryResponse =
            D2ControllerClient.discoverCluster(d2Client, d2ServiceName, storeName);
        Assert.assertEquals(discoveryResponse.getD2Service(), clusterD2Service);
      }
    } finally {
      if (d2Client != null) {
        D2ClientUtils.shutdownClient(d2Client);
      }
    }
  }
}
