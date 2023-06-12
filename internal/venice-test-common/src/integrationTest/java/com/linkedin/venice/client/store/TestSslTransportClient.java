package com.linkedin.venice.client.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.venice.client.store.transport.HttpsTransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.integration.utils.MockVeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.SslUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestSslTransportClient {
  @DataProvider(name = "leaderControllerPathProvider")
  public static Object[][] dataProvider() {
    // go/inclusivecode deprecated (alias="leader_controller")
    return new Object[][] { { "master_controller" }, { "leader_controller" } };
  }

  @Test(dataProvider = "leaderControllerPathProvider")
  public void testSslTransportClientCanTalkToRouter(String leaderControllerPath)
      throws ExecutionException, InterruptedException, IOException {
    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        MockVeniceRouterWrapper router =
            ServiceFactory.getMockVeniceRouter(zkServer.getAddress(), true, new Properties())) {
      String routerSslUrl = "https://" + router.getHost() + ":" + router.getSslPort();
      try (HttpsTransportClient client =
          new HttpsTransportClient(routerSslUrl, 0, 0, false, SslUtils.getVeniceLocalSslFactory())) {

        TransportClientResponse transportClientResponse = client.get(leaderControllerPath).get();
        byte[] response = transportClientResponse.getBody();

        String responseJson = new String(response, StandardCharsets.UTF_8);
        Map<String, String> responseMap =
            ObjectMapperFactory.getInstance().readValue(responseJson, new TypeReference<HashMap<String, String>>() {
            });
        Assert.assertEquals(responseMap.get("cluster"), router.getClusterName());
        Assert.assertEquals(responseMap.get("url"), "http://localhost:1234");
      }
    }
  }
}
