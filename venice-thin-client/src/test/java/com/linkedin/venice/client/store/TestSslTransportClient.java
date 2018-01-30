package com.linkedin.venice.client.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.HttpsTransportClient;
import com.linkedin.venice.integration.utils.MockVeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.utils.SslUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSslTransportClient {

  @Test
  public void SslTransportClientCanTalkToRouter() throws ExecutionException, InterruptedException, IOException {
    MockVeniceRouterWrapper router = ServiceFactory.getMockVeniceRouter(ServiceFactory.getZkServer().getAddress(), true, new Properties());
    String routerSslUrl = "https://" + router.getHost() + ":" + router.getSslPort();
    HttpsTransportClient client = new HttpsTransportClient(routerSslUrl, SslUtils.getLocalSslFactory());

    TransportClientResponse transportClientResponse = client.get("/master_controller").get();
    byte[] response = transportClientResponse.getBody();

    String responseJson = new String(response, StandardCharsets.UTF_8);
    Map<String, String> responseMap = new ObjectMapper().readValue(responseJson, new TypeReference<HashMap<String, String>>(){});
    Assert.assertEquals(responseMap.get("cluster"), router.getClusterName());
    Assert.assertEquals(responseMap.get("url"), "http://localhost:1234");

    client.close();
  }
}
