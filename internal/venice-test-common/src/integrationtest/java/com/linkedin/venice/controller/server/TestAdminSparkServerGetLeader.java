package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.server.AdminSparkServer.OBJECT_MAPPER;
import static com.linkedin.venice.controllerapi.ControllerRoute.LEADER_CONTROLLER;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Separate test case from TestAdminParkServer because controller client is not required for this test.
 */
public class TestAdminSparkServerGetLeader {
  private String cluster = "test-primary-cluster";
  private VeniceControllerWrapper veniceControllerWrapper;
  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private ZkServerWrapper zkServer;

  @BeforeMethod
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
    pubSubBrokerWrapper =
        ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer).build());
    veniceControllerWrapper = ServiceFactory
        .getVeniceController(new VeniceControllerCreateOptions.Builder(cluster, zkServer, pubSubBrokerWrapper).build());
  }

  @AfterMethod
  public void cleanUp() {
    veniceControllerWrapper.close();
    pubSubBrokerWrapper.close();
    zkServer.close();
  }

  @Test
  public void testGetLeaderController() throws IOException {
    String controllerUrl = veniceControllerWrapper.getControllerUrl();

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      List<NameValuePair> queryParams = new ArrayList<>();
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, cluster));
      String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);

      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        try {
          HttpGet get = new HttpGet(controllerUrl + LEADER_CONTROLLER.getPath() + "?" + queryString);
          HttpResponse response = client.execute(get);
          String jsonStr;
          try (InputStream bodyStream = response.getEntity().getContent()) {
            jsonStr = IOUtils.toString(bodyStream);
          }

          LeaderControllerResponse responseObject = OBJECT_MAPPER.readValue(jsonStr, LeaderControllerResponse.class);

          String leaderControllerUrl = responseObject.getUrl();

          Assert.assertEquals(
              leaderControllerUrl,
              "http://" + Utils.getHostName() + ":" + veniceControllerWrapper.getPort(),
              "Can not find correct leader controller url.");
        } catch (IOException e) {
          throw new VeniceException(e);
        }
      });
    }
  }
}
