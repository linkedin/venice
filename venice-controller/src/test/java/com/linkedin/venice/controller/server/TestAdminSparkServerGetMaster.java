package com.linkedin.venice.controller.server;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.controller.server.AdminSparkServer.mapper;


/**
 * Separate test case from TestAdminParkServer because controller client is not required for this test.
 */
public class TestAdminSparkServerGetMaster {
  private String cluster = "test-master-cluster";
  private VeniceControllerWrapper veniceControllerWrapper;
  private KafkaBrokerWrapper kafkaBrokerWrapper;

  @BeforeMethod
  public void setup() {
    kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    veniceControllerWrapper = ServiceFactory.getVeniceController(cluster, kafkaBrokerWrapper);
  }

  @AfterMethod
  public void cleanup() {
    veniceControllerWrapper.close();
    kafkaBrokerWrapper.close();
  }

  @Test
  public void testGetMasterController()
      throws IOException {
    String controllerUrl = veniceControllerWrapper.getControllerUrl();

    HttpClient client = HttpClients.createDefault();
    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, cluster));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);

    HttpGet get = new HttpGet(controllerUrl + ControllerApiConstants.GET_MASTER_CONTROLLER_PATH + "?" + queryString);
    HttpResponse response = client.execute(get);
    String jsonStr;
    try (InputStream bodyStream = response.getEntity().getContent()) {
      jsonStr = IOUtils.toString(bodyStream);
    }
    MasterControllerResponse responseObject = mapper.readValue(jsonStr, MasterControllerResponse.class);

    String masterControllerUrl = responseObject.getUrl();

    Assert.assertEquals(masterControllerUrl, "http://" + Utils.getHostName() + ":" + veniceControllerWrapper.getPort(),
        "Can not find correct master controller url.");
  }
}
