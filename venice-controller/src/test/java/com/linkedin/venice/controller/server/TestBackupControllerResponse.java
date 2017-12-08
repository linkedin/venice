package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.controllerapi.ControllerRoute.CREATE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.JOB;

public class TestBackupControllerResponse {
  @Test
  public void backupControllerThrows421()
      throws Exception {

    String clusterName = "backupControllerThrows421";
    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();



    //Start 2 controllers.
    VeniceControllerWrapper controller1 = ServiceFactory.getVeniceController(clusterName, kafka);
    VeniceControllerWrapper controller2 = ServiceFactory.getVeniceController(clusterName, kafka);

    Thread.sleep(2000);
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
    httpClient.start();
    // Find the url for slave controller
    String controllerUrl = controller1.isMasterController(clusterName)?controller2.getControllerUrl():controller1.getControllerUrl();

    final HttpPost post = new HttpPost(controllerUrl + CREATE_VERSION.getPath());
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    post.setEntity(new UrlEncodedFormEntity(params));

    HttpResponse response = httpClient.execute(post, null).get();
    int responseStatus = response.getStatusLine().getStatusCode();
    Assert.assertEquals(responseStatus, HttpConstants.SC_MISDIRECTED_REQUEST);

    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + JOB.getPath() + "?" + queryString);
    HttpResponse response2 = httpClient.execute(get, null).get();

    int responseStatus2 = response2.getStatusLine().getStatusCode();
    Assert.assertEquals(responseStatus2, HttpConstants.SC_MISDIRECTED_REQUEST);

    httpClient.close();
    controller1.close();
    controller2.close();
    kafka.close();
  }
}
