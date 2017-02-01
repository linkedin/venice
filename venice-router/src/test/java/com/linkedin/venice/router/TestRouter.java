package com.linkedin.venice.router;


import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.client.exceptions.VeniceServerException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroStoreClientFactory;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MockVeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLContext;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by mwise on 3/9/16.
 */
public class TestRouter {

  /**
   * TODO: Make this test more resilient. It is flaky on slower hardware.
   */
  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void testRouterWithD2() throws Exception {
    ZkServerWrapper zk = ServiceFactory.getZkServer();
    D2TestUtils.setupD2Config(zk.getAddress());

    MockVeniceRouterWrapper router = ServiceFactory.getMockVeniceRouter(zk.getAddress());
    D2Client d2Client = D2TestUtils.getAndStartD2Client(zk.getAddress());

    URI requestUri = new URI("d2://" + D2TestUtils.D2_SERVICE_NAME + "/storage/myStore/myKey"); /* D2 client only supports d2:// scheme */
    RestRequest request = new RestRequestBuilder(requestUri).setMethod("get").build();
    RestResponse response;
    try {
      response = d2Client.restRequest(request).get();
    } catch (ExecutionException e){
      if (e.getCause() instanceof RestException){
        response = ((RestException) e.getCause()).getResponse();
      } else {
        throw e;
      }
    }

    Assert.assertEquals(response.getStatus(), HttpStatus.SC_SERVICE_UNAVAILABLE,
      "Router with Mock components should return a 503 Service Unavailable");

    AbstractAvroStoreClient<Object> storeClient = (AbstractAvroStoreClient<Object>) AvroStoreClientFactory.getAndStartAvroGenericStoreClient(
      D2TestUtils.D2_SERVICE_NAME, d2Client, "myStore");

    try {
      byte[] value = storeClient.getRaw("storage/myStore/myKey").get();
      Assert.fail("Router with Mock components should trigger VeniceServerException");
    } catch (ExecutionException e){
      if (e.getCause() instanceof VeniceServerException){
        // expected.
      } else {
        throw e;
      }
    } finally {
      storeClient.close();
      router.close();
      zk.close();
    }
  }

  @Test
  public void testRouterWithSsl() throws ExecutionException, InterruptedException, IOException {
    ZkServerWrapper zk = ServiceFactory.getZkServer();
    MockVeniceRouterWrapper router = ServiceFactory.getMockVeniceRouter(zk.getAddress());
    SSLContext sslContext = TestUtils.getClientSslFactory().getSSLContext();
    SSLIOSessionStrategy sslSessionStrategy = new SSLIOSessionStrategy(sslContext, (s, sslSession) -> {
      return true; //hostname verifier; because the fake router cert doesn't match the hostname 'localhost'
    });
    CloseableHttpAsyncClient httpclient = HttpAsyncClients.custom()
        .setSSLStrategy(sslSessionStrategy)
        .build();
    try {
      httpclient.start();
      String address = router.getHost() + ":" + router.getSslPort();
      HttpGet request = new HttpGet("https://" + address + ControllerRoute.MASTER_CONTROLLER.getPath());
      HttpResponse response = httpclient.execute(request, null).get();
      String jsonContent = IOUtils.toString(response.getEntity().getContent());
      MasterControllerResponse controllerResponse = new ObjectMapper().readValue(jsonContent, MasterControllerResponse.class);

      Assert.assertEquals(controllerResponse.getCluster(), router.getClusterName());
    } finally {
      httpclient.close();
    }
    router.close();
    zk.close();
  }

  @Test
  public void routerWithSslRefusesNonSecureCommunication() throws ExecutionException, InterruptedException, IOException {
    ZkServerWrapper zk = ServiceFactory.getZkServer();
    MockVeniceRouterWrapper router = ServiceFactory.getMockVeniceRouter(zk.getAddress());
    CloseableHttpAsyncClient httpclient = HttpAsyncClients.custom().build();
    try {
      httpclient.start();
      String address = router.getHost() + ":" + router.getSslPort();
      HttpGet request = new HttpGet("http://" + address + ControllerRoute.MASTER_CONTROLLER.getPath());
      HttpResponse response = httpclient.execute(request, null).get();
      Assert.assertEquals(response.getStatusLine().getStatusCode(), 403);
    } finally {
      httpclient.close();
    }
    router.close();
    zk.close();
  }

}
