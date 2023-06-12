package com.linkedin.venice.router;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MockVeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.SslUtils;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLContext;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRouter {
  private static final boolean SSL_TO_STORAGE_NODES = false;

  @Test
  public void testRouterWithHttpD2() throws Exception {
    testRouterWithD2(false, false);
  }

  @Test
  public void testSecureRouterWithHttpD2() throws Exception {
    try {
      testRouterWithD2(false, true);
      Assert.fail("Cannot connect with secure router with http.");
    } catch (Exception e) {
      // expected
      Assert.assertEquals(e.getClass(), ExecutionException.class);
    }
  }

  @Test
  public void testRouterWithHttpsD2() throws Exception {
    testRouterWithD2(true, false);
  }

  public void testRouterWithD2(boolean https, boolean secureOnly) throws Exception {
    try (ZkServerWrapper zk = ServiceFactory.getZkServer()) {
      Properties extraConfigs = new Properties();
      if (secureOnly) {
        extraConfigs.put(ConfigKeys.ENFORCE_SECURE_ROUTER, true);
      }

      try (MockVeniceRouterWrapper router =
          ServiceFactory.getMockVeniceRouter(zk.getAddress(), SSL_TO_STORAGE_NODES, extraConfigs)) {
        D2Client d2Client;
        if (https) {
          d2Client = D2TestUtils.getAndStartHttpsD2Client(zk.getAddress());
        } else {
          d2Client = D2TestUtils.getAndStartD2Client(zk.getAddress());
        }

        URI requestUri = new URI(
            "d2://" + router.getRouterD2Service()
                + "/storage/myStore/myKey"); /* D2 client only supports d2:// scheme */
        // "get" method name is put on purpose, since we would like Venice Router to support both 'get' and 'GET'
        RestRequest request = new RestRequestBuilder(requestUri).setMethod("get").build();
        RestResponse response;
        try {
          response = d2Client.restRequest(request).get();
        } catch (ExecutionException e) {
          if (e.getCause() instanceof RestException) {
            response = ((RestException) e.getCause()).getResponse();
          } else {
            throw e;
          }
        }
        if (secureOnly) {
          Assert.assertEquals(
              response.getStatus(),
              HttpStatus.SC_FORBIDDEN,
              "SecureRouter should return a 403 forbidden error");
        } else {
          Assert.assertEquals(
              response.getStatus(),
              HttpStatus.SC_SERVICE_UNAVAILABLE,
              "Router with Mock components should return a 503 service unavailable error. Response: "
                  + response.toString());
        }

        try (InternalAvroStoreClient<Object, Object> storeClient =
            (InternalAvroStoreClient<Object, Object>) ClientFactory.getAndStartGenericAvroClient(
                ClientConfig.defaultGenericClientConfig("myStore")
                    .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
                    .setD2Client(d2Client))) {
          storeClient.getRaw("storage/myStore/myKey").get();
          Assert.fail("Router with Mock components should trigger VeniceClientHttpException");
        } catch (ExecutionException e) {
          if (e.getCause() instanceof VeniceClientHttpException) {
            // expected.
          } else {
            throw e;
          }
        }
      }
    }
  }

  @Test
  public void testRouterWithSsl() throws ExecutionException, InterruptedException, IOException {
    try (ZkServerWrapper zk = ServiceFactory.getZkServer();
        MockVeniceRouterWrapper router =
            ServiceFactory.getMockVeniceRouter(zk.getAddress(), SSL_TO_STORAGE_NODES, new Properties())) {

      SSLContext sslContext = SslUtils.getVeniceLocalSslFactory().getSSLContext();
      SSLIOSessionStrategy sslSessionStrategy = new SSLIOSessionStrategy(sslContext);
      try (CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom().setSSLStrategy(sslSessionStrategy).build()) {

        httpClient.start();
        HttpGet request = new HttpGet(
            "https://" + router.getHost() + ":" + router.getSslPort() + ControllerRoute.LEADER_CONTROLLER.getPath());
        HttpResponse response = httpClient.execute(request, null).get();
        String jsonContent = IOUtils.toString(response.getEntity().getContent());
        LeaderControllerResponse controllerResponse =
            ObjectMapperFactory.getInstance().readValue(jsonContent, LeaderControllerResponse.class);
        Assert.assertEquals(controllerResponse.getCluster(), router.getClusterName());
      }

      MetricsRepository metrics = router.getMetricsRepository();
      Assert.assertEquals(metrics.getMetric(".security--ssl_error.Count").value(), 0.);
    }
  }

  @Test
  public void routerWithSslRefusesNonSecureCommunication()
      throws ExecutionException, InterruptedException, IOException {
    try (ZkServerWrapper zk = ServiceFactory.getZkServer();
        MockVeniceRouterWrapper router =
            ServiceFactory.getMockVeniceRouter(zk.getAddress(), SSL_TO_STORAGE_NODES, new Properties())) {

      try (CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault()) {
        httpClient.start();
        HttpGet request = new HttpGet(
            "http://" + router.getHost() + ":" + router.getSslPort() + ControllerRoute.LEADER_CONTROLLER.getPath());
        HttpResponse response = httpClient.execute(request, null).get();
        Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_FORBIDDEN);
      }

      MetricsRepository metrics = router.getMetricsRepository();
      Assert.assertEquals(metrics.getMetric(".security--ssl_error.Count").value(), 1.);
    }
  }

  @Test
  public void testRouterRespondsToAdminOperations() throws ExecutionException, InterruptedException, IOException {
    try (ZkServerWrapper zk = ServiceFactory.getZkServer();
        MockVeniceRouterWrapper router =
            ServiceFactory.getMockVeniceRouter(zk.getAddress(), SSL_TO_STORAGE_NODES, new Properties())) {

      SSLContext sslContext = SslUtils.getVeniceLocalSslFactory().getSSLContext();
      SSLIOSessionStrategy sslSessionStrategy = new SSLIOSessionStrategy(sslContext);
      try (CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom().setSSLStrategy(sslSessionStrategy).build()) {

        httpClient.start();
        HttpGet request =
            new HttpGet("https://" + router.getHost() + ":" + router.getSslPort() + "/admin/readQuotaThrottle");
        HttpResponse response = httpClient.execute(request, null).get();
        Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
      }

      MetricsRepository metrics = router.getMetricsRepository();
      Assert.assertEquals(metrics.getMetric(".security--ssl_error.Count").value(), 0.);
    }
  }
}
