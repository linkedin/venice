package com.linkedin.venice.router;


import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.client.VeniceD2Client;
import com.linkedin.venice.client.VeniceServerErrorException;
import com.linkedin.venice.integration.utils.MockVeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.httpclient.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;



/**
 * Created by mwise on 3/9/16.
 */
public class TestRouter {

  /**
   * TODO: Make this test more resilient. It is flaky on slower hardware.
   */
  @Test
  public void testRouterWithD2() throws Exception {
    ZkServerWrapper zk = ServiceFactory.getZkServer();
    MockVeniceRouterWrapper.setupD2Config(zk.getAddress());

    List<D2Server> d2List = MockVeniceRouterWrapper.getD2Servers(zk.getAddress());
    MockVeniceRouterWrapper router = ServiceFactory.getMockVeniceRouter(d2List);
    D2Client d2Client = MockVeniceRouterWrapper.getAndStartD2Client(zk.getAddress());

    URI requestUri = new URI("d2://venice-service/storage/myStore/myKey"); /* D2 client only supports d2:// scheme */
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

    VeniceD2Client vClient = new VeniceD2Client(zk.getAddress(), "venice-service");

    try {
      byte[] value = vClient.get("myStore", "myKey".getBytes(StandardCharsets.UTF_8)).get();
      Assert.fail("Router with Mock components should trigger VeniceServerErrorException");
    } catch (ExecutionException e){
      if (e.getCause() instanceof VeniceServerErrorException){
        // expected.
      } else {
        throw e;
      }
    }

    vClient.close();

    router.close();
    zk.close();
  }

}
