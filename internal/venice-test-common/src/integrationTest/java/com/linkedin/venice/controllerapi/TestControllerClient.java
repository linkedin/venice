package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.integration.utils.MockD2ServerWrapper;
import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestControllerClient {
  @Test
  public static void clientReturnsErrorObjectOnConnectionFailure() {
    ControllerClient client = new ControllerClient(Utils.getUniqueString("cluster"), "http://localhost:17079");
    StoreResponse r3 = client.getStore("mystore");
    Assert.assertTrue(r3.isError());
  }

  private static class TestJsonObject {
    private String field1;
    private String field2;

    public String getField1() {
      return field1;
    }

    public String getField2() {
      return field2;
    }

    public void setField1(String fld) {
      field1 = fld;
    }

    public void setField2(String fld) {
      field2 = fld;
    }
  }

  @Test
  public void testObjectMapperIgnoringUnknownProperties() throws IOException {
    ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
    String field1Value = "field1_value";
    String jsonStr = "{\"field1\":\"" + field1Value + "\",\"field3\":\"" + field1Value + "\"}";
    TestJsonObject jsonObject = objectMapper.readValue(jsonStr, TestJsonObject.class);
    Assert.assertEquals(jsonObject.getField1(), field1Value);
    Assert.assertNull(jsonObject.getField2());
  }

  @Test
  public void testGetLeaderControllerUrlWithUrlContainingSpace() throws IOException {
    String clusterName = Utils.getUniqueString("test-cluster");
    String fakeLeaderControllerUrl = "http://fake.leader.controller.url";
    try (MockHttpServerWrapper mockController = ServiceFactory.getMockHttpServer("mock_controller")) {
      LeaderControllerResponse controllerResponse = new LeaderControllerResponse();
      controllerResponse.setCluster(clusterName);
      controllerResponse.setUrl(fakeLeaderControllerUrl);
      ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
      ByteBuf body = Unpooled.wrappedBuffer(objectMapper.writeValueAsBytes(controllerResponse));
      FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
      // We must specify content_length header, otherwise netty will keep polling, since it
      // doesn't know when to finish writing the response.
      response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
      mockController.addResponseForUriPattern(ControllerRoute.LEADER_CONTROLLER.getPath() + ".*", response);

      String controllerUrlWithSpaceAtBeginning = "   http://" + mockController.getAddress();
      ControllerClient controllerClient =
          ControllerClient.constructClusterControllerClient(clusterName, controllerUrlWithSpaceAtBeginning);
      String leaderControllerUrl = controllerClient.getLeaderControllerUrl();
      Assert.assertEquals(leaderControllerUrl, fakeLeaderControllerUrl);
    }
  }

  @Test
  public void testControllerClientWithInvalidUrls() throws IOException {
    String clusterName = Utils.getUniqueString("test-cluster");
    String storeName = Utils.getUniqueString("test-store");
    String nonExistentStoreName = Utils.getUniqueString("test-missing-store");
    String errorResponseStoreName = Utils.getUniqueString("test-error-store");
    String fakeLeaderControllerUrl = "http://fake.leader.controller.url";

    String nonExistentControllerUrl1 = "http://localhost:" + TestUtils.getFreePort();
    String nonExistentControllerUrl2 = "http://localhost:" + TestUtils.getFreePort();

    try (MockHttpServerWrapper mockController = ServiceFactory.getMockHttpServer("mock_controller");
        MockHttpServerWrapper mockLegacyHost = ServiceFactory.getMockHttpServer("mock_legacy_router")) {
      String validControllerUrl = "http://" + mockController.getAddress();
      String legacyControllerUrl = "http://" + mockLegacyHost.getAddress();

      LeaderControllerResponse controllerResponse = new LeaderControllerResponse();
      controllerResponse.setCluster(clusterName);
      controllerResponse.setUrl(fakeLeaderControllerUrl);

      FullHttpResponse response = wrapControllerResponseAsFullHttpResponse(controllerResponse, HttpResponseStatus.OK);
      mockController.addResponseForUriPattern(ControllerRoute.LEADER_CONTROLLER.getPath() + ".*", response);

      D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
      discoveryResponse.setName(storeName);
      discoveryResponse.setCluster(clusterName);

      FullHttpResponse discoHttpResponse =
          wrapControllerResponseAsFullHttpResponse(discoveryResponse, HttpResponseStatus.OK);
      mockController.addResponseForUriPattern(
          ControllerRoute.CLUSTER_DISCOVERY.getPath() + ".*store_name=" + storeName + ".*",
          discoHttpResponse);

      D2ServiceDiscoveryResponse nonExistentStoreDiscoveryResponse = new D2ServiceDiscoveryResponse();
      nonExistentStoreDiscoveryResponse.setName(nonExistentStoreName);
      nonExistentStoreDiscoveryResponse.setError("Store " + nonExistentStoreName + " doesn't exist");
      nonExistentStoreDiscoveryResponse.setErrorType(ErrorType.STORE_NOT_FOUND);

      FullHttpResponse nonExistentStoreDiscoHttpResponse =
          wrapControllerResponseAsFullHttpResponse(nonExistentStoreDiscoveryResponse, HttpResponseStatus.NOT_FOUND);
      mockController.addResponseForUriPattern(
          ControllerRoute.CLUSTER_DISCOVERY.getPath() + ".*store_name=" + nonExistentStoreName + ".*",
          nonExistentStoreDiscoHttpResponse);

      D2ServiceDiscoveryResponse nonExistentStoreLegacyDiscoveryResponse = new D2ServiceDiscoveryResponse();
      nonExistentStoreLegacyDiscoveryResponse.setName(nonExistentStoreName);
      nonExistentStoreLegacyDiscoveryResponse.setError("Store " + nonExistentStoreName + " doesn't exist");
      nonExistentStoreLegacyDiscoveryResponse.setErrorType(ErrorType.GENERAL_ERROR);

      FullHttpResponse nonExistentStoreLegacyDiscoRouterHttpResponse = wrapControllerResponseAsFullHttpResponse(
          nonExistentStoreLegacyDiscoveryResponse,
          HttpResponseStatus.NOT_FOUND);
      mockLegacyHost.addResponseForUriPattern(
          ControllerRoute.CLUSTER_DISCOVERY.getPath() + ".*store_name=" + nonExistentStoreName + ".*",
          nonExistentStoreLegacyDiscoRouterHttpResponse);
      mockLegacyHost.addResponseForUriPattern(
          ControllerRoute.CLUSTER_DISCOVERY.getPath() + "/" + nonExistentStoreName + ".*",
          nonExistentStoreLegacyDiscoRouterHttpResponse);

      D2ServiceDiscoveryResponse errorStoreDiscoveryResponse = new D2ServiceDiscoveryResponse();
      errorStoreDiscoveryResponse.setName(errorResponseStoreName);
      errorStoreDiscoveryResponse.setError("Internal server error");
      errorStoreDiscoveryResponse.setErrorType(ErrorType.GENERAL_ERROR);

      FullHttpResponse errorStoreDiscoHttpResponse = wrapControllerResponseAsFullHttpResponse(
          errorStoreDiscoveryResponse,
          HttpResponseStatus.INTERNAL_SERVER_ERROR);
      mockController.addResponseForUriPattern(
          ControllerRoute.CLUSTER_DISCOVERY.getPath() + ".*store_name=" + errorResponseStoreName + ".*",
          errorStoreDiscoHttpResponse);

      // When all controllers are missing, the ConnectException should be bubbled up
      D2ServiceDiscoveryResponse discoResponseInvalidControllers = ControllerClient
          .discoverCluster(nonExistentControllerUrl1 + "," + nonExistentControllerUrl2, storeName, Optional.empty(), 1);
      Assert.assertTrue(discoResponseInvalidControllers.isError());
      Assert.assertTrue(
          discoResponseInvalidControllers.getError().contains(ConnectException.class.getCanonicalName()),
          discoResponseInvalidControllers.getError());

      // When only some controllers are missing, the ConnectException should never be bubbled up. Since this behavior is
      // triggered from Java libs, and we randomise the controller list to do some load balancing, the best way to
      // validate is to try multiple invocations
      for (int i = 0; i < 100; i++) {
        D2ServiceDiscoveryResponse discoResponsePartialValidController = ControllerClient
            .discoverCluster(nonExistentControllerUrl1 + "," + validControllerUrl, storeName, Optional.empty(), 1);
        Assert.assertFalse(discoResponsePartialValidController.isError());

        D2ServiceDiscoveryResponse nonExistentStoreDiscoResponsePartialValidController =
            ControllerClient.discoverCluster(
                nonExistentControllerUrl1 + "," + validControllerUrl,
                nonExistentStoreName,
                Optional.empty(),
                1);
        Assert.assertTrue(nonExistentStoreDiscoResponsePartialValidController.isError());
        Assert.assertEquals(
            nonExistentStoreDiscoResponsePartialValidController.getErrorType(),
            ErrorType.STORE_NOT_FOUND);

        D2ServiceDiscoveryResponse nonExistentStoreDiscoResponseValidInvalidAndLegacy =
            ControllerClient.discoverCluster(
                nonExistentControllerUrl1 + "," + legacyControllerUrl + "," + validControllerUrl,
                nonExistentStoreName,
                Optional.empty(),
                1);
        Assert.assertTrue(nonExistentStoreDiscoResponseValidInvalidAndLegacy.isError());
        Assert
            .assertEquals(nonExistentStoreDiscoResponseValidInvalidAndLegacy.getErrorType(), ErrorType.STORE_NOT_FOUND);

        // Backward compatibility test. When the controller/router doesn't return STORE_NOT_FOUND, but the client can
        // still infer that it was due to store being deleted, convert to a STORE_NOT_FOUND
        D2ServiceDiscoveryResponse nonExistentStoreDiscoResponseInvalidAndLegacy = ControllerClient.discoverCluster(
            nonExistentControllerUrl1 + "," + legacyControllerUrl,
            nonExistentStoreName,
            Optional.empty(),
            1);
        Assert.assertTrue(nonExistentStoreDiscoResponseInvalidAndLegacy.isError());
        Assert.assertEquals(nonExistentStoreDiscoResponseInvalidAndLegacy.getErrorType(), ErrorType.STORE_NOT_FOUND);

        // Backward compatibility test. When the controller/router doesn't return STORE_NOT_FOUND, and the client cannot
        // identify it as a STORE_NOT_FOUND error, try to query routers using the path param type of cluster discovery
        D2ServiceDiscoveryResponse errorDiscoResponseInvalidAndLegacy = ControllerClient.discoverCluster(
            nonExistentControllerUrl1 + "," + validControllerUrl,
            errorResponseStoreName,
            Optional.empty(),
            1);
        Assert.assertTrue(errorDiscoResponseInvalidAndLegacy.isError());
        Assert.assertEquals(errorDiscoResponseInvalidAndLegacy.getErrorType(), ErrorType.BAD_REQUEST);
      }

      try (ControllerClient controllerClient =
          ControllerClientFactory.getControllerClient(clusterName, validControllerUrl, Optional.empty())) {
        String leaderControllerUrl = controllerClient.getLeaderControllerUrl();
        Assert.assertEquals(leaderControllerUrl, fakeLeaderControllerUrl);
      }
    }
  }

  @Test
  public void testD2ControllerClient() throws Exception {
    String d2ServiceName = Utils.getUniqueString("VeniceController");
    String veniceClusterName = Utils.getUniqueString("test-cluster");
    String fakeLeaderControllerUri = Utils.getUniqueString("http://fake_uri");

    try (MockD2ServerWrapper mockController = ServiceFactory.getMockD2Server("test-controller", d2ServiceName)) {
      String uriPattern = ControllerRoute.LEADER_CONTROLLER.getPath() + ".*cluster_name=" + veniceClusterName + ".*";
      mockController.addResponseForUriPattern(
          uriPattern,
          constructLeaderControllerResponse(veniceClusterName, fakeLeaderControllerUri));
      try (D2ControllerClient d2ControllerClient =
          new D2ControllerClient(d2ServiceName, veniceClusterName, mockController.getZkAddress(), Optional.empty())) {
        String leaderControllerUrl = d2ControllerClient.getLeaderControllerUrl();
        Assert.assertEquals(fakeLeaderControllerUri, leaderControllerUrl);
      }
    }
  }

  @Test
  public void testD2ControllerClientWithExternalD2Client() throws Exception {
    String d2ServiceName = Utils.getUniqueString("VeniceController");
    String veniceClusterName = Utils.getUniqueString("test-cluster");
    String fakeLeaderControllerUri = Utils.getUniqueString("http://fake_uri");

    try (MockD2ServerWrapper mockController = ServiceFactory.getMockD2Server("test-controller", d2ServiceName)) {
      String uriPattern = ControllerRoute.LEADER_CONTROLLER.getPath() + ".*cluster_name=" + veniceClusterName + ".*";
      mockController.addResponseForUriPattern(
          uriPattern,
          constructLeaderControllerResponse(veniceClusterName, fakeLeaderControllerUri));
      D2Client d2Client = new D2ClientBuilder().setZkHosts(mockController.getZkAddress()).build();
      try {
        D2ClientUtils.startClient(d2Client);
        try (D2ControllerClient d2ControllerClient =
            new D2ControllerClient(d2ServiceName, veniceClusterName, d2Client)) {
          String leaderControllerUrl = d2ControllerClient.getLeaderControllerUrl();
          Assert.assertEquals(fakeLeaderControllerUri, leaderControllerUrl);
        }
      } finally {
        D2ClientUtils.shutdownClient(d2Client);
      }
    }
  }

  private FullHttpResponse constructLeaderControllerResponse(String clusterName, String url) throws Exception {
    LeaderControllerResponse response = new LeaderControllerResponse();
    response.setCluster(clusterName);
    response.setUrl(url);

    ByteBuf body = Unpooled.wrappedBuffer(ObjectMapperFactory.getInstance().writeValueAsBytes(response));
    FullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
    httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());

    return httpResponse;
  }

  private FullHttpResponse wrapControllerResponseAsFullHttpResponse(
      ControllerResponse controllerResponse,
      HttpResponseStatus responseStatus) throws JsonProcessingException {
    ByteBuf body = Unpooled.wrappedBuffer(ObjectMapperFactory.getInstance().writeValueAsBytes(controllerResponse));
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, responseStatus, body);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    // We must specify content_length header, otherwise netty will keep polling, since it
    // doesn't know when to finish writing the response.
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
    return response;
  }
}
