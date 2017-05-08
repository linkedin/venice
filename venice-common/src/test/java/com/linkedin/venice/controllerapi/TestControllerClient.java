package com.linkedin.venice.controllerapi;

import com.google.common.net.HttpHeaders;

import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class TestControllerClient {

  @Test
  public static void clientReturnsErrorObjectOnConnectionFailure(){
    ControllerClient client = new ControllerClient(TestUtils.getUniqueString("cluster"), "http://localhost:17079");
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
    ObjectMapper objectMapper = ControllerClient.getObjectMapper();
    String field1Value = "field1_value";
    String jsonStr = "{\"field1\":\"" + field1Value + "\",\"field3\":\"" + field1Value + "\"}";
    TestJsonObject jsonObject = objectMapper.readValue(jsonStr, TestJsonObject.class);
    Assert.assertEquals(jsonObject.getField1(), field1Value);
    Assert.assertNull(jsonObject.getField2());
  }

  @Test
  public void testGetMasterControllerUrlWithUrlContainingSpace() throws IOException {
    String clusterName = TestUtils.getUniqueString("test-cluster");
    String fakeMasterControllerUrl = "http://fake.master.controller.url";
    try (MockHttpServerWrapper mockController = ServiceFactory.getMockHttpServer("mock_controller")) {
      MasterControllerResponse controllerResponse = new MasterControllerResponse();
      controllerResponse.setCluster(clusterName);
      controllerResponse.setUrl(fakeMasterControllerUrl);
      ObjectMapper objectMapper = new ObjectMapper();
      ByteBuf body = Unpooled.wrappedBuffer(objectMapper.writeValueAsBytes(controllerResponse));
      FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
      response.headers().set(HttpHeaders.CONTENT_TYPE, "application/json");
      // We must specify content_length header, otherwise netty will keep polling, since it
      // doesn't know when to finish writing the response.
      response.headers().set(HttpHeaders.CONTENT_LENGTH, response.content().readableBytes());
      mockController.addResponseForUriPattern(ControllerRoute.MASTER_CONTROLLER.getPath() + ".*", response);

      String controllerUrlWithSpaceAtBeginning = "   http://" + mockController.getAddress();
      ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrlWithSpaceAtBeginning);
      String masterControllerUrl = controllerClient.getMasterControllerUrl(controllerUrlWithSpaceAtBeginning);
      Assert.assertEquals(masterControllerUrl, fakeMasterControllerUrl);
    }
  }
}