package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Created by mwise on 3/8/16.
 */
public class HandlerTest {

  static ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void handlerCreatesStores() throws Exception {

    String requestUri = "http://localhost:1234/create";
    String requestContent = "storename=mystore&store_size=8000&owner=devuser";

    DefaultFullHttpResponse response = testHandler(requestUri, requestContent, 5);
    Map<String, Object> parsedJson = getJsonFromHttp(response);
    Map<String,Object> params = (Map<String,Object>) parsedJson.get("parameters");

    //Assert it is what we expect.
    Assert.assertEquals(params.get("storename"), "mystore");
    Assert.assertEquals(params.get("owner"), "devuser");
    Assert.assertEquals(params.get("store_size"), "8000");
    Assert.assertEquals(parsedJson.get("version"), 5);
    Assert.assertEquals(parsedJson.get("action"), "creating store-version");
    Assert.assertEquals(parsedJson.get("store_status"), "created");

    Assert.assertFalse(parsedJson.containsKey("error"));
    Assert.assertEquals(response.getStatus(), HttpResponseStatus.OK);
  }

  @Test
  public void handlerCapturesBadValidation() throws Exception {
    badRequestContent("storename=mystore&store_size=NaN&owner=devuser"); //non numrtic store size
    badRequestContent("storename=mystore&owner=devuser"); //missing store size
    badRequestContent("store_size=8000&owner=devuser"); // missing storename
    badRequestContent("storename=mystore&store_size=8000"); //missing owner
    badRequestContent("storename=&store_size=100&owner=devuser"); //empty storename
    badRequestContent("storename=mystore&store_size=100&owner="); //empty owner
  }

  public void badRequestContent(String requestContent) throws Exception {
    String requestUri = "http://localhost:1234/create";

    DefaultFullHttpResponse response = testHandler(requestUri, requestContent, 5);
    Map<String, Object> parsedJson = getJsonFromHttp(response);

    Assert.assertTrue(parsedJson.containsKey("error"),
        "Controller admin interface should throw an error on bad input: " + requestContent);
    Assert.assertEquals(response.getStatus(), HttpResponseStatus.BAD_REQUEST,
        "Controller admin interface should send an HTTP 400 on bad input: " + requestContent);
  }

  public DefaultFullHttpResponse testHandler(String requestUri, String requestContent, int newStoreVersion)
      throws Exception {
    //Mock Objects
    Admin mockAdmin = Mockito.mock(Admin.class);
    when(mockAdmin.incrementVersion(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(5);
    ChannelHandlerContext mockCtx = Mockito.mock(ChannelHandlerContext.class);
    Channel mockChannel = Mockito.mock(Channel.class);
    ChannelFuture mockFuture = Mockito.mock(ChannelFuture.class);
    when(mockCtx.channel()).thenReturn(mockChannel);
    when(mockChannel.writeAndFlush(Mockito.anyObject())).thenReturn(mockFuture); //Capture this FullHttpResponse ?

    //Handler that we're testing with
    Handler testHandler = new Handler("pretend-cluster-for-testing", mockAdmin);

    //Creating request objects to pass to handler
    ByteBuf content = Unpooled.buffer(1000);
    content.writeBytes(requestContent.getBytes());
    HttpRequest testRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, requestUri, content);

    //Do the thing we're testing
    testHandler.channelRead0(mockCtx, testRequest);
    ArgumentCaptor<DefaultFullHttpResponse> argumentCaptor = ArgumentCaptor.forClass(DefaultFullHttpResponse.class);
    verify(mockChannel).writeAndFlush(argumentCaptor.capture());
    return argumentCaptor.getValue();
  }

  public Map<String, Object> getJsonFromHttp(DefaultFullHttpResponse response) throws IOException {
    return objectMapper.readValue(new ByteArrayInputStream(response.content().array()),
        new TypeReference<HashMap<String, Object>>() {});
  }

}
