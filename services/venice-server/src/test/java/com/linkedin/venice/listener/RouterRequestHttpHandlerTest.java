package com.linkedin.venice.listener;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.grpc.GrpcHandlerContext;
import com.linkedin.venice.listener.grpc.GrpcHandlerPipeline;
import com.linkedin.venice.listener.grpc.GrpcStatsContext;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RouterRequestHttpHandlerTest {
  @Test
  public void parsesRequests() throws Exception {
    testRequestParsing("/storage/store_v1/1/key1", "store_v1", 1, "key1".getBytes(StandardCharsets.UTF_8));
    testBadRequest("/nopath", HttpMethod.GET);
    testBadRequest("/read/store_v1/1/key1", HttpMethod.GET);
    testBadRequest("/storage/store_v1/key1", HttpMethod.GET);
    testBadRequest("/storage/store_v1/1", HttpMethod.GET);
    testBadRequest("/storage/store_v1/1/key1", HttpMethod.POST);
  }

  @Test
  public void respondsToHealthCheck() throws Exception {
    RouterRequestHttpHandler testHander =
        new RouterRequestHttpHandler(mock(StatsHandler.class), Collections.emptyMap());
    ChannelHandlerContext mockContext = mock(ChannelHandlerContext.class);
    ArgumentCaptor<HealthCheckRequest> argumentCaptor = ArgumentCaptor.forClass(HealthCheckRequest.class);
    HttpRequest healthMsg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/health");
    testHander.channelRead(mockContext, healthMsg);
    verify(mockContext).fireChannelRead(argumentCaptor.capture());
    HealthCheckRequest requestObject = argumentCaptor.getValue();
    Assert.assertNotNull(requestObject);
  }

  public void testRequestParsing(String path, String expectedStore, int expectedPartition, byte[] expectedKey)
      throws Exception {

    // Test handler
    RouterRequestHttpHandler testHander =
        new RouterRequestHttpHandler(mock(StatsHandler.class), Collections.emptyMap());
    ChannelHandlerContext mockContext = mock(ChannelHandlerContext.class);
    ArgumentCaptor<GetRouterRequest> argumentCaptor = ArgumentCaptor.forClass(GetRouterRequest.class);
    HttpRequest msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
    testHander.channelRead(mockContext, msg);
    verify(mockContext).fireChannelRead(argumentCaptor.capture());
    GetRouterRequest requestObject = argumentCaptor.getValue();
    Assert.assertEquals(
        requestObject.getResourceName(),
        expectedStore,
        "Store from path: " + path + " should be parsed as: " + expectedStore);
    Assert.assertEquals(
        requestObject.getPartition(),
        expectedPartition,
        "Partition from path: " + path + " should be parsed as: " + expectedPartition);
    Assert.assertEquals(requestObject.getKeyBytes(), expectedKey, "Key from path: " + path + " was parsed incorrectly");

    // Test parse method
    GetRouterRequest getRouterRequest = GetRouterRequest.parseGetHttpRequest(msg);
    Assert.assertEquals(
        getRouterRequest.getResourceName(),
        expectedStore,
        "Store from path: " + path + " should be parsed as: " + expectedStore);
    Assert.assertEquals(
        getRouterRequest.getPartition(),
        expectedPartition,
        "Partition from path: " + path + " should be parsed as: " + expectedPartition);
    Assert.assertEquals(
        getRouterRequest.getKeyBytes(),
        expectedKey,
        "Key from path: " + path + " was parsed incorrectly");
  }

  public void testBadRequest(String path, HttpMethod method) throws Exception {
    RouterRequestHttpHandler testHander =
        new RouterRequestHttpHandler(mock(StatsHandler.class), Collections.emptyMap());
    ChannelHandlerContext mockContext = mock(ChannelHandlerContext.class);
    ArgumentCaptor<HttpShortcutResponse> argumentCaptor = ArgumentCaptor.forClass(HttpShortcutResponse.class);
    HttpRequest msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path);
    testHander.channelRead(mockContext, msg);
    verify(mockContext).writeAndFlush(argumentCaptor.capture());
    HttpShortcutResponse httpShortcutResponse = argumentCaptor.getValue();
    Assert.assertEquals(httpShortcutResponse.getStatus(), HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void parsesKeys() {
    String b64Key = "bWF0dCB3aXNlIGlzIGF3ZXNvbWU=";
    Base64.Decoder d = Base64.getUrlDecoder();
    doKeyTest("myKey", "myKey".getBytes());
    doKeyTest("myKey?a=b", "myKey".getBytes());
    doKeyTest("myKey?f=string", "myKey".getBytes());
    doKeyTest("myKey?f=b65", "myKey".getBytes());
    doKeyTest(b64Key + "?f=b64", d.decode(b64Key.getBytes()));
    doKeyTest(b64Key + "?a=b&f=b64", d.decode(b64Key.getBytes()));
    doKeyTest(b64Key + "?f=b64&a=b", d.decode(b64Key.getBytes()));
  }

  public void doKeyTest(String urlString, byte[] expectedKey) {
    byte[] parsedKey = GetRouterRequest.getKeyBytesFromUrlKeyString(urlString);
    Assert.assertEquals(
        parsedKey,
        expectedKey,
        urlString + " not parsed correctly as key.  Parsed: " + new String(parsedKey));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void parsesActionBadMethod() {
    doActionTest("/storage/suffix", HttpMethod.HEAD, QueryAction.STORAGE);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void parsesActionBadAction() {
    doActionTest("/get/suffix", HttpMethod.GET, QueryAction.STORAGE);
  }

  @Test
  public void parsesAction() {
    doActionTest("/storage/suffix", HttpMethod.GET, QueryAction.STORAGE);
  }

  public void doActionTest(String urlString, HttpMethod method, QueryAction expectedAction) {
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, urlString);
    QueryAction parsedAction = RouterRequestHttpHandler.getQueryActionFromRequest(request);
    Assert.assertEquals(parsedAction, expectedAction, "parsed wrong query action from string: " + urlString);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void verifyBadApiVersionIsCaught() {
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpConstants.VENICE_API_VERSION, "2");
    GetRouterRequest.verifyApiVersion(headers, "1");
  }

  @Test
  public void verifyGoodApiVersionOk() {
    HttpHeaders headers = new DefaultHttpHeaders();
    GetRouterRequest.verifyApiVersion(headers, "1"); /* missing header parsed as current version */

    headers.add(HttpConstants.VENICE_API_VERSION, "1");
    GetRouterRequest.verifyApiVersion(headers, "1");
  }

  @Test
  public void testGrpcRead() {
    VeniceClientRequest request =
        VeniceClientRequest.newBuilder().setResourceName("teststore_v1").setIsBatchRequest(true).build();
    GrpcHandlerContext ctx = new GrpcHandlerContext(request, VeniceServerResponse.newBuilder(), null);
    GrpcHandlerPipeline pipeline = mock(GrpcHandlerPipeline.class);
    GrpcStatsContext statsContextMock = mock(GrpcStatsContext.class);
    RouterRequestHttpHandler handler = new RouterRequestHttpHandler(mock(StatsHandler.class), Collections.emptyMap());
    ctx.setGrpcStatsContext(statsContextMock);

    // MultiGetRouterRequestWrapper
    handler.grpcRead(ctx, pipeline);

    verify(pipeline).processRequest(any());
  }
}
