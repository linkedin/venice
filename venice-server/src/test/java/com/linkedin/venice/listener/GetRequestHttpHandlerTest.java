package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.meta.QueryAction;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/10/16.
 */
public class GetRequestHttpHandlerTest {

  @Test
  public void parsesRequests()
      throws Exception {
    testRequestParsing("/read/store_v1/1/key1", "store_v1", 1, "key1".getBytes(StandardCharsets.UTF_8));
    testBadRequest("/nopath", HttpMethod.GET);
    testBadRequest("/get/store_v1/1/key1", HttpMethod.GET);
    testBadRequest("/read/store_v1/key1", HttpMethod.GET);
    testBadRequest("/read/store_v1/1", HttpMethod.GET);
    testBadRequest("/read/store_v1/1/key1", HttpMethod.POST);
  }

  public void testRequestParsing(String path, String expectedStore, int expectedPartition, byte[] expectedKey)
      throws Exception {

    // Test handler
    GetRequestHttpHandler testHander = new GetRequestHttpHandler();
    ChannelHandlerContext mockContext = Mockito.mock(ChannelHandlerContext.class);
    ArgumentCaptor<GetRequestObject> argumentCaptor = ArgumentCaptor.forClass(GetRequestObject.class);
    HttpRequest msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
    testHander.channelRead(mockContext, msg);
    verify(mockContext).fireChannelRead(argumentCaptor.capture());
    GetRequestObject requestObject = argumentCaptor.getValue();
    Assert.assertEquals(requestObject.getStoreString(), expectedStore,
        "Store from path: " + path + " should be parsed as: " + expectedStore);
    Assert.assertEquals(requestObject.getPartition(), expectedPartition,
        "Partition from path: " + path + " should be parsed as: " + expectedPartition);
    Assert.assertEquals(requestObject.getKey(), expectedKey, "Key from path: " + path + " was parsed incorrectly");

    //Test parse method
    GetRequestObject parsedRequestObject = GetRequestHttpHandler.parseReadFromUri(path);
    Assert.assertEquals(parsedRequestObject.getStoreString(), expectedStore,
        "Store from path: " + path + " should be parsed as: " + expectedStore);
    Assert.assertEquals(parsedRequestObject.getPartition(), expectedPartition,
        "Partition from path: " + path + " should be parsed as: " + expectedPartition);
    Assert.assertEquals(parsedRequestObject.getKey(), expectedKey, "Key from path: " + path + " was parsed incorrectly");
  }

  public void testBadRequest(String path, HttpMethod method)
      throws Exception {
    GetRequestHttpHandler testHander = new GetRequestHttpHandler();
    ChannelHandlerContext mockContext = Mockito.mock(ChannelHandlerContext.class);
    ArgumentCaptor<HttpError> argumentCaptor = ArgumentCaptor.forClass(HttpError.class);
    HttpRequest msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path);
    testHander.channelRead(mockContext, msg);
    verify(mockContext).writeAndFlush(argumentCaptor.capture());
    HttpError httpError = argumentCaptor.getValue();
    Assert.assertEquals(httpError.getStatus(), HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void parsesKeys(){
    String b64Key = "bWF0dCB3aXNlIGlzIGF3ZXNvbWU=";
    Base64.Decoder d = Base64.getDecoder();
    doKeyTest("myKey", "myKey".getBytes());
    doKeyTest("myKey?a=b", "myKey".getBytes());
    doKeyTest("myKey?f=string", "myKey".getBytes());
    doKeyTest("myKey?f=b65", "myKey".getBytes());
    doKeyTest(b64Key + "?f=b64", d.decode(b64Key.getBytes()));
    doKeyTest(b64Key + "?a=b&f=b64", d.decode(b64Key.getBytes()));
    doKeyTest(b64Key + "?f=b64&a=b", d.decode(b64Key.getBytes()));
  }

  public void doKeyTest(String urlString, byte[] expectedKey){
    byte[] parsedKey = GetRequestHttpHandler.getKeyBytesFromUrlKeyString(urlString);
    Assert.assertEquals(parsedKey, expectedKey,
        urlString + " not parsed correctly as key.  Parsed: " + new String(parsedKey));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void parsesActionBadMethod(){
    doActionTest("/read/suffix", HttpMethod.POST, QueryAction.READ);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void parsesActionBadAction(){
    doActionTest("/get/suffix", HttpMethod.GET, QueryAction.READ);
  }

  @Test
  public void parsesAction(){
    doActionTest("/read/suffix", HttpMethod.GET, QueryAction.READ);
  }

  public void doActionTest(String urlString, HttpMethod method, QueryAction expectedAction){
    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, urlString);
    QueryAction parsedAction = GetRequestHttpHandler.getQueryActionFromRequest(request);
    Assert.assertEquals(parsedAction, expectedAction, "parsed wrong query action from string: " + urlString);
  }

}
