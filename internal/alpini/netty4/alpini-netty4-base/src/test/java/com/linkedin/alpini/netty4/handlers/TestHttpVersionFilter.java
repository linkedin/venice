package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.netty4.misc.BadHttpRequest;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 9/28/17.
 */
public class TestHttpVersionFilter {
  @Test(groups = "unit")
  public void testBadVersion() throws Exception {

    EmbeddedChannel channel = new EmbeddedChannel(new HttpVersionFilter());

    HttpRequest request = new DefaultHttpRequest(HttpVersion.valueOf("HTTP/0.9"), HttpMethod.GET, "/");

    channel.writeOneInbound(request).sync();

    HttpResponse response = channel.readOutbound();

    Assert.assertSame(response.status(), HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
  }

  @DataProvider
  public Object[][] goodVersions() {
    return new Object[][] { new Object[] { HttpVersion.HTTP_1_0 }, new Object[] { HttpVersion.HTTP_1_1 } };
  }

  @Test(groups = "unit", dataProvider = "goodVersions")
  public void testGoodVersion(HttpVersion goodVersion) throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel(new HttpVersionFilter());

    HttpRequest request = new DefaultHttpRequest(goodVersion, HttpMethod.GET, "/");

    channel.writeOneInbound(request).sync();

    Assert.assertSame(channel.readInbound(), request);

    channel.writeOneInbound(LastHttpContent.EMPTY_LAST_CONTENT).sync();

    Assert.assertSame(channel.readInbound(), LastHttpContent.EMPTY_LAST_CONTENT);
  }

  @Test(groups = "unit")
  public void testBadRequest() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel(new HttpVersionFilter());

    class MyBadRequest extends DefaultHttpRequest implements BadHttpRequest {
      public MyBadRequest(HttpVersion httpVersion, HttpMethod method, String uri) {
        super(httpVersion, method, uri);
      }
    }

    HttpRequest request = new MyBadRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");

    channel.writeOneInbound(request).sync();

    HttpResponse response = channel.readOutbound();

    Assert.assertSame(response.status(), HttpResponseStatus.BAD_REQUEST);
  }

  @Test(groups = "unit")
  public void testBadDecode() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel(new HttpVersionFilter());

    Exception cause = new IllegalArgumentException("Some random message");

    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    request.setDecoderResult(DecoderResult.failure(cause));

    channel.writeOneInbound(request).sync();

    HttpResponse response = channel.readOutbound();

    Assert.assertSame(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert.assertEquals(response.headers().get(HeaderNames.X_ERROR_CLASS), cause.getClass().getName());
    Assert.assertEquals(response.headers().get(HeaderNames.X_ERROR_MESSAGE), cause.getMessage());
  }

}
