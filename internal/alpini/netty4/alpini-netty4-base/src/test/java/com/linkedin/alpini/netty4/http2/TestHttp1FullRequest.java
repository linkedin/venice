package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestHttp1FullRequest {
  public void testConstruct1() {
    BasicFullHttpRequest request =
        new Http1FullRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    Assert.assertSame(request.method(), HttpMethod.GET);
    Assert.assertSame(request.uri(), "/");

    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
  }

  public void testConstruct2() {
    BasicFullHttpRequest request = new Http1FullRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/",
        false,
        Time.currentTimeMillis(),
        Time.nanoTime());
    Assert.assertSame(request.method(), HttpMethod.GET);
    Assert.assertSame(request.uri(), "/");

    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
  }

  public void testConstruct3() {
    BasicFullHttpRequest basicFullHttpRequest =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    BasicFullHttpRequest request = new Http1FullRequest(basicFullHttpRequest);
    Assert.assertSame(request.method(), HttpMethod.GET);
    Assert.assertSame(request.uri(), "/");

    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
  }

  public void testConstruct4() {
    BasicFullHttpRequest basicFullHttpRequest =
        new Http1FullRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    BasicFullHttpRequest request = new Http1FullRequest(basicFullHttpRequest);
    Assert.assertSame(request.method(), HttpMethod.GET);
    Assert.assertSame(request.uri(), "/");

    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
    Assert.assertSame(request.headers(), basicFullHttpRequest.headers());
  }

  public void testReplace() {
    BasicFullHttpRequest request =
        new Http1FullRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    Assert.assertSame(request.method(), HttpMethod.GET);
    Assert.assertSame(request.uri(), "/");

    BasicFullHttpRequest duplicate = request.duplicate();

    Http2Headers headers = ((Http1Headers) duplicate.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
  }

  @Test
  public void testSetMethod() {
    BasicFullHttpRequest request =
        new Http1FullRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    request.setMethod(HttpMethod.POST);
    Assert.assertEquals(headers.method(), HttpMethod.POST.asciiName());
  }

  @Test
  public void testSetUri() {
    BasicFullHttpRequest request =
        new Http1FullRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
    request.setUri("/hello");
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/hello"));
  }
}
