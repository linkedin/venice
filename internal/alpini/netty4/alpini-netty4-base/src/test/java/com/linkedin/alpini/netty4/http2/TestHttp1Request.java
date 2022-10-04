package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestHttp1Request {
  public void testConstruct1() {
    BasicHttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    Assert.assertSame(request.method(), HttpMethod.GET);
    Assert.assertSame(request.uri(), "/");

    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
  }

  public void testConstruct2() {
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");

    BasicHttpRequest request = new Http1Request(httpRequest);
    Assert.assertSame(request.method(), HttpMethod.GET);
    Assert.assertSame(request.uri(), "/");

    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
  }

  public void testConstruct3() {
    HttpRequest httpRequest = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");

    BasicHttpRequest request = new Http1Request(httpRequest);
    Assert.assertSame(request.method(), HttpMethod.GET);
    Assert.assertSame(request.uri(), "/");

    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));

    Assert.assertSame(httpRequest.headers(), request.headers());
  }

  public void testConstruct4() {
    Http2Headers headers = new DefaultHttp2Headers();
    headers.method(HttpMethod.GET.asciiName());
    headers.path("/hello");
    BasicHttpRequest request = new Http1Request(
        HttpVersion.HTTP_1_1,
        new Http1Headers(headers, false),
        null,
        Time.currentTimeMillis(),
        Time.nanoTime());
    Assert.assertEquals(request.method(), HttpMethod.GET);
    Assert.assertEquals(request.uri(), "/hello");
    Assert.assertSame(((Http1Headers) request.headers()).getHttp2Headers(), headers);
  }

  public void testSetMethod() {
    BasicHttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertEquals(headers.method(), HttpMethod.GET.asciiName());
    request.setMethod(HttpMethod.POST);
    Assert.assertEquals(headers.method(), HttpMethod.POST.asciiName());
  }

  public void testSetUri() {
    BasicHttpRequest request = new Http1Request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    Http2Headers headers = ((Http1Headers) request.headers()).getHttp2Headers();
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/"));
    request.setUri("/hello");
    Assert.assertTrue(AsciiString.contentEquals(headers.path(), "/hello"));
  }
}
