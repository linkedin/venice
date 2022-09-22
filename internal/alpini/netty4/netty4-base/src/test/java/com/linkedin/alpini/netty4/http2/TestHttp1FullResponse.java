package com.linkedin.alpini.netty4.http2;

import static org.testng.Assert.*;

import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestHttp1FullResponse {
  public void testConstruct1() {
    BasicHttpRequest request = new BasicHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    BasicFullHttpResponse response = new Http1FullResponse(request, HttpResponseStatus.OK);
    assertSame(response.status(), HttpResponseStatus.OK);
    Http2Headers headers = ((Http1Headers) response.headers()).getHttp2Headers();
    Assert.assertEquals(headers.status(), HttpResponseStatus.OK.codeAsText());
  }

  public void testConstruct2() {
    BasicHttpRequest request = new BasicHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    BasicFullHttpResponse response = new Http1FullResponse(request, HttpResponseStatus.OK, false);
    assertSame(response.status(), HttpResponseStatus.OK);
    Http2Headers headers = ((Http1Headers) response.headers()).getHttp2Headers();
    Assert.assertEquals(headers.status(), HttpResponseStatus.OK.codeAsText());
  }

  public void testConstruct3() {
    BasicHttpRequest request = new BasicHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    BasicFullHttpResponse basicFullHttpResponse = new BasicFullHttpResponse(request, HttpResponseStatus.OK);
    BasicFullHttpResponse response = new Http1FullResponse(basicFullHttpResponse);
    assertSame(response.status(), HttpResponseStatus.OK);
    Http2Headers headers = ((Http1Headers) response.headers()).getHttp2Headers();
    Assert.assertEquals(headers.status(), HttpResponseStatus.OK.codeAsText());
  }

  public void testConstruct4() {
    BasicHttpRequest request = new BasicHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    BasicFullHttpResponse basicFullHttpResponse = new Http1FullResponse(request, HttpResponseStatus.OK);
    BasicFullHttpResponse response = new Http1FullResponse(basicFullHttpResponse);
    assertSame(response.status(), HttpResponseStatus.OK);
    Http2Headers headers = ((Http1Headers) response.headers()).getHttp2Headers();
    Assert.assertEquals(headers.status(), HttpResponseStatus.OK.codeAsText());
    Assert.assertSame(response.headers(), basicFullHttpResponse.headers());
  }

  public void testSetStatus() {
    BasicHttpRequest request = new BasicHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    BasicFullHttpResponse response = new Http1FullResponse(request, HttpResponseStatus.OK);
    assertSame(response.status(), HttpResponseStatus.OK);
    response.setStatus(HttpResponseStatus.NOT_FOUND);
    assertSame(response.status(), HttpResponseStatus.NOT_FOUND);
    Http2Headers headers = ((Http1Headers) response.headers()).getHttp2Headers();
    Assert.assertEquals(headers.status(), HttpResponseStatus.NOT_FOUND.codeAsText());
    Assert.assertNull(headers.get(Http1Response.X_HTTP_STATUS_REASON));
    response.setStatus(HttpResponseStatus.valueOf(HttpResponseStatus.FORBIDDEN.code(), "Foo Access Denied"));
    Assert.assertEquals(headers.status(), HttpResponseStatus.FORBIDDEN.codeAsText());
    Assert.assertTrue(AsciiString.contentEquals(headers.get(Http1Response.X_HTTP_STATUS_REASON), "Foo Access Denied"));
    response.setStatus(HttpResponseStatus.NOT_FOUND);
    Assert.assertNull(headers.get(Http1Response.X_HTTP_STATUS_REASON));
  }
}
