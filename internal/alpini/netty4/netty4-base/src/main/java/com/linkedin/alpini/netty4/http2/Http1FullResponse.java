package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2Headers;


/**
 * An implementation of {@link BasicFullHttpResponse} which always uses a {@link Http1Headers} for
 * holding the headers. This is to aid encapsulating a Http2 request object as a Http1 object
 * which may be efficiently unwrapped before being sent.
 */
public class Http1FullResponse extends BasicFullHttpResponse {
  public Http1FullResponse(HttpRequest httpRequest, HttpResponseStatus status) {
    this(httpRequest, status, Unpooled.buffer(0));
  }

  public Http1FullResponse(HttpRequest httpRequest, HttpResponseStatus status, ByteBuf content) {
    this(httpRequest, status, content, true);
  }

  public Http1FullResponse(HttpRequest httpRequest, HttpResponseStatus status, boolean validateHeaders) {
    this(httpRequest, status, Unpooled.buffer(0), validateHeaders);
  }

  public Http1FullResponse(
      HttpRequest httpRequest,
      HttpResponseStatus status,
      ByteBuf content,
      boolean validateHeaders) {
    this(httpRequest, content, Http1Response.buildHeaders(status, validateHeaders), new Http1Headers(validateHeaders));
  }

  public Http1FullResponse(
      HttpRequest httpRequest,
      ByteBuf content,
      Http1Headers headers,
      Http1Headers trailingHeaders) {
    super(httpRequest, Http1Response.status(headers.getHttp2Headers()), content, headers, trailingHeaders);
  }

  public Http1FullResponse(FullHttpResponse httpResponse) {
    this(
        httpResponse,
        httpResponse.headers() instanceof Http1Headers
            ? (Http1Headers) httpResponse.headers()
            : (Http1Headers) Http1Response.buildHeaders(httpResponse.status(), false).add(httpResponse.headers()),
        httpResponse.trailingHeaders() instanceof Http1Headers
            ? (Http1Headers) httpResponse.trailingHeaders()
            : (Http1Headers) new Http1Headers(false).add(httpResponse.trailingHeaders()),
        httpResponse.content());
  }

  public Http1FullResponse(
      HttpResponse httpResponse,
      Http1Headers headers,
      Http1Headers trailingHeaders,
      ByteBuf content) {
    super(httpResponse, headers, trailingHeaders, content);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FullHttpResponse setStatus(HttpResponseStatus status) {
    Http2Headers headers = ((Http1Headers) headers()).getHttp2Headers();
    Http1Response.setStatus(headers, status);
    return super.setStatus(status);
  }
}
