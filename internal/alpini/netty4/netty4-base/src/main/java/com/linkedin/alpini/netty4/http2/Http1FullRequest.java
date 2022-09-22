package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.Http2Headers;
import java.util.UUID;


/**
 * An implementation of {@link BasicFullHttpRequest} which always uses a {@link Http1Headers} for
 * holding the headers. This is to aid efficiently encapsulating a Http2 request object as a Http1 object.
 */
public class Http1FullRequest extends BasicFullHttpRequest {
  public Http1FullRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, Unpooled.buffer(0), requestTimestamp, requestNanos);
  }

  public Http1FullRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      ByteBuf content,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, content, true, requestTimestamp, requestNanos);
  }

  public Http1FullRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      boolean validateHeaders,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, Unpooled.buffer(0), validateHeaders, requestTimestamp, requestNanos);
  }

  public Http1FullRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      ByteBuf content,
      boolean validateHeaders,
      long requestTimestamp,
      long requestNanos) {
    this(
        httpVersion,
        content,
        Http1Request.buildHeaders(method, uri, validateHeaders),
        generateRequestId(method, uri, requestTimestamp, requestNanos),
        requestTimestamp,
        requestNanos);
  }

  public Http1FullRequest(
      HttpVersion httpVersion,
      ByteBuf content,
      Http1Headers headers,
      UUID requestId,
      long requestTimestamp,
      long requestNanos) {
    this(
        httpVersion,
        HttpMethod.valueOf(headers.getHttp2Headers().method().toString()),
        headers.getHttp2Headers().path().toString(),
        content,
        headers,
        new Http1Headers(true),
        requestId,
        requestTimestamp,
        requestNanos);
  }

  public Http1FullRequest(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      ByteBuf content,
      HttpHeaders headers,
      HttpHeaders trailingHeader,
      UUID requestId,
      long requestTimestamp,
      long requestNanos) {
    super(
        httpVersion,
        method,
        uri,
        content,
        headers,
        trailingHeader,
        Http1Request.generateRequestId(requestId, method, uri, requestTimestamp, requestNanos),
        requestTimestamp,
        requestNanos);
  }

  public Http1FullRequest(FullHttpRequest request) {
    this(
        request,
        request.headers() instanceof Http1Headers
            ? (Http1Headers) request.headers()
            : (Http1Headers) Http1Request.buildHeaders(request.method(), request.uri(), false).add(request.headers()),
        request.trailingHeaders() instanceof Http1Headers
            ? (Http1Headers) request.trailingHeaders()
            : (Http1Headers) new Http1Headers(false).add(request.trailingHeaders()),
        request.content());
  }

  public Http1FullRequest(HttpRequest request, Http1Headers headers, Http1Headers trailingHeaders, ByteBuf content) {
    super(request, headers, trailingHeaders, content);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BasicFullHttpRequest replace(ByteBuf content) {
    return new Http1FullRequest(this, (Http1Headers) headers(), (Http1Headers) trailingHeaders(), content);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BasicFullHttpRequest setMethod(HttpMethod method) {
    Http2Headers headers = ((Http1Headers) headers()).getHttp2Headers();
    headers.method(method.asciiName());
    return super.setMethod(method);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BasicFullHttpRequest setUri(String uri) {
    Http2Headers headers = ((Http1Headers) headers()).getHttp2Headers();
    headers.path(uri);
    return super.setUri(uri);
  }
}
