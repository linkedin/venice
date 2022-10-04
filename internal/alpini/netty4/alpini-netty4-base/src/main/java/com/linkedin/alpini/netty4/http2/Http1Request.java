package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.Http2Headers;
import java.util.UUID;


/**
 * An implementation of {@link BasicHttpRequest} which always uses a {@link Http1Headers} for
 * holding the headers. This is to aid efficiently encapsulating a Http2 request object as a Http1 object.
 */
public class Http1Request extends BasicHttpRequest {
  public Http1Request(HttpVersion httpVersion, HttpMethod method, String uri) {
    this(httpVersion, method, uri, Time.currentTimeMillis(), Time.nanoTime());
  }

  public Http1Request(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      long requestTimestamp,
      long requestNanos) {
    this(httpVersion, method, uri, true, requestTimestamp, requestNanos);
  }

  public Http1Request(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      boolean validateHeaders,
      long requestTimestamp,
      long requestNanos) {
    this(
        httpVersion,
        buildHeaders(method, uri, validateHeaders),
        generateRequestId(method, uri, requestTimestamp, requestNanos),
        requestTimestamp,
        requestNanos);
  }

  public Http1Request(
      HttpVersion httpVersion,
      Http1Headers headers,
      UUID requestId,
      long requestTimestamp,
      long requestNanos) {
    this(
        httpVersion,
        HttpMethod.valueOf(headers.getHttp2Headers().method().toString()),
        headers.getHttp2Headers().path().toString(),
        headers,
        requestId,
        requestTimestamp,
        requestNanos);
  }

  private Http1Request(
      HttpVersion httpVersion,
      HttpMethod method,
      String uri,
      Http1Headers headers,
      UUID requestId,
      long requestTimestamp,
      long requestNanos) {
    super(
        httpVersion,
        method,
        uri,
        headers,
        generateRequestId(requestId, method, uri, requestTimestamp, requestNanos),
        requestTimestamp,
        requestNanos);
  }

  public Http1Request(HttpRequest request) {
    this(
        request,
        request.headers() instanceof Http1Headers
            ? (Http1Headers) request.headers()
            : (Http1Headers) buildHeaders(request.method(), request.uri(), false).add(request.headers()));
  }

  public Http1Request(HttpRequest request, Http1Headers headers) {
    super(request, headers);
  }

  static Http1Headers buildHeaders(HttpMethod method, CharSequence path, boolean validateHeaders) {
    Http1Headers headers = new Http1Headers(validateHeaders);
    headers.getHttp2Headers().method(method.asciiName());
    headers.getHttp2Headers().path(path);
    return headers;
  }

  static UUID generateRequestId(
      UUID requestId,
      HttpMethod method,
      String uri,
      long requestTimestamp,
      long requestNanos) {
    return requestId != null
        ? requestId
        : BasicHttpRequest.generateRequestId(method, uri, requestTimestamp, requestNanos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpRequest setMethod(HttpMethod method) {
    Http2Headers headers = ((Http1Headers) headers()).getHttp2Headers();
    headers.method(method.asciiName());
    return super.setMethod(method);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpRequest setUri(String uri) {
    Http2Headers headers = ((Http1Headers) headers()).getHttp2Headers();
    headers.path(uri);
    return super.setUri(uri);
  }
}
