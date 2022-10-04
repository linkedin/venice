package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;


/**
 * An implementation of {@link BasicHttpResponse} which always uses a {@link Http1Headers} for
 * holding the headers. This is to aid encapsulating a Http2 request object as a Http1 object
 * which may be efficiently unwrapped before being sent.
 */
public class Http1Response extends BasicHttpResponse {
  static final AsciiString X_HTTP_STATUS_REASON = AsciiString.cached("x-http-status-reason");

  public Http1Response(HttpRequest httpRequest, HttpResponseStatus status) {
    this(httpRequest, status, true);
  }

  public Http1Response(HttpRequest httpRequest, HttpResponseStatus status, boolean validateHeaders) {
    this(httpRequest, buildHeaders(status, validateHeaders));
  }

  public Http1Response(HttpRequest httpRequest, Http1Headers headers) {
    super(httpRequest, status(headers.getHttp2Headers()), headers);
  }

  public Http1Response(HttpResponse response) {
    this(
        response,
        response.headers() instanceof Http1Headers
            ? (Http1Headers) response.headers()
            : (Http1Headers) buildHeaders(response.status(), false).add(response.headers()));
  }

  protected Http1Response(HttpResponse httpResponse, Http1Headers headers) {
    super(httpResponse, headers);
  }

  static Http1Headers buildHeaders(HttpResponseStatus status, boolean validateHeaders) {
    Http1Headers headers = new Http1Headers(validateHeaders);
    setStatus(headers.getHttp2Headers(), status);
    return headers;
  }

  static HttpResponseStatus status(Http2Headers headers) {
    int code = AsciiString.of(headers.status()).parseInt();
    CharSequence reasonPhrase = headers.get(X_HTTP_STATUS_REASON);
    return reasonPhrase != null
        ? HttpResponseStatus.valueOf(code, reasonPhrase.toString())
        : HttpResponseStatus.valueOf(code);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpResponse setStatus(HttpResponseStatus status) {
    Http2Headers headers = ((Http1Headers) headers()).getHttp2Headers();
    setStatus(headers, status);
    return super.setStatus(status);
  }

  static void setStatus(Http2Headers headers, HttpResponseStatus status) {
    headers.status(status.codeAsText());
    if (AsciiString.contentEquals(HttpResponseStatus.valueOf(status.code()).reasonPhrase(), status.reasonPhrase())) {
      headers.remove(X_HTTP_STATUS_REASON);
    } else {
      headers.set(X_HTTP_STATUS_REASON, status.reasonPhrase());
    }
  }
}
