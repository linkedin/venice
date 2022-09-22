package com.linkedin.alpini.netty4.http2;

import static io.netty.util.internal.ObjectUtil.checkNotNull;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.HttpConversionUtil;
import java.util.function.BiConsumer;
import java.util.function.Function;


public class Http2StreamFrameClientCodec extends AbstractHttp2StreamFrameCodec {
  private HttpRequest _httpRequest;

  public Http2StreamFrameClientCodec() {
    this(true);
  }

  public Http2StreamFrameClientCodec(boolean validateHeaders) {
    super(validateHeaders);
  }

  @Override
  protected Http2Headers toHttp2Headers(ChannelHandlerContext ctx, HttpMessage msg, boolean validateHeaders)
      throws Http2Exception {
    if (msg instanceof HttpRequest) {
      _httpRequest = (HttpRequest) msg;

      if (msg.headers() instanceof Http1Headers) {
        Http2Headers headers = ((Http1Headers) msg.headers()).getHttp2Headers();

        if (headers.scheme() == null) {
          headers.scheme(connectionScheme(ctx).name());
        }

        HTTP_TO_HTTP2_HEADER_BLACKLIST.forEach(headers::remove);

        return headers;
      }

      // slower old path, we must construct a new Http2Headers
      msg.headers().set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), connectionScheme(ctx));
    } else {
      throw new Http2Exception(Http2Error.PROTOCOL_ERROR);
    }

    return HttpConversionUtil.toHttp2Headers(msg, validateHeaders);
  }

  @Override
  protected HttpMessage newMessage(int id, Http2Headers headers, boolean validateHeaders) throws Http2Exception {
    checkNotNull(headers.status(), "status header cannot be null in conversion to HTTP/1.x");

    headers.remove(HttpHeaderNames.TRANSFER_ENCODING);
    return new Http1Response(_httpRequest, new ResponseHeaders(id, headers, validateHeaders));
  }

  @Override
  protected FullHttpMessage newFullMessage(
      int id,
      Http2Headers headers,
      boolean validateHeaders,
      ByteBufAllocator alloc) throws Http2Exception {
    checkNotNull(headers.status(), "status header cannot be null in conversion to HTTP/1.x");

    headers.remove(HttpHeaderNames.TRANSFER_ENCODING);
    return new Http1FullResponse(
        _httpRequest,
        alloc.buffer(),
        new ResponseHeaders(id, headers, validateHeaders),
        new Http1Headers(validateHeaders));
  }

  private static class ResponseHeaders extends Http1Headers {
    public ResponseHeaders(int id, Http2Headers headers, boolean validateCopies) {
      super(headers, validateCopies);
      setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), id);
    }

    private Function<CharSequence, CharSequence> getter(CharSequence name, Function<CharSequence, CharSequence> dflt) {
      if (isVisible(name)) {
        return dflt;
      } else {
        return ignore -> null;
      }
    }

    private BiConsumer<CharSequence, Object> setter(CharSequence name, BiConsumer<CharSequence, Object> dflt) {
      if (isVisible(name)) {
        return dflt;
      } else {
        return (ignore, value) -> { /* discard */ };
      }
    }

    @Override
    public String get(CharSequence name) {
      return str(getter(name, super::get).apply(name));
    }

    @Override
    public String get(CharSequence name, String defaultValue) {
      CharSequence content = getter(name, super::get).apply(name);
      return content != null ? str(content) : defaultValue;
    }

    @Override
    public HttpHeaders add(CharSequence name, Object value) {
      setter(name, super::add).accept(name, value);
      return this;
    }

    @Override
    public HttpHeaders set(CharSequence name, Object value) {
      setter(name, super::set).accept(name, value);
      return this;
    }

    @Override
    protected boolean isVisible(CharSequence cs) {
      return super.isVisible(cs) && !(HttpHeaderNames.TRAILER.contentEqualsIgnoreCase(cs));
    }
  }
}
