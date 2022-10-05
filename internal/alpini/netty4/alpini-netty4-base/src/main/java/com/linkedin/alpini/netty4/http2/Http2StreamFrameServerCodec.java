/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linkedin.alpini.netty4.http2;

import static io.netty.util.internal.ObjectUtil.checkNotNull;

import com.linkedin.alpini.base.misc.IteratorUtil;
import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.AsciiString;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;


@ChannelHandler.Sharable
public class Http2StreamFrameServerCodec extends AbstractHttp2StreamFrameCodec {
  public static final HttpVersion HTTP_2_0 = new HttpVersion("HTTP", 2, 0, true);

  private final HttpVersion _httpVersion;

  public Http2StreamFrameServerCodec(HttpVersion httpVersion, boolean validateHeaders) {
    super(validateHeaders);
    _httpVersion = Objects.requireNonNull(httpVersion);
  }

  public Http2StreamFrameServerCodec() {
    this(HTTP_2_0, true);
  }

  protected Http2Headers toHttp2Headers(final ChannelHandlerContext ctx, final HttpMessage msg, boolean validateHeaders)
      throws Http2Exception {

    if (msg.headers() instanceof Http1Headers) {
      Http2Headers headers = ((Http1Headers) msg.headers()).getHttp2Headers();

      HTTP_TO_HTTP2_HEADER_DENY_LIST.forEach(headers::remove);

      return headers;
    }

    // slower old path, we must construct a new Http2Headers
    return HttpConversionUtil.toHttp2Headers(msg, validateHeaders);
  }

  @Override
  protected HttpMessage newMessage(int id, Http2Headers headers, boolean validateHttpHeaders) throws Http2Exception {
    // HTTP/2 does not define a way to carry the version identifier that is included in the HTTP/1.1 request line.
    checkNotNull(headers.method(), "method header cannot be null in conversion to HTTP/1.x");
    checkNotNull(headers.path(), "path header cannot be null in conversion to HTTP/1.x");

    headers.remove(HttpHeaderNames.TRANSFER_ENCODING);
    return new Http1Request(
        _httpVersion,
        new RequestHeaders(id, headers, validateHttpHeaders),
        null,
        Time.currentTimeMillis(),
        Time.nanoTime());
  }

  @Override
  protected FullHttpMessage newFullMessage(
      int id,
      Http2Headers headers,
      boolean validateHttpHeaders,
      ByteBufAllocator alloc) throws Http2Exception {
    // HTTP/2 does not define a way to carry the version identifier that is included in the HTTP/1.1 request line.
    checkNotNull(headers.method(), "method header cannot be null in conversion to HTTP/1.x");
    checkNotNull(headers.path(), "path header cannot be null in conversion to HTTP/1.x");

    headers.remove(HttpHeaderNames.TRANSFER_ENCODING);
    return new Http1FullRequest(
        _httpVersion,
        alloc.buffer(),
        new RequestHeaders(id, headers, validateHttpHeaders),
        null,
        Time.currentTimeMillis(),
        Time.nanoTime());
  }

  private static class RequestHeaders extends Http1Headers {
    final Function<CharSequence, CharSequence> authority;
    final Function<CharSequence, CharSequence> scheme;
    final Function<CharSequence, CharSequence> path;

    public RequestHeaders(int id, Http2Headers headers, boolean validateCopies) {
      this(
          id,
          headers,
          validateCopies,
          ignore -> headers.authority(),
          ignore -> headers.scheme(),
          ignore -> headers.path());
    }

    private RequestHeaders(
        int id,
        Http2Headers headers,
        boolean validateCopies,
        final Function<CharSequence, CharSequence> authority,
        final Function<CharSequence, CharSequence> scheme,
        final Function<CharSequence, CharSequence> path) {
      super(headers, validateCopies);
      this.authority = authority;
      this.scheme = scheme;
      this.path = path;
      setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), id);
    }

    private Function<CharSequence, CharSequence> getter(CharSequence name, Function<CharSequence, CharSequence> dflt) {
      if (HttpHeaderNames.HOST.contentEqualsIgnoreCase(name)) {
        return authority;
      }
      if (HttpConversionUtil.ExtensionHeaderNames.SCHEME.text().contentEqualsIgnoreCase(name)) {
        return scheme;
      }
      if (HttpConversionUtil.ExtensionHeaderNames.PATH.text().contentEqualsIgnoreCase(name)) {
        return path;
      }
      if (isVisible(name)) {
        return dflt;
      } else {
        return ignore -> null;
      }
    }

    private BiConsumer<CharSequence, Object> setter(CharSequence name, BiConsumer<CharSequence, Object> dflt) {
      if (HttpHeaderNames.HOST.contentEqualsIgnoreCase(name)) {
        return (ignore, value) -> getHttp2Headers().authority((CharSequence) value);
      }
      if (HttpConversionUtil.ExtensionHeaderNames.SCHEME.text().contentEqualsIgnoreCase(name)) {
        return (ignore, value) -> getHttp2Headers().scheme((CharSequence) value);
      }
      if (HttpConversionUtil.ExtensionHeaderNames.PATH.text().contentEqualsIgnoreCase(name)) {
        return (ignore, value) -> getHttp2Headers().path((CharSequence) value);
      }
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

    private Iterator<Map.Entry<CharSequence, CharSequence>> iteratorCharSequenceConvert(
        Function<CharSequence, CharSequence> supplier,
        AsciiString target) {
      CharSequence content = supplier.apply(target);
      return content != null
          ? IteratorUtil.singleton(new AbstractMap.SimpleImmutableEntry<>(target, content))
          : IteratorUtil.empty();
    }

    @Override
    protected boolean isVisible(CharSequence cs) {
      return super.isVisible(cs) && !(HttpHeaderNames.TRAILER.contentEqualsIgnoreCase(cs));
    }

    @Override
    public Iterator<Map.Entry<CharSequence, CharSequence>> iteratorCharSequence() {
      return IteratorUtil.concat(
          iteratorCharSequenceConvert(authority, HttpHeaderNames.HOST),
          iteratorCharSequenceConvert(scheme, HttpConversionUtil.ExtensionHeaderNames.SCHEME.text()),
          iteratorCharSequenceConvert(path, HttpConversionUtil.ExtensionHeaderNames.PATH.text()),
          super.iteratorCharSequence());
    }
  }
}
