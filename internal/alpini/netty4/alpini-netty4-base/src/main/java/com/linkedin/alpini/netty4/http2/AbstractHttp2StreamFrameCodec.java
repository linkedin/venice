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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.CharSequenceMap;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameStream;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamFrame;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AsciiString;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * A copy of the netty {@linkplain io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec} except with the
 * methods which actually return the converted objects have been made abstract.
 */
public abstract class AbstractHttp2StreamFrameCodec extends MessageToMessageCodec<Http2StreamFrame, HttpObject> {
  private static final AttributeKey<HttpScheme> SCHEME_ATTR_KEY =
      AttributeKey.valueOf(HttpScheme.class, "STREAMFRAMECODEC_SCHEME");

  /**
   * The set of headers that should not be directly copied when converting headers from HTTP to HTTP/2.
   */
  protected static final List<AsciiString> HTTP_TO_HTTP2_HEADER_DENY_LIST;

  static {
    try {
      // go/inclusivecode exempt(String is a netty4 field name of the same name)
      Field field = HttpConversionUtil.class.getDeclaredField("HTTP_TO_HTTP2_HEADER_BLACKLIST");
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      CharSequenceMap<AsciiString> map = (CharSequenceMap<AsciiString>) field.get(null);
      HTTP_TO_HTTP2_HEADER_DENY_LIST = Collections
          .unmodifiableList(Arrays.asList(map.names().stream().map(AsciiString::of).toArray(AsciiString[]::new)));
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  private final boolean validateHeaders;

  protected AbstractHttp2StreamFrameCodec(final boolean validateHeaders) {
    this.validateHeaders = validateHeaders;
  }

  @Override
  public boolean acceptInboundMessage(Object msg) throws Exception {
    return (msg instanceof Http2HeadersFrame) || (msg instanceof Http2DataFrame);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, Http2StreamFrame frame, List<Object> out) throws Exception {
    if (frame instanceof Http2HeadersFrame) {
      Http2HeadersFrame headersFrame = (Http2HeadersFrame) frame;
      Http2Headers headers = headersFrame.headers();
      Http2FrameStream stream = headersFrame.stream();
      int id = stream == null ? 0 : stream.id();

      final CharSequence status = headers.status();

      // 100-continue response is a special case where Http2HeadersFrame#isEndStream=false
      // but we need to decode it as a FullHttpResponse to play nice with HttpObjectAggregator.
      if (null != status && HttpResponseStatus.CONTINUE.codeAsText().contentEquals(status)) {
        final FullHttpMessage fullMsg = newFullMessage(id, headers, validateHeaders, ctx.alloc());
        out.add(fullMsg);
        return;
      }

      if (headersFrame.isEndStream()) {
        if (headers.method() == null && status == null) {
          LastHttpContent last = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER, validateHeaders);
          HttpConversionUtil
              .addHttp2ToHttpHeaders(id, headers, last.trailingHeaders(), HttpVersion.HTTP_1_1, true, true);
          out.add(last);
        } else {
          FullHttpMessage full = newFullMessage(id, headers, validateHeaders, ctx.alloc());
          out.add(full);
        }
      } else {
        HttpMessage req = newMessage(id, headers, validateHeaders);
        if (!HttpUtil.isContentLengthSet(req)) {
          req.headers().add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        }
        out.add(req);
      }
    } else if (frame instanceof Http2DataFrame) {
      Http2DataFrame dataFrame = (Http2DataFrame) frame;
      if (dataFrame.isEndStream()) {
        out.add(new DefaultLastHttpContent(dataFrame.content().retain(), validateHeaders));
      } else {
        out.add(new DefaultHttpContent(dataFrame.content().retain()));
      }
    }
  }

  private void encodeLastContent(LastHttpContent last, List<Object> out) {
    boolean needFiller = !(last instanceof FullHttpMessage) && last.trailingHeaders().isEmpty();
    if (last.content().isReadable() || needFiller) {
      out.add(new DefaultHttp2DataFrame(last.content().retain(), last.trailingHeaders().isEmpty()));
    }
    if (!last.trailingHeaders().isEmpty()) {
      Http2Headers headers = HttpConversionUtil.toHttp2Headers(last.trailingHeaders(), validateHeaders);
      out.add(new DefaultHttp2HeadersFrame(headers, true));
    }
  }

  /**
   * Encode from an {@link HttpObject} to an {@link Http2StreamFrame}. This method will
   * be called for each written message that can be handled by this encoder.
   *
   * NOTE: 100-Continue responses that are NOT {@link FullHttpResponse} will be rejected.
   *
   * @param ctx           the {@link ChannelHandlerContext} which this handler belongs to
   * @param obj           the {@link HttpObject} message to encode
   * @param out           the {@link List} into which the encoded msg should be added
   *                      needs to do some kind of aggregation
   * @throws Exception    is thrown if an error occurs
   */
  @Override
  protected void encode(ChannelHandlerContext ctx, HttpObject obj, List<Object> out) throws Exception {
    // 100-continue is typically a FullHttpResponse, but the decoded
    // Http2HeadersFrame should not be marked as endStream=true
    if (obj instanceof HttpResponse) {
      final HttpResponse res = (HttpResponse) obj;
      if (res.status().equals(HttpResponseStatus.CONTINUE)) {
        if (res instanceof FullHttpResponse) {
          final Http2Headers headers = toHttp2Headers(ctx, res, validateHeaders);
          out.add(new DefaultHttp2HeadersFrame(headers, false));
          return;
        } else {
          throw new EncoderException("" + HttpResponseStatus.CONTINUE + " must be a FullHttpResponse");
        }
      }
    }

    if (obj instanceof HttpMessage) {
      Http2Headers headers = toHttp2Headers(ctx, (HttpMessage) obj, validateHeaders);
      boolean noMoreFrames = false;
      if (obj instanceof FullHttpMessage) {
        FullHttpMessage full = (FullHttpMessage) obj;
        noMoreFrames = !full.content().isReadable() && full.trailingHeaders().isEmpty();
      }

      out.add(new DefaultHttp2HeadersFrame(headers, noMoreFrames));
    }

    if (obj instanceof LastHttpContent) {
      LastHttpContent last = (LastHttpContent) obj;
      encodeLastContent(last, out);
    } else if (obj instanceof HttpContent) {
      HttpContent cont = (HttpContent) obj;
      out.add(new DefaultHttp2DataFrame(cont.content().retain(), false));
    }
  }

  protected abstract Http2Headers toHttp2Headers(
      final ChannelHandlerContext ctx,
      final HttpMessage msg,
      boolean validateHeaders) throws Http2Exception;

  protected abstract HttpMessage newMessage(final int id, final Http2Headers headers, boolean validateHeaders)
      throws Http2Exception;

  protected abstract FullHttpMessage newFullMessage(
      final int id,
      final Http2Headers headers,
      boolean validateHeaders,
      final ByteBufAllocator alloc) throws Http2Exception;

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
    super.handlerAdded(ctx);

    // this handler is typically used on an Http2StreamChannel. At this
    // stage, ssl handshake should've been established. checking for the
    // presence of SslHandler in the parent's channel pipeline to
    // determine the HTTP scheme should suffice, even for the case where
    // SniHandler is used.
    final Attribute<HttpScheme> schemeAttribute = connectionSchemeAttribute(ctx);
    if (schemeAttribute.get() == null) {
      final HttpScheme scheme = isSsl(ctx) ? HttpScheme.HTTPS : HttpScheme.HTTP;
      schemeAttribute.set(scheme);
    }
  }

  protected boolean isSsl(final ChannelHandlerContext ctx) {
    final Channel connChannel = connectionChannel(ctx);
    return null != connChannel.pipeline().get(SslHandler.class);
  }

  protected static HttpScheme connectionScheme(ChannelHandlerContext ctx) {
    final HttpScheme scheme = connectionSchemeAttribute(ctx).get();
    return scheme == null ? HttpScheme.HTTP : scheme;
  }

  protected static Attribute<HttpScheme> connectionSchemeAttribute(ChannelHandlerContext ctx) {
    final Channel ch = connectionChannel(ctx);
    return ch.attr(SCHEME_ATTR_KEY);
  }

  protected static Channel connectionChannel(ChannelHandlerContext ctx) {
    final Channel ch = ctx.channel();
    return ch instanceof Http2StreamChannel ? ch.parent() : ch;
  }
}
