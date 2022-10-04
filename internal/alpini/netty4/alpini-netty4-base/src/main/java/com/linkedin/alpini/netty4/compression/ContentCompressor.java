package com.linkedin.alpini.netty4.compression;

import com.linkedin.alpini.base.misc.Preconditions;
import com.linkedin.alpini.netty4.handlers.BasicHttpContentEncoder;
import com.linkedin.alpini.netty4.handlers.ChannelInitializer;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentEncoder.Result;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AsciiString;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ContentCompressor extends BasicHttpContentEncoder {
  private static final Logger LOG = LogManager.getLogger(ContentCompressor.class);

  private final EventExecutorGroup _executor;
  private final int compressionLevel;
  private final int windowBits;
  private final int memLevel;
  private ChannelHandlerContext _ctx;
  // The content encodings this node supports.
  private final List<AsciiString> _supportedContentEncodings;

  /**
   * Creates a new handler with the default compression level (<tt>6</tt>),
   * default window size (<tt>15</tt>) and default memory level (<tt>8</tt>).
   */
  public ContentCompressor() {
    this(null, null);
  }

  // if input supportedContentEncodings is null, no supported contentEncodings checked
  public ContentCompressor(String supportedContentEncodings) {
    this(supportedContentEncodings, null);
  }

  /**
   * Creates a new handler with the specified compression level, default
   * window size (<tt>15</tt>) and default memory level (<tt>8</tt>).
   *
   * @param compressionLevel
   *        {@code 1} yields the fastest compression and {@code 9} yields the
   *        best compression.  {@code 0} means no compression.  The default
   *        compression level is {@code 6}.
   */
  public ContentCompressor(int compressionLevel) {
    this(null, null, compressionLevel);
  }

  public ContentCompressor(EventExecutorGroup executor) {
    this(null, executor);
  }

  public ContentCompressor(String supportedContentEncodings, EventExecutorGroup executor) {
    this(supportedContentEncodings, executor, 6);
  }

  public ContentCompressor(EventExecutorGroup executor, int compressionLevel) {
    this(null, executor, compressionLevel, 15, 8);
  }

  public ContentCompressor(String supportedContentEncodings, EventExecutorGroup executor, int compressionLevel) {
    this(supportedContentEncodings, executor, compressionLevel, 15, 8);
  }

  /**
   * Creates a new handler with the specified compression level, window size,
   * and memory level..
   *
   * @param compressionLevel
   *        {@code 1} yields the fastest compression and {@code 9} yields the
   *        best compression.  {@code 0} means no compression.  The default
   *        compression level is {@code 6}.
   * @param windowBits
   *        The base two logarithm of the size of the history buffer.  The
   *        value should be in the range {@code 9} to {@code 15} inclusive.
   *        Larger values result in better compression at the expense of
   *        memory usage.  The default value is {@code 15}.
   * @param memLevel
   *        How much memory should be allocated for the internal compression
   *        state.  {@code 1} uses minimum memory and {@code 9} uses maximum
   *        memory.  Larger values result in better and faster compression
   *        at the expense of memory usage.  The default value is {@code 8}
   */
  public ContentCompressor(int compressionLevel, int windowBits, int memLevel) {
    this(null, null, compressionLevel, windowBits, memLevel);
  }

  public ContentCompressor(String supportedContentEncodings, int compressionLevel, int windowBits, int memLevel) {
    this(supportedContentEncodings, null, compressionLevel, windowBits, memLevel);
  }

  public ContentCompressor(
      String supportedContentEncodingString,
      EventExecutorGroup executor,
      int compressionLevel,
      int windowBits,
      int memLevel) {
    _supportedContentEncodings = supportedContentEncodingString == null
        ? null
        : Stream.of(supportedContentEncodingString.split(",")).map(AsciiString::of).collect(Collectors.toList());

    _executor = executor;
    this.compressionLevel = Preconditions.between(compressionLevel, "compressionLevel", 0, 9);
    this.windowBits = Preconditions.between(windowBits, "windowBits", 9, 15);
    this.memLevel = Preconditions.between(memLevel, "memLevel", 1, 9);
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    _ctx = ctx;
    super.handlerAdded(ctx);
  }

  /**
   * Prepare to encode the HTTP message content.
   *
   * @param headers        the headers
   * @param acceptEncoding the value of the {@code "Accept-Encoding"} header
   * @return the result of preparation, which is composed of the determined
   * target content encoding and a new {@link EmbeddedChannel} that
   * encodes the content into the target content encoding.
   * {@code null} if {@code acceptEncoding} is unsupported or rejected
   * and thus the content should be handled as-is (i.e. no encoding).
   */
  @Override
  protected Result beginEncode(HttpMessage headers, String acceptEncoding) throws Exception {
    if (HttpHeaderValues.IDENTITY.contentEqualsIgnoreCase(headers.headers().get(HttpHeaderNames.CONTENT_ENCODING))) {
      headers.headers().remove(HttpHeaderNames.CONTENT_ENCODING);
    }

    if (HttpUtil.getContentLength(headers, Integer.MAX_VALUE) < 1024) {
      return null;
    }

    // If the HttpMessage is already encoded with snappy, gzip or deflate, we do not need to perform
    // further encoding so this method must return null.
    // If there is no preferred encoding available from the acceptable encodings, we return null.
    String contentEncoding = headers.headers().get(HttpHeaderNames.CONTENT_ENCODING);

    List<Encoding> acceptable = decodeAcceptEncoding(acceptEncoding);

    // Check if the user asks for any compression and it is already encoded
    if (contentEncoding != null
        && acceptable.stream().anyMatch(encoding -> encoding._encoding == ANY && encoding._value > 0.0)) {
      return null;
    }

    // Check if the compression is already in the list of acceptable encodings
    if (contentEncoding != null && acceptable.stream()
        .anyMatch(encoding -> encoding._encoding.contentEqualsIgnoreCase(contentEncoding) && encoding._value > 0.0)) {
      return null;
    }

    if (contentEncoding != null) {
      LOG.warn(
          "Storage node responded in an encoding which the client did not request: {} {} {}",
          acceptEncoding,
          contentEncoding,
          headers);
      return null;
    }

    List<Encoding> supportedEncodings = getSupportedEncodings(acceptable);

    AsciiString preferred = preferredEncoding(
        supportedEncodings,
        HttpUtil.isTransferEncodingChunked(headers) || !(headers instanceof HttpContent));

    if (preferred == CompressionUtils.SNAPPY) {
      return new Result(
          CompressionUtils.SNAPPY_ENCODING,
          new EmbeddedChannel(
              _ctx.channel().id(),
              _ctx.channel().metadata().hasDisconnect(),
              _ctx.channel().config(),
              handler(new SnappyEncoder())));
    }

    if (preferred == CompressionUtils.SNAPPY_FRAMED) {
      return new Result(
          CompressionUtils.SNAPPY_FRAMED_ENCODING,
          new EmbeddedChannel(
              _ctx.channel().id(),
              _ctx.channel().metadata().hasDisconnect(),
              _ctx.channel().config(),
              handler(new SnappyFrameEncoder() {
                /**
                 * The latest Netty version has way to skip the parent class methods if there is no override.
                 * When a different executor is used, the order is not preserved.
                 */
                @Override
                public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
                  super.close(ctx, promise);
                }

                @Override
                public void read(ChannelHandlerContext ctx) throws Exception {
                  super.read(ctx);
                }

                @Override
                public void flush(ChannelHandlerContext ctx) throws Exception {
                  super.flush(ctx);
                }
              })));
    }

    ZlibWrapper wrapper;
    String targetContentEncoding;

    if (preferred == CompressionUtils.GZIP) {
      wrapper = ZlibWrapper.GZIP;
      targetContentEncoding = "gzip";
    } else if (preferred == CompressionUtils.DEFLATE) {
      wrapper = ZlibWrapper.ZLIB;
      targetContentEncoding = "deflate";
    } else {
      headers.headers().remove(HttpHeaderNames.CONTENT_ENCODING);
      return null;
    }

    return new Result(
        targetContentEncoding,
        new EmbeddedChannel(
            _ctx.channel().id(),
            _ctx.channel().metadata().hasDisconnect(),
            _ctx.channel().config(),
            handler(new ChannelOutboundHandlerAdapter() {
              /**
               * The latest Netty version has way to skip the parent class methods if there is no override.
               * When a different executor is used, the order is not preserved.
               */
              @Override
              public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                encoder.handlerAdded(ctx);
              }

              @Override
              public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
                encoder.handlerRemoved(ctx);
              }

              @Override
              public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
                encoder.close(ctx, promise);
              }

              @Override
              public void read(ChannelHandlerContext ctx) throws Exception {
                encoder.read(ctx);
              }

              @Override
              public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                encoder.write(ctx, msg, promise);
              }

              @Override
              public void flush(ChannelHandlerContext ctx) throws Exception {
                encoder.flush(ctx);
              }

              final ChannelOutboundHandler encoder =
                  ZlibCodecFactory.newZlibEncoder(wrapper, compressionLevel, windowBits, memLevel);
            })));
  }

  private ChannelHandler handler(ChannelHandler channelHandler) {
    return new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel ch) throws Exception {
        if (_executor == null) {
          ch.pipeline().addLast(NettyUtils.executorGroup(ch), channelHandler);
        } else {
          ch.pipeline().addLast(_executor, channelHandler);
        }
      }
    };
  }

  private List<Encoding> getSupportedEncodings(List<Encoding> acceptable) {
    // if it is null or empty, just return all the accepted encodings
    if (_supportedContentEncodings == null || _supportedContentEncodings.isEmpty()) {
      return acceptable;
    }

    List<Encoding> supported = _supportedContentEncodings.stream()
        .flatMap(encString -> acceptable.stream().filter(enc -> enc.matchType(encString)))
        .distinct()
        .collect(Collectors.toList());

    return supported;
  }

  private static final AsciiString ANY = AsciiString.of("*");

  private static class Encoding {
    private final AsciiString _encoding;
    private final double _value;

    private Encoding(AsciiString encoding, double value) {
      _encoding = encoding;
      _value = value;
    }

    private boolean matchType(AsciiString encoding) {
      return _encoding.contentEqualsIgnoreCase(encoding) || _encoding.equals(ANY);
    }
  }

  static List<Encoding> decodeAcceptEncoding(String acceptEncoding) {
    if (acceptEncoding == null || acceptEncoding.isEmpty()) {
      return Collections.emptyList();
    }
    List<Encoding> encodings = new ArrayList<>();
    for (String encoding: acceptEncoding.split(",")) {
      float q = 1.0f;
      int equalsPos = encoding.indexOf('=');
      if (equalsPos != -1) {
        try {
          q = Float.valueOf(encoding.substring(equalsPos + 1));
        } catch (NumberFormatException e) {
          // Ignore encoding
          q = 0.0f;
        }
      }
      double value = q;
      AsciiString e;
      if (encoding.indexOf('*') >= 0) {
        e = ANY;
      } else if (encoding.contains("x-snappy-framed")) {
        e = CompressionUtils.SNAPPY_FRAMED;
      } else if (encoding.contains("snappy")) {
        e = CompressionUtils.SNAPPY;
      } else if (encoding.contains("gzip")) {
        e = CompressionUtils.GZIP;
      } else if (encoding.contains("deflate")) {
        e = CompressionUtils.DEFLATE;
      } else if (encoding.contains("identity")) {
        e = HttpHeaderValues.IDENTITY;
      } else {
        LOG.debug("unknown encoding {}", encoding);
        continue;
      }
      if (encodings.removeIf(enc -> enc._encoding == e && enc._value < value)
          || encodings.stream().noneMatch(enc -> enc._encoding == e)) {
        encodings.add(new Encoding(e, value));
      }
    }
    return encodings;
  }

  static AsciiString preferredEncoding(List<Encoding> encodings, boolean streaming) {

    OptionalDouble maxValue = encodings.stream()
        // snappy is not a streaming encoding
        .filter(enc -> !(streaming && enc._encoding == CompressionUtils.SNAPPY))
        .mapToDouble(enc -> enc._value)
        .max();

    if (maxValue.isPresent()) {
      Optional<AsciiString> preferred = encodings.stream()
          .filter(enc -> enc._value == maxValue.getAsDouble())
          .map(enc -> enc._encoding)
          .findFirst()
          .map(encoding -> {
            if (encoding == ANY) {
              if (encodings.stream().noneMatch(enc -> enc._encoding == CompressionUtils.GZIP)) {
                return CompressionUtils.GZIP;
              } else if (encodings.stream().noneMatch(enc -> enc._encoding == CompressionUtils.DEFLATE)) {
                return CompressionUtils.DEFLATE;
              } else {
                return HttpHeaderValues.IDENTITY;
              }
            }
            return encoding;
          })
          .filter(encoding -> encoding != HttpHeaderValues.IDENTITY);

      return preferred.orElse(null);
    }
    return null;
  }

  static AsciiString preferredEncoding(String accept) {
    return preferredEncoding(decodeAcceptEncoding(accept), false);
  }

}
