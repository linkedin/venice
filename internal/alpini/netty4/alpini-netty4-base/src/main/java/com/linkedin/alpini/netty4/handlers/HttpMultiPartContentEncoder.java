package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.HeaderUtils;
import com.linkedin.alpini.base.misc.ImmutableMapEntry;
import com.linkedin.alpini.netty4.misc.BasicFullHttpMultiPart;
import com.linkedin.alpini.netty4.misc.BasicHttpMultiPart;
import com.linkedin.alpini.netty4.misc.FullHttpMultiPart;
import com.linkedin.alpini.netty4.misc.HttpMultiPart;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpConstants;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectEncoder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AsciiString;
import io.netty.util.AttributeMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * Encodes a stream of {@link HttpMultiPart} objects into a MIME encoded document.
 *
 * Created by acurtis on 3/22/17.
 */
public class HttpMultiPartContentEncoder extends HttpObjectEncoder<HttpMultiPart> {
  private HeaderUtils.ContentType _contentType;
  private boolean _encodingMultiPart;
  private boolean _hasEmittedMultiPart;
  private boolean _previousPartComplete;
  private ByteBuf _boundary;
  private final int _chunkSize;
  private final int _maxChunkSize;

  private static final ByteBuf BOUNDARY_PREFIX = Unpooled.unreleasableBuffer(
      Unpooled.wrappedBuffer(new byte[] { HttpConstants.CR, HttpConstants.LF, '-', '-' }).asReadOnly());
  private static final ByteBuf BOUNDARY_SUFFIX = Unpooled
      .unreleasableBuffer(Unpooled.wrappedBuffer(new byte[] { HttpConstants.CR, HttpConstants.LF }).asReadOnly());
  private static final ByteBuf TERMINAL_SUFFIX = Unpooled.unreleasableBuffer(
      Unpooled.wrappedBuffer(new byte[] { '-', '-', HttpConstants.CR, HttpConstants.LF }).asReadOnly());

  public HttpMultiPartContentEncoder() {
    this(3072, 4096);
  }

  public HttpMultiPartContentEncoder(int chunkSize, int maxChunkSize) {
    assert !isSharable() : "Not sharable";
    _chunkSize = chunkSize;
    _maxChunkSize = maxChunkSize;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    if (msg instanceof HttpContent && !(msg instanceof FullHttpMessage)) {
      HttpContent httpContent = (HttpContent) msg;
      if (httpContent.content().readableBytes() > _maxChunkSize) {
        if (httpContent instanceof FullHttpMultiPart) {
          FullHttpMultiPart part = (FullHttpMultiPart) httpContent;
          super.encode(ctx, new BasicHttpMultiPart(part, part.headers()), out);
          httpContent = new DefaultLastHttpContent(httpContent.content());
        }
        ByteBuf bytes = httpContent.content().duplicate();
        do {
          HttpContent chunk = new DefaultHttpContent(NettyUtils.read(bytes, _chunkSize));
          try {
            super.encode(ctx, chunk, out);
          } finally {
            chunk.release();
          }
        } while (bytes.readableBytes() > _chunkSize);
        httpContent = httpContent.replace(NettyUtils.read(bytes, bytes.readableBytes()));
        try {
          super.encode(ctx, httpContent, out);
          return;
        } finally {
          httpContent.release();
        }
      }
    }
    super.encode(ctx, msg, out);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if ((msg instanceof HttpRequest || msg instanceof HttpResponse) && !(msg instanceof LastHttpContent)) {
      HttpMessage message = (HttpMessage) msg;
      _contentType = HeaderUtils.parseContentType(message.headers().get(HttpHeaderNames.CONTENT_TYPE, "text/plain"));
      _encodingMultiPart = false;
      _hasEmittedMultiPart = false;

      if (_contentType.isMultipart()) {
        _boundary = StreamSupport
            .stream(Spliterators.spliteratorUnknownSize(_contentType.parameters(), Spliterator.IMMUTABLE), false)
            .filter(entry -> "boundary".equals(entry.getKey()))
            .map(Map.Entry::getValue)
            .findFirst()
            .map(AsciiString::of)
            .map(boundary -> ByteBufUtil.writeAscii(ctx.alloc(), boundary))
            .orElseGet(() -> generateBoundary(ctx.alloc(), message))
            .asReadOnly();
      }
      ctx.write(message, promise);
    } else if (msg instanceof HttpMultiPart) {
      if (!_contentType.isMultipart()) {
        throw new IllegalStateException("Cannot encode multipart objects for content-type: " + _contentType);
      }

      if (_hasEmittedMultiPart && !_previousPartComplete) {
        super.write(ctx, LastHttpContent.EMPTY_LAST_CONTENT, ctx.newPromise());
      }
      _encodingMultiPart = true;
      if (msg instanceof FullHttpMultiPart && !(msg instanceof LastHttpContent)) {
        msg = new FullMultiPart(
            (HttpMessage) msg,
            ((FullHttpMultiPart) msg).content(),
            ((FullHttpMultiPart) msg).headers());
      }
      super.write(ctx, msg, promise);
      _encodingMultiPart = !(msg instanceof HttpContent);
      _previousPartComplete = msg instanceof LastHttpContent;
      _hasEmittedMultiPart = true;
    } else if (_encodingMultiPart && msg instanceof LastHttpContent) {
      _encodingMultiPart = false;
      super.write(ctx, msg, promise);
    } else if (_hasEmittedMultiPart && msg instanceof LastHttpContent) {
      _hasEmittedMultiPart = false;
      CompositeByteBuf buf = ctx.alloc().compositeBuffer();
      buf.addComponents(
          true,
          BOUNDARY_PREFIX.duplicate(),
          _boundary.duplicate(),
          TERMINAL_SUFFIX.duplicate(),
          ((LastHttpContent) msg).content());
      ctx.write(((LastHttpContent) msg).replace(buf), promise);
      _boundary = null;
    } else if (_encodingMultiPart) {
      super.write(ctx, msg, promise);
    } else if (msg instanceof LastHttpContent) {
      ctx.write(msg, promise);
      if (_boundary != null) {
        _boundary.release();
        _boundary = null;
      }
    } else {
      ctx.write(msg, promise);
    }
  }

  /**
   * The HttpObjectEncoder requires receiving LastHttpContent in order for its internal state be correct.
   */
  private static final class FullMultiPart extends BasicFullHttpMultiPart implements LastHttpContent {
    FullMultiPart(HttpMessage httpMessage, ByteBuf content, HttpHeaders headers) {
      super(httpMessage, content, headers);
    }

    private FullMultiPart(AttributeMap attributes, ByteBuf content, HttpHeaders headers) {
      super(attributes, content, headers);
    }

    @Override
    public FullMultiPart retain() {
      super.retain();
      return this;
    }

    @Override
    public FullMultiPart retain(int increment) {
      super.retain(increment);
      return this;
    }

    @Override
    public FullMultiPart touch() {
      super.touch();
      return this;
    }

    @Override
    public FullMultiPart touch(Object hint) {
      super.touch(hint);
      return this;
    }

    @Override
    public FullMultiPart copy() {
      return replace(content().copy());
    }

    @Override
    public FullMultiPart duplicate() {
      return replace(content().duplicate());
    }

    @Override
    public FullMultiPart retainedDuplicate() {
      return replace(content().retainedDuplicate());
    }

    @Override
    public FullMultiPart replace(ByteBuf content) {
      return new FullMultiPart((AttributeMap) this, content, headers());
    }

    @Override
    public HttpHeaders trailingHeaders() {
      return LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders();
    }
  }

  private ByteBuf generateBoundary(ByteBufAllocator alloc, HttpMessage message) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    // Not using securerandom because that drains the entropy pool
    String boundary = "--=Part_" + new UUID(random.nextLong(), random.nextLong()).toString();
    String contentType = HeaderUtils.buildContentType(
        _contentType.type(),
        _contentType.subType(),
        Stream.concat(
            Stream.of(ImmutableMapEntry.make("boundary", boundary)),
            StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(_contentType.parameters(), Spliterator.IMMUTABLE), false))
            .iterator());
    _contentType = HeaderUtils.parseContentType(contentType);
    message.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
    return ByteBufUtil.writeAscii(alloc, boundary);
  }

  @Override
  protected void encodeInitialLine(ByteBuf buf, HttpMultiPart message) throws Exception {
    buf.writeBytes(BOUNDARY_PREFIX, BOUNDARY_PREFIX.readerIndex(), BOUNDARY_PREFIX.readableBytes());
    buf.writeBytes(_boundary, _boundary.readerIndex(), _boundary.readableBytes());
    buf.writeBytes(BOUNDARY_SUFFIX, BOUNDARY_SUFFIX.readerIndex(), BOUNDARY_SUFFIX.readableBytes());
  }
}
