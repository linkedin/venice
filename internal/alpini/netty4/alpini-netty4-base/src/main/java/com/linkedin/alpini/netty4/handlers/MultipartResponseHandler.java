package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.HeaderUtils;
import com.linkedin.alpini.base.misc.ImmutableMapEntry;
import com.linkedin.alpini.netty4.misc.ChannelTaskSerializer;
import com.linkedin.alpini.netty4.misc.ChunkedHttpResponse;
import com.linkedin.alpini.netty4.misc.MultipartContent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpConstants;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AsciiString;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * @deprecated Use the HttpMultiPartContentEncoder
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@ChannelHandler.Sharable
@Deprecated
public class MultipartResponseHandler extends ChannelInitializer<Channel> {
  private static final byte[] CRLF = { HttpConstants.CR, HttpConstants.LF };
  private static final byte[] DASH_DASH = { '-', '-' };
  private static final byte[] DASH_DASH_CRLF = { '-', '-', HttpConstants.CR, HttpConstants.LF };

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().replace(this, "multipart-response-handler", new Handler());
  }

  class Handler extends ChannelOutboundHandlerAdapter {
    private ChannelTaskSerializer _serializer;
    private ChannelFuture _afterWrite;

    private HttpContent _boundary;
    private int _partCount;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      _serializer = new ChannelTaskSerializer(ctx);
      _afterWrite = ctx.newSucceededFuture();
      _boundary = null;
    }

    /**
     * Calls {@link ChannelHandlerContext#write(Object, ChannelPromise)} to forward
     * to the next {@link io.netty.channel.ChannelOutboundHandler} in the {@link io.netty.channel.ChannelPipeline}.
     * <p>
     * Sub-classes may override this method to change behavior.
     *
     * @param ctx
     * @param msg
     * @param promise
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (msg instanceof ChunkedHttpResponse && ((ChunkedHttpResponse) msg).content().isReadable()) {
        throw new IllegalStateException("Chunked responses must not have content");
      }

      ChannelPromise afterWrite = ctx.newPromise();

      _serializer.executeTask(completion -> {
        try {
          write0(ctx, msg, promise);
        } catch (Exception ex) {
          promise.setFailure(ex);
        } finally {
          afterWrite.setSuccess();
          completion.setSuccess();
        }
      }, future -> {});
      _afterWrite = afterWrite.isDone() ? ctx.newSucceededFuture() : afterWrite;
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
      ChannelFuture afterWrite = _afterWrite;
      if (afterWrite.isDone()) {
        super.flush(ctx);
      } else {
        afterWrite.addListener(ignored -> super.flush(ctx));
      }
    }

    private void write0(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (!(msg instanceof FullHttpMessage)) {
        if (msg instanceof HttpMessage && !(msg instanceof MultipartContent)) {
          HttpMessage message = (HttpMessage) msg;
          HeaderUtils.ContentType contentType =
              HeaderUtils.parseContentType(message.headers().get(HttpHeaderNames.CONTENT_TYPE, "text/plain"));

          if (contentType.isMultipart()) {
            String boundaryText = CollectionUtil.stream(contentType.parameters())
                .filter(e -> "boundary".equals(e.getKey()))
                .map(Map.Entry::getValue)
                .findAny()
                .orElseGet(() -> {
                  String boundary = HeaderUtils.randomWeakUUID().toString();
                  message.headers()
                      .set(
                          HeaderNames.CONTENT_TYPE,
                          HeaderUtils.buildContentType(
                              contentType.type(),
                              contentType.subType(),
                              CollectionUtil.concat(
                                  contentType.parameters(),
                                  Collections.singleton(ImmutableMapEntry.make("boundary", boundary)).iterator())));
                  return boundary;
                });

            if (!boundaryText.isEmpty()) {
              ByteBuf boundary = ctx.alloc().buffer();
              boundary.writeBytes(DASH_DASH);
              boundary.writeCharSequence(boundaryText, StandardCharsets.US_ASCII);
              boundary.writeBytes(CRLF);

              _boundary = new DefaultHttpContent(boundary);
              _partCount = 0;

              HttpUtil.setTransferEncodingChunked(message, true);
              message.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
            }
          }
        } else if (msg instanceof HttpContent) {
          HttpContent httpContent = (HttpContent) msg;

          if (_boundary != null) {
            if (httpContent instanceof MultipartContent) {
              MultipartContent multipartContent = (MultipartContent) httpContent;

              if (_partCount > 0) {
                ctx.write(_boundary.retainedDuplicate());
              }

              _partCount++;

              if (!multipartContent.headers().contains(HttpHeaderNames.CONTENT_TYPE)) {
                multipartContent.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/binary");
              }
              multipartContent.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, httpContent.content().readableBytes());

              ByteBuf part = ctx.alloc().buffer();
              encodeHeaders(multipartContent.headers(), part);

              if (httpContent.content().isReadable()) {
                part = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(ctx.alloc(), part, httpContent.content());
                if (!endsInCrLf(httpContent.content())) {
                  part = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(ctx.alloc(), part, Unpooled.wrappedBuffer(CRLF));
                }
              }
              ctx.write(new DefaultHttpContent(part), promise);
              return;
            } else if (httpContent instanceof LastHttpContent) {
              ByteBuf trailer = ctx.alloc().buffer();

              trailer.writeBytes(_boundary.content().duplicate());
              trailer.writerIndex(trailer.writerIndex() - 2); // remove trailing CRLF
              trailer.writeBytes(DASH_DASH_CRLF);
              trailer.writeBytes(httpContent.content());

              ctx.write(httpContent.replace(trailer), promise);

              _boundary.release();
              _boundary = null;
              return;
            } else {
              if (_partCount == 0) {
                _boundary.release();
                _boundary = null;
              }
            }
          }
        }
      }

      super.write(ctx, msg, promise);
    }
  }

  /**
   * Encode the {@link HttpHeaders} into a {@link ByteBuf}.
   */
  protected void encodeHeaders(HttpHeaders headers, ByteBuf buf) throws Exception {
    Iterator<Map.Entry<CharSequence, CharSequence>> iter = headers.iteratorCharSequence();
    while (iter.hasNext()) {
      Map.Entry<CharSequence, CharSequence> header = iter.next();
      encodeHeader(header.getKey(), header.getValue(), buf);
    }
    buf.ensureWritable(2);
    int offset = buf.writerIndex();
    buf.setByte(offset++, '\r');
    buf.setByte(offset++, '\n');
    buf.writerIndex(offset);
  }

  private static boolean endsInCrLf(ByteBuf buffer) {
    return buffer.readableBytes() > 1 && buffer.getUnsignedByte(buffer.writerIndex() - 1) == '\n'
        && buffer.getUnsignedByte(buffer.writerIndex() - 2) == '\r';
  }

  public static void encodeHeader(CharSequence name, CharSequence value, ByteBuf buf) throws Exception {
    final int nameLen = name.length();
    final int valueLen = value.length();
    final int entryLen = nameLen + valueLen + 4;
    buf.ensureWritable(entryLen);
    int offset = buf.writerIndex();
    writeAscii(buf, offset, name, nameLen);
    offset += nameLen;
    buf.setByte(offset++, ':');
    buf.setByte(offset++, ' ');
    writeAscii(buf, offset, value, valueLen);
    offset += valueLen;
    buf.setByte(offset++, '\r');
    buf.setByte(offset++, '\n');
    buf.writerIndex(offset);
  }

  private static void writeAscii(ByteBuf buf, int offset, CharSequence value, int valueLen) {
    if (value instanceof AsciiString) {
      ByteBufUtil.copy((AsciiString) value, 0, buf, offset, valueLen);
    } else {
      writeCharSequence(buf, offset, value, valueLen);
    }
  }

  private static void writeCharSequence(ByteBuf buf, int offset, CharSequence value, int valueLen) {
    for (int i = 0; i < valueLen; ++i) {
      buf.setByte(offset++, AsciiString.c2b(value.charAt(i)));
    }
  }

  static class Info {
    final HttpContent _boundary;
    int _partCount;

    Info(@Nonnull ByteBuf boundary) {
      _boundary = new DefaultHttpContent(boundary);
    }
  }
}
